use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::{ActorActionDraft, FrozenEntityIssueAuthority};
use pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame;
use pumpkin_cluster::canonical_action::{Action, ActionState, ApplyFailure, Cell, Effect, Target, TruthView, Value};
use pumpkin_cluster::entity_body::EntityPayload;
use pumpkin_cluster::item_contact::ContactObservation;
use pumpkin_cluster::item_spatial::ItemSpatialSample;
use pumpkin_cluster::projectile_tick::ProjectileTickIntent;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

pub(super) struct FrozenProjectileTickJob {
    pub intent: ProjectileTickIntent,
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    draft: Option<ActorActionDraft>,
}

impl FrozenProjectileTickJob {
    pub(super) fn actor(&self) -> ActorId { self.intent.projectile.actor() }
    pub(super) fn incarnation(&self) -> u64 {
        match self.actor() { ActorId::Entity(id) => id.generation, _ => 0 }
    }
}

pub(super) struct ProjectileEmissionPage {
    pub completed_jobs: usize,
    pub actions: Vec<(Action, ActorActionDraft)>,
    pub samples: Vec<(ChunkKey, ItemSpatialSample)>,
}

pub(super) fn capture_frame(
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    source: PeerGeneration, tick: TickStamp,
    drafts: &HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
) -> Result<FrozenActorTickFrame<FrozenProjectileTickJob>, ApplyFailure> {
    capture_frame_with_cuts(snapshot, source, tick, drafts, &[])
}

pub(super) fn capture_frame_with_cuts(
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    source: PeerGeneration, tick: TickStamp,
    drafts: &HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
    cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut],
) -> Result<FrozenActorTickFrame<FrozenProjectileTickJob>, ApplyFailure> {
    let bodies = snapshot.captured_entity_bodies().filter(|(_, body)| matches!(body.payload, EntityPayload::Projectile(_)) && body.owner == source)
        .map(|(chunk, body)| (chunk, body.clone())).collect::<Vec<_>>();
    let mut jobs = Vec::new();
    for (chunk, body) in &bodies {
        let EntityPayload::Projectile(projectile) = &body.payload else { continue; };
        if body.owner != source || projectile.shot_tick == tick || projectile.shot_tick.is_after(tick)
            || projectile.last_simulation_tick.is_some_and(|previous| !tick.is_after(previous)) { continue; }
        let target = Target::Chunk { key: *chunk, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(body.id), inventory: None };
        let sample = snapshot.original_entity_sample(body.id).ok_or(ApplyFailure::Condition(target))?;
        snapshot.actor_occupancy_cut(target, &Value::Entity((body.clone()).into()))?;
        let effective = sample.spatial.clone();
        let observation = ContactObservation::entity(body, *chunk, &effective, 0.125, 0.25, tick,
            sample.sequence).ok_or(ApplyFailure::InvalidAction)?;
        let mut blocks = ProjectileTickIntent::block_targets(&effective)?;
        let start = effective.position.map(f64::from_bits);
        let end: [f64; 3] = std::array::from_fn(|axis| start[axis] + effective.velocity_micro[axis] as f64 / 1_000_000.0);
        let area = match projectile.kind { pumpkin_cluster::entity_body::ProjectileKind::SplashPotion
            | pumpkin_cluster::entity_body::ProjectileKind::LingeringPotion | pumpkin_cluster::entity_body::ProjectileKind::Firework => 4.0, _ => 0.0 };
        let bounds = [
            [start[0].min(end[0]) - 0.3 - area, start[2].min(end[2]) - 0.3 - area],
            [start[0].max(end[0]) + 0.3 + area, start[2].max(end[2]) + 0.3 + area],
        ];
        let minimum = bounds[0].map(|coordinate| (coordinate / 16.0).floor() as i32);
        let maximum = bounds[1].map(|coordinate| (coordinate / 16.0).floor() as i32);
        let mut seed_targets = blocks.iter().copied().collect::<std::collections::BTreeSet<_>>();
        seed_targets.insert(target);
        for x in minimum[0]..=maximum[0] {
            for z in minimum[1]..=maximum[1] {
                seed_targets.insert(Target::Chunk { key: ChunkKey::new(chunk.world, x, z),
                    part: pumpkin_cluster::canonical_action::ChunkPart::ActorOccupancy, inventory: None });
            }
        }
        let mut seed_leases = Vec::new();
        if projectile.kind == pumpkin_cluster::entity_body::ProjectileKind::EnderPearl
            && let Some(lease) = projectile.shooter_lease
            && let Some((home, _)) = snapshot.player_association(lease) {
            seed_targets.insert(Target::Chunk { key: home, part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid), inventory: None });
            seed_leases.push(lease);
        }
        let seed_chunks = seed_targets.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }).collect::<std::collections::BTreeSet<_>>();
        let mut snapshot = snapshot.select_scope(&seed_chunks, &seed_targets.iter().copied().collect::<Vec<_>>(), &seed_leases, &[])?;
        let available_cuts = cuts.iter().filter(|cut| seed_chunks.contains(&cut.chunk()) && !snapshot.original_store().contains(cut.chunk())).cloned().collect::<Vec<_>>();
        if !available_cuts.is_empty() { snapshot = snapshot.with_historical_chunks(&available_cuts)?; }
        let mut participants = std::collections::BTreeSet::new();
        for x in minimum[0]..=maximum[0] {
            for z in minimum[1]..=maximum[1] {
                let root_target = Target::Chunk { key: ChunkKey::new(chunk.world, x, z),
                    part: pumpkin_cluster::canonical_action::ChunkPart::ActorOccupancy, inventory: None };
                let root = snapshot.read(TruthView::Local, root_target).ok_or(ApplyFailure::Condition(root_target))?;
                let Value::ActorOccupancy(root) = &root.value else { return Err(ApplyFailure::Type(root_target)); };
                if !root.valid() || !root.is_complete() { return Err(ApplyFailure::Condition(root_target)); }
                for entry in root.entries.iter() {
                    participants.insert(entry.body_target());
                    if let pumpkin_cluster::actor_occupancy::OccupancyIdentity::Player(lease) = &entry.identity { seed_leases.push(*lease); }
                }
            }
        }
        seed_targets.extend(participants.iter().copied());
        let expanded_chunks = seed_targets.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }).collect::<std::collections::BTreeSet<_>>();
        snapshot = snapshot.select_scope(&expanded_chunks, &seed_targets.iter().copied().collect::<Vec<_>>(), &seed_leases, &[])?;
        let available_cuts = cuts.iter().filter(|cut| expanded_chunks.contains(&cut.chunk()) && !snapshot.original_store().contains(cut.chunk())).cloned().collect::<Vec<_>>();
        if !available_cuts.is_empty() { snapshot = snapshot.with_historical_chunks(&available_cuts)?; }
        for participant in participants {
            if snapshot.read(TruthView::Local, participant).is_none() { return Err(ApplyFailure::Condition(participant)); }
        }
        let candidates = snapshot.combat_targets(chunk.world, bounds)?;
        let mut contacts = Vec::new();
        let mut yaws = BTreeMap::new();
        for candidate in candidates {
            let target = candidate.contact.target();
            let cell = snapshot.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
            let living = match &cell.value { Value::Living(_) => true, Value::Entity(body) => matches!(body.payload, EntityPayload::Living(_)), _ => false };
            if !living { continue; }
            yaws.insert(candidate.contact.actor(), candidate.yaw);
            contacts.push(candidate.contact);
        }
        let victims = contacts.iter().filter(|contact| {
            if contact.world != chunk.world || contact.actor() == observation.actor() { return false; }
            let at = contact.position.map(f64::from_bits);
            let half = f64::from_bits(contact.half_width) + 0.3 + area;
            let height = f64::from_bits(contact.height);
            at[0] + half >= start[0].min(end[0]) && at[0] - half <= start[0].max(end[0])
                && at[2] + half >= start[2].min(end[2]) && at[2] - half <= start[2].max(end[2])
                && at[1] + height + 0.3 + area >= start[1].min(end[1]) && at[1] - 0.3 - area <= start[1].max(end[1])
        }).cloned().collect::<Vec<_>>();
        let victim_yaws = victims.iter().map(|victim| yaws.get(&victim.actor()).copied()
            .map(|yaw| (victim.actor(), yaw)).ok_or(ApplyFailure::InvalidAction)).collect::<Result<BTreeMap<_, _>, _>>()?;
        let mut equipment = victims.iter().filter_map(|victim| match victim.identity {
            pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => Some(pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                lease, victim.chunk, snapshot.inventory_equipment_attribute_ids())), _ => None,
        }).collect::<Vec<_>>();
        equipment.sort_by_key(|binding| binding.lease);
        let (rules, policy) = snapshot.world_rules_at(chunk.world, tick).ok_or(ApplyFailure::InvalidAction)?;
        blocks.retain(|target| !ProjectileTickIntent::outside_world_height(policy, *target));
        let shooter = if projectile.kind == pumpkin_cluster::entity_body::ProjectileKind::EnderPearl {
            let identity = match (&projectile.shooter, &projectile.shooter_birth, projectile.shooter_lease) {
                (ActorId::Player(player), None, Some(lease)) if *player == lease.gid => Some(pumpkin_cluster::item_contact::ContactIdentity::Player(lease)),
                (ActorId::Entity(id), Some(birth), None) => Some(pumpkin_cluster::item_contact::ContactIdentity::Entity { id: *id, birth: birth.clone() }),
                _ => None,
            };
            identity.map(|identity| snapshot.original_motion(&identity, tick).map(|sample| sample.contact)).transpose()?
        } else { None };
        let intent = ProjectileTickIntent { projectile: observation, original: effective, blocks, victims, victim_yaws, equipment,
            armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
            toughness_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id), rules, births: Vec::new(),
            status_attributes: snapshot.inventory_equipment_attribute_ids(), max_health_attribute: u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id), shooter };
        let targets = intent.targets().into_iter().collect::<Vec<_>>();
        let chunks = targets.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }).collect();
        let leases = intent.victims.iter().chain(intent.shooter.iter()).filter_map(|contact| match contact.identity {
            pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => Some(lease), _ => None }).collect::<Vec<_>>();
        let finite = snapshot.select_scope(&chunks, &targets, &leases, &[])?;
        jobs.push(FrozenProjectileTickJob { intent, snapshot: finite,
            draft: drafts.get(&(ActorId::Entity(body.id), body.id.generation, tick)).cloned() });
    }
    jobs.sort_by_key(FrozenProjectileTickJob::actor);
    FrozenActorTickFrame::new(source, tick, jobs).ok_or(ApplyFailure::InvalidAction)
}

pub(super) fn emit_frame(frame: &FrozenActorTickFrame<FrozenProjectileTickJob>,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    entities: &mut pumpkin_cluster::entity_origin::EntityOriginAllocator,
    capacity: usize) -> Result<ProjectileEmissionPage, ApplyFailure> {
    let mut page = ProjectileEmissionPage { completed_jobs: 0, actions: Vec::new(), samples: Vec::new() };
    for job in frame.remaining() {
        if capacity.saturating_sub(page.actions.len()) < 2 { break; }
        let Some(reservation) = clock.reserve(job.actor(), job.incarnation(), frame.tick, 2) else { break; };
        let result = (|| {
            let id = reservation.id(0, 16).ok_or(ApplyFailure::InvalidAction)?;
            let target = job.intent.projectile.target();
            let mut intent = job.intent.clone();
            let Some(Cell { value: Value::Entity(original), .. }) = job.snapshot.read(TruthView::Local, target) else { return Err(ApplyFailure::Type(target)); };
            if let EntityPayload::Projectile(projectile) = &original.payload
                && projectile.kind == pumpkin_cluster::entity_body::ProjectileKind::EnderPearl
                && intent.shooter.is_none()
                && let Some(lease) = projectile.shooter_lease
                && let Some((chunk, _)) = job.snapshot.player_association(lease) {
                return Err(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)));
            }
            let birth_count = match &original.payload { EntityPayload::Projectile(projectile)
                if projectile.kind == pumpkin_cluster::entity_body::ProjectileKind::ExperienceBottle => 3, _ => 0 };
            let cause = pumpkin_cluster::canonical_action::ActionRef { tick: frame.tick, id };
            let snapshot = if birth_count != 0 {
                intent.births = entities.reserve(frame.source, cause, birth_count).map_err(|_| ApplyFailure::Capacity)?.to_vec();
                job.snapshot.clone().with_reserved_projectile_births(entities, cause, &intent)?
            } else { job.snapshot.clone() };
            let pumpkin_cluster::item_contact::ContactIdentity::Entity { id: entity, birth } = &job.intent.projectile.identity else { return Err(ApplyFailure::InvalidAction); };
            let mut draft = match &job.draft { Some(draft) => draft.clone(), None => ActorActionDraft::new_entity(
                snapshot.as_ref(), FrozenEntityIssueAuthority { target, birth: birth.clone(), producer: frame.source,
                    controlled_owner: frame.source, effective_tick: frame.tick, next_sequence: id.sequence },
                frame.tick, 4096, 16384).map_err(|_| ApplyFailure::InvalidAction)? };
            let view = draft.view(snapshot.as_ref());
            let values: BTreeMap<Target, Cell> = intent.targets().into_iter()
                .filter_map(|target| view.read(TruthView::Local, target).map(|cell| (target, cell))).collect();
            let evaluated = super::cluster_projectile_tick::evaluate(&view, &intent, id, frame.tick, &values)?;
            let effective = evaluated.iter().find_map(|effect| match effect {
                Effect::Put { target: written, value: Value::Entity(body) } if *written == target => Some(body.spatial.clone()), _ => None });
            let action = pumpkin_cluster::projectile_tick::capture(&view, id, frame.tick, intent)?;
            draft.issue(snapshot.as_ref(), action).map_err(|_| ApplyFailure::InvalidAction)?;
            let mut actions = vec![(draft.actions().last().unwrap().clone(), draft.clone())];
            let mut sample = None;
            if let Some(effective) = effective {
                if !effective.belongs_to(job.intent.projectile.chunk) {
                    let Some(Cell { value: Value::Entity(body), .. }) = draft.view(snapshot.as_ref()).read(TruthView::Local, target) else { return Err(ApplyFailure::Type(target)); };
                    let transition = pumpkin_cluster::entity_transition::capture(&body, job.intent.projectile.chunk,
                        effective, frame.tick, reservation.id(1, 8).ok_or(ApplyFailure::InvalidAction)?)?;
                    draft.issue(snapshot.as_ref(), transition).map_err(|_| ApplyFailure::InvalidAction)?;
                    actions.push((draft.actions().last().unwrap().clone(), draft));
                } else {
                    sample = Some((job.intent.projectile.chunk, ItemSpatialSample { id: *entity, birth: birth.clone(), epoch: job.intent.projectile.epoch,
                        issuer: frame.source, tick: frame.tick, sequence: pumpkin_cluster::identity::PlayerSeq(job.intent.projectile.observed_sequence.0.wrapping_add(1)),
                        head_yaw: effective.yaw, spatial: effective }));
                }
            }
            Ok::<_, ApplyFailure>((actions, sample))
        })();
        match result {
            Ok((actions, sample)) => {
                page.actions.extend(actions); page.samples.extend(sample); page.completed_jobs += 1;
            }
            Err(reason) => { clock.release_unused(reservation); if page.completed_jobs == 0 { return Err(reason); } break; }
        }
    }
    Ok(page)
}

impl FrozenProjectileTickJob {
    pub(super) fn original_snapshot(&self) -> &Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot> { &self.snapshot }

    pub(super) fn with_historical_chunks(&mut self, cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut]) -> Result<(), ApplyFailure> {
        self.snapshot = self.snapshot.with_historical_chunks(cuts)?;
        Ok(())
    }
}
