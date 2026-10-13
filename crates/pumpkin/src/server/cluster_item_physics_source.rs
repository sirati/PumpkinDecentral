use std::sync::Arc;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame;
use pumpkin_cluster::canonical_action::{Action, ActionState, ApplyFailure, ChunkPart, Target, Value};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::item_spatial::ItemSpatialSample;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;
use super::cluster_item_physics::ItemPhysicsFailure;
use super::cluster_world_callback_snapshot::CallbackTickSnapshot;

pub(super) struct FrozenPhysicsJob {
    pub chunk: ChunkKey,
    pub body: EntityBody,
}

impl FrozenPhysicsJob {
    pub(super) fn actor(&self) -> (ActorId, u64) { (ActorId::Entity(self.body.id), self.body.id.generation) }
}

pub(super) struct FrozenPhysicsSource {
    pub frame: FrozenActorTickFrame<FrozenPhysicsJob>,
    snapshot: Arc<CallbackTickSnapshot>,
    collectors: std::collections::BTreeMap<ChunkKey, Vec<(pumpkin_cluster::canonical_action::LivePlayerLease, [f64; 3])>>,
}

pub(super) struct PhysicsEmissionPage {
    pub completed_jobs: usize,
    pub actions: Vec<Action>,
    pub samples: Vec<(ChunkKey, ItemSpatialSample)>,
}

pub(super) fn capture(snapshot: Arc<CallbackTickSnapshot>, source: PeerGeneration, tick: TickStamp)
    -> Result<FrozenPhysicsSource, ApplyFailure> {
    if snapshot.original_source_slot().is_none_or(|slot| slot as u16 != tick.0) { return Err(ApplyFailure::InvalidAction); }
    let mut jobs = Vec::new();
    for (chunk, body) in snapshot.captured_entity_bodies() {
        if !matches!(body.payload, EntityPayload::Item(_) | EntityPayload::Experience(_))
            || snapshot.scheduled_executor(chunk)? != source { continue; }
        if matches!(body.payload, EntityPayload::Experience(_)) && body.owner != source { return Err(ApplyFailure::InvalidAction); }
        let target = Target::Chunk { key: chunk, part: ChunkPart::Entity(body.id), inventory: None };
        let value = Value::Entity((body.clone()).into());
        snapshot.actor_occupancy_cut(target, &value)?;
        jobs.push(FrozenPhysicsJob { chunk, body: body.clone() });
    }
    jobs.sort_by_key(|job| job.body.id);
    let mut declared = std::collections::BTreeSet::new();
    let mut chunks = std::collections::BTreeSet::new();
    for job in &jobs {
        let target = Target::Chunk { key: job.chunk, part: ChunkPart::Entity(job.body.id), inventory: None };
        declared.insert(target);
        declared.insert(Target::Chunk { key: job.chunk, part: ChunkPart::ActorOccupancy, inventory: None });
        let sample = snapshot.original_entity_sample(job.body.id).ok_or(ApplyFailure::Condition(target))?;
        declared.extend(pumpkin_cluster::projectile_tick::ProjectileTickIntent::block_targets(&sample.spatial)?);
    }
    let mut leases = Vec::new();
    let mut collectors = std::collections::BTreeMap::<ChunkKey, Vec<(pumpkin_cluster::canonical_action::LivePlayerLease, [f64; 3])>>::new();
    for contact in snapshot.player_contacts(source, tick)? {
        let target = contact.target();
        declared.insert(target);
        if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = contact.identity { leases.push(lease); }
        let cell = snapshot.read(pumpkin_cluster::canonical_action::TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
        let Value::Living(living) = cell.value else { return Err(ApplyFailure::Type(target)); };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
        if living.combat.health_milli == 0 || avatar.game_mode == 3 { continue; }
        let mut position = contact.position.map(f64::from_bits);
        position[1] += f64::from(super::cluster_living_metadata::player_eye_height(&living));
        let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = contact.identity else { return Err(ApplyFailure::InvalidAction); };
        collectors.entry(contact.chunk).or_default().push((lease, position));
    }
    chunks.extend(declared.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }));
    let snapshot = snapshot.select_scope(&chunks, &declared.into_iter().collect::<Vec<_>>(), &leases, &[])?;
    Ok(FrozenPhysicsSource { frame: FrozenActorTickFrame::new(source, tick, jobs).ok_or(ApplyFailure::InvalidAction)?, snapshot, collectors })
}

fn failure(reason: ItemPhysicsFailure, job: &FrozenPhysicsJob) -> ApplyFailure {
    match reason {
        ItemPhysicsFailure::MissingChunk(key) => ApplyFailure::Condition(Target::Chunk { key, part: ChunkPart::Whole, inventory: None }),
        _ => ApplyFailure::Condition(Target::Chunk { key: job.chunk, part: ChunkPart::Entity(job.body.id), inventory: None }),
    }
}

pub(super) fn emit(source: &FrozenPhysicsSource, clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    capacity: usize) -> Result<PhysicsEmissionPage, ApplyFailure> {
    let mut selected = std::collections::BTreeSet::new();
    let mut prepared = Vec::new();
    let mut expected_actions = 0usize;
    let mut completed_jobs = 0usize;
    for job in source.frame.remaining().iter().take(32) {
        let prior = source.snapshot.original_entity_sample(job.body.id).ok_or(ApplyFailure::InvalidAction)?;
        let position = prior.spatial.position.map(f64::from_bits);
        let attraction = if matches!(job.body.payload, EntityPayload::Experience(_)) {
            let center_x = (position[0] / 16.0).floor() as i32;
            let center_z = (position[2] / 16.0).floor() as i32;
            (center_x - 1..=center_x + 1).flat_map(|x| {
                (center_z - 1..=center_z + 1).filter_map(move |z| {
                    source.collectors.get(&ChunkKey::new(job.chunk.world, x, z))
                })
            }).flatten().filter_map(|(lease, target)| {
                let distance = target.iter().zip(position).map(|(a, b)| (a-b).powi(2)).sum::<f64>();
                (distance.is_finite() && distance < 64.0).then_some((distance, *lease, *target))
            }).min_by(|a,b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1))).map(|(_, _, target)| target)
        } else { None };
        let step = match super::cluster_item_physics::simulate_with_attraction(&job.body, Some(prior), job.chunk,
            source.frame.source, source.frame.tick, source.snapshot.original_store(),
            job.chunk.world.dimension == pumpkin_data::dimension::Dimension::THE_NETHER.id, attraction) {
            Ok(step) => step,
            Err(ItemPhysicsFailure::StaleTick) => { completed_jobs += 1; continue; },
            Err(reason) => return Err(failure(reason, job)),
        };
        if !source.snapshot.original_store().contains(step.destination_chunk) {
            return Err(ApplyFailure::Condition(Target::Chunk { key: step.destination_chunk, part: ChunkPart::Whole, inventory: None }));
        }
        let below_void = source.snapshot.original_store().minimum_y(job.chunk).is_some_and(|minimum| position[1] < f64::from(minimum)-64.0);
        let damage = below_void || step.cactus_damage || source.frame.tick.0 % 20 == 0
            && (step.lava_damage || step.fire_damage)
            && !matches!(&job.body.payload, EntityPayload::Item(item) if item.fire_immune);
        let count = usize::from(damage) + usize::from(step.destination_chunk != job.chunk);
        if expected_actions + count > capacity { break; }
        if clock.available(ActorId::Entity(job.body.id), job.body.id.generation, source.frame.tick) < count as u32 { break; }
        prepared.push((job, step));
        expected_actions += count;
        selected.insert(job.body.id);
        completed_jobs += 1;
    }
    let mut reservations = Vec::new();
    let result = (|| {
        let mut actions = Vec::new();
        let mut samples = Vec::new();
        for (job, step) in prepared {
            let prior = source.snapshot.original_entity_sample(job.body.id).ok_or(ApplyFailure::InvalidAction)?;
            let position = prior.spatial.position.map(f64::from_bits);
            let below_void = source.snapshot.original_store().minimum_y(job.chunk).is_some_and(|minimum| position[1] < f64::from(minimum)-64.0);
            let fire_damage = (step.lava_damage || step.fire_damage) && source.frame.tick.0 % 20 == 0
                && matches!(&job.body.payload, EntityPayload::Item(item) if !item.fire_immune);
            let damage = if below_void && matches!(job.body.payload, EntityPayload::Item(_)) { Some((5_000, false)) }
                else if step.cactus_damage && matches!(job.body.payload, EntityPayload::Item(_)) { Some((1_000, false)) }
                else if fire_damage { Some((if step.lava_damage { 4_000 } else { 1_000 }, true)) } else { None };
            let hazard = if matches!(job.body.payload, EntityPayload::Experience(_)) {
                use pumpkin_cluster::experience_orb::ExperienceHazard as H;
                if below_void { Some(H::Void) } else if step.cactus_damage { Some(H::Cactus) }
                else if source.frame.tick.0 % 20 == 0 && step.lava_damage { Some(H::Lava) }
                else if source.frame.tick.0 % 20 == 0 && step.fire_damage { Some(H::Fire) } else { None }
            } else { None };
            let damaged = damage.is_some() || hazard.is_some();
            let crosses = step.destination_chunk != job.chunk;
            let count = u32::from(damaged) + u32::from(crosses);
            let reservation = if count > 0 {
                Some(clock.reserve(ActorId::Entity(job.body.id), job.body.id.generation, source.frame.tick, count).ok_or(ApplyFailure::Capacity)?)
            } else { None };
            if let Some(reservation) = reservation { reservations.push(reservation); }
            let reservation = (count > 0).then(|| reservations.last().unwrap());
            if let Some((amount_milli, fire)) = damage {
                let id = reservation.unwrap().id(0, 2).ok_or(ApplyFailure::InvalidAction)?;
                actions.push(pumpkin_cluster::item_lifecycle::lifecycle_action(&job.body, job.chunk, source.frame.source,
                    source.frame.source, source.frame.tick, id.sequence,
                    pumpkin_cluster::entity_body::DroppedItemChange::Damage { amount_milli, fire }).map_err(|_| ApplyFailure::InvalidAction)?);
            }
            if let Some(hazard) = hazard {
                let id = reservation.unwrap().id(0, 2).ok_or(ApplyFailure::InvalidAction)?;
                let target = Target::Chunk { key: job.chunk, part: ChunkPart::Entity(job.body.id), inventory: None };
                let cell = source.snapshot.read(pumpkin_cluster::canonical_action::TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                actions.push(pumpkin_cluster::experience_orb::capture_damage(id, source.frame.tick, &cell, job.chunk, hazard).map_err(|_| ApplyFailure::InvalidAction)?);
            }
            if crosses {
                let id = reservation.unwrap().id(u32::from(damaged), 8).ok_or(ApplyFailure::InvalidAction)?;
                actions.push(pumpkin_cluster::item_lifecycle::chunk_transition(&job.body, job.chunk, step.destination_chunk,
                    step.sample.spatial.clone(), source.frame.source, source.frame.tick, id.sequence).map_err(|_| ApplyFailure::InvalidAction)?);
            }
            samples.push((job.chunk, step.sample));
        }
        if actions.len() != expected_actions || samples.len() != selected.len() { return Err(ApplyFailure::InvalidAction); }
        Ok(PhysicsEmissionPage { completed_jobs, actions, samples })
    })();
    if result.is_err() {
        for reservation in reservations.into_iter().rev() { assert!(clock.release_unused(reservation)); }
    }
    result
}

impl FrozenPhysicsSource {
    pub(super) fn original_snapshot(&self) -> &Arc<CallbackTickSnapshot> { &self.snapshot }

    pub(super) fn with_historical_chunks(&mut self, cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut]) -> Result<(), ApplyFailure> {
        self.snapshot = self.snapshot.with_historical_chunks(cuts)?;
        Ok(())
    }
}
