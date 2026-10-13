use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{
    ActionRef, ActionState, ApplyFailure, ChunkPart, ScheduledTickFact, ScheduledTickOrigin,
    Target, TruthView, Value,
};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

use super::cluster_world_callback_snapshot::CallbackTickSnapshot;
use super::cluster_world_environment_callbacks::EnvironmentCallbackContext;

#[derive(Clone)]
pub(super) struct EnvironmentCategory {
    pub world: Arc<crate::world::World>,
    pub chunk: ChunkKey,
    pub random_ticks: bool,
}

struct RandomCursor {
    blocks: Arc<super::cluster_chunk_state::OwnedChunkBlocks>,
    chunk: ChunkKey,
    rng: u64,
    index: usize,
    count: usize,
    per_section: usize,
}

impl RandomCursor {
    fn new(
        blocks: Arc<super::cluster_chunk_state::OwnedChunkBlocks>,
        chunk: ChunkKey,
        source_slot: u64,
        per_section: usize,
    ) -> Result<Self, ApplyFailure> {
        let count = blocks
            .block_palettes()
            .len()
            .checked_mul(per_section)
            .ok_or(ApplyFailure::Capacity)?;
        chunk
            .x()
            .checked_mul(16)
            .and_then(|x| x.checked_add(15))
            .ok_or(ApplyFailure::InvalidAction)?;
        chunk
            .z()
            .checked_mul(16)
            .and_then(|z| z.checked_add(15))
            .ok_or(ApplyFailure::InvalidAction)?;
        let height = blocks
            .block_palettes()
            .len()
            .checked_mul(16)
            .and_then(|height| i32::try_from(height).ok())
            .ok_or(ApplyFailure::InvalidAction)?;
        blocks
            .min_y()
            .checked_add(height)
            .ok_or(ApplyFailure::InvalidAction)?;
        let rng = super::cluster_world_apply::CLUSTER_SEED
            ^ u64::from(chunk.world.key)
            ^ (u64::from(chunk.world.dimension) << 16)
            ^ source_slot.rotate_left(32)
            ^ (chunk.x() as u32 as u64).rotate_left(13)
            ^ (chunk.z() as u32 as u64).rotate_left(41);
        Ok(Self {
            blocks,
            chunk,
            rng,
            index: 0,
            count,
            per_section,
        })
    }

    fn step(&mut self) -> Option<(BlockPos, u64)> {
        if self.index == self.count {
            return None;
        }
        let section = self.index / self.per_section;
        self.index += 1;
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut sample = self.rng;
        sample = (sample ^ (sample >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        sample = (sample ^ (sample >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        sample ^= sample >> 31;
        let (x, y, z) = (
            (sample & 15) as usize,
            ((sample >> 4) & 15) as usize,
            ((sample >> 8) & 15) as usize,
        );
        let state = self.blocks.block_palettes()[section].get(x, y, z);
        if !pumpkin_data::block_properties::has_random_ticks(state) {
            return None;
        }
        Some((
            BlockPos {
                x: self.chunk.x() * 16 + x as i32,
                y: self.blocks.min_y() + (section * 16 + y) as i32,
                z: self.chunk.z() * 16 + z as i32,
            },
            sample,
        ))
    }
}

struct CategoryCursor {
    category: EnvironmentCategory,
    fluid_rules: crate::block::fluid::owned::FluidRules,
    fluid_rules_witness: pumpkin_cluster::world_rules::WorldRulesWitness,
    scheduled: VecDeque<ScheduledTickFact>,
    block_entities: VecDeque<BlockPos>,
    random: Option<RandomCursor>,
}

#[derive(Clone)]
enum SeedKind {
    BlockEntity(super::cluster_inventory_block_tick::FrozenBlockEntityTick),
    Scheduled(ScheduledTickFact),
    Random(u64),
}
struct Seed {
    category: usize,
    position: BlockPos,
    kind: SeedKind,
}

pub(super) struct EnvironmentSourceBank {
    source: PeerGeneration,
    tick: TickStamp,
    fixed: Arc<CallbackTickSnapshot>,
    categories: Vec<CategoryCursor>,
    category: usize,
    seed: Option<Seed>,
    drafts: Vec<(
        BTreeSet<pumpkin_cluster::canonical_action::Stakeholder>,
        ActorActionDraft,
    )>,
}

impl EnvironmentSourceBank {
    pub(super) fn capture(
        server: Arc<super::Server>,
        owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        source: PeerGeneration,
        tick: TickStamp,
        categories: Vec<EnvironmentCategory>,
        executors: &BTreeMap<ChunkKey, PeerGeneration>,
    ) -> Result<Self, ApplyFailure> {
        if categories.len() > 4096 || source.generation == 0 {
            return Err(ApplyFailure::Capacity);
        }
        let chunks = categories
            .iter()
            .map(|category| category.chunk)
            .collect::<BTreeSet<_>>();
        let targets = chunks
            .iter()
            .map(|&key| Target::Chunk {
                key,
                part: ChunkPart::Whole,
                inventory: None,
            })
            .collect::<Vec<_>>();
        let fixed = CallbackTickSnapshot::capture(
            server.clone(),
            owners,
            tick,
            &chunks,
            &targets,
            &[],
            &[],
        )?
        .with_scheduled_executors(executors);
        Self::capture_from_snapshot(fixed, server, owners, source, tick, categories, executors)
    }

    pub(super) fn capture_from_snapshot(
        fixed: Arc<CallbackTickSnapshot>,
        server: Arc<super::Server>,
        owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        source: PeerGeneration,
        tick: TickStamp,
        categories: Vec<EnvironmentCategory>,
        executors: &BTreeMap<ChunkKey, PeerGeneration>,
    ) -> Result<Self, ApplyFailure> {
        if categories.len() > 4096 || source.generation == 0 {
            return Err(ApplyFailure::Capacity);
        }
        if !std::ptr::eq(server.as_ref(), owners.server)
            || fixed
                .original_source_slot()
                .is_none_or(|slot| slot as u16 != tick.0)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let fixed = fixed.with_scheduled_executors(executors);
        let categories = categories
            .into_iter()
            .map(|category| {
                let target = Target::Chunk {
                    key: category.chunk,
                    part: ChunkPart::Whole,
                    inventory: None,
                };
                let cell = fixed
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?;
                let Value::ScheduledQueue(queue) = cell.value else {
                    return Err(ApplyFailure::Type(target));
                };
                let scheduled = queue
                    .into_iter()
                    .filter(|fact| !fact.due.is_after(tick) && fact.executor == source)
                    .collect();
                let (fluid_rules_witness, rules) = fixed
                    .world_rules_at(category.chunk.world, tick)
                    .ok_or(ApplyFailure::Condition(target))?;
                let random = if category.random_ticks {
                    let count = usize::try_from(rules.random_tick_speed)
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                    let blocks = fixed
                        .original_palettes(category.chunk)
                        .ok_or(ApplyFailure::Condition(target))?;
                    Some(RandomCursor::new(
                        blocks,
                        category.chunk,
                        fixed
                            .original_source_slot()
                            .ok_or(ApplyFailure::InvalidAction)?,
                        count,
                    )?)
                } else {
                    None
                };
                if category.random_ticks {
                    let phase = fixed.original_chunk_phase(category.chunk, &[])?;
                    let elected = phase
                        .holders
                        .get(&category.chunk)
                        .and_then(|holders| holders.intersection(&phase.primaries).copied().next());
                    if elected != Some(source) || executors.get(&category.chunk) != Some(&source) {
                        return Err(ApplyFailure::Condition(target));
                    }
                }
                Ok(CategoryCursor {
                    fluid_rules_witness,
                    block_entities: VecDeque::new(),
                    fluid_rules: crate::block::fluid::owned::FluidRules {
                        fast_lava: category.world.dimension.fast_lava,
                        water_source_conversion: rules.water_source_conversion,
                        lava_source_conversion: rules.lava_source_conversion,
                    },
                    category,
                    scheduled,
                    random,
                })
            })
            .collect::<Result<Vec<_>, ApplyFailure>>()?;
        Ok(Self {
            source,
            tick,
            fixed,
            categories,
            category: 0,
            seed: None,
            drafts: Vec::new(),
        })
    }

    pub(super) fn tick(&self) -> TickStamp {
        self.tick
    }
    pub(super) fn open_source_tick(
        &self,
        clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    ) -> bool {
        clock.open_tick(
            ActorId::Peer(self.source.server),
            self.source.generation,
            self.tick,
        )
    }
    pub(super) fn complete(&self) -> bool {
        self.category == self.categories.len() && self.seed.is_none()
    }

    pub(super) fn ordinal_exhausted(
        &self,
        clock: &pumpkin_cluster::action_issue::ActionIssueClock,
    ) -> bool {
        self.seed.is_some()
            && clock.available(
                ActorId::Peer(self.source.server),
                self.source.generation,
                self.tick,
            ) == 0
    }
    pub(super) fn prepare_category(
        &self,
        completed: &super::cluster_world_callback_work::CompletedCallbackWork,
    ) -> Result<(ActorActionDraft, bool), ApplyFailure> {
        self.validate_completed(completed)?;
        let footprint = self.category_footprint(completed.footprint());
        let mut actions = self
            .drafts
            .iter()
            .filter(|(group, _)| !group.is_disjoint(&footprint))
            .flat_map(|(_, draft)| draft.actions().iter())
            .cloned()
            .collect::<Vec<_>>();
        actions.sort_by(|left, right| {
            if left.id.sequence == right.id.sequence {
                std::cmp::Ordering::Equal
            } else if left.id.sequence.wrapping_sub(right.id.sequence) < 32768 {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        });
        actions.dedup_by_key(|action| action.id);
        let replay = actions.iter().any(|action| {
            !completed
                .draft()
                .actions()
                .iter()
                .any(|prior| prior.id == action.id)
        });
        let mut draft = ActorActionDraft::new(
            ActorId::Peer(self.source.server),
            self.source.generation,
            self.tick,
            4096,
            16384,
        );
        for action in actions {
            draft
                .issue(self.fixed.as_ref(), action)
                .map_err(|_| ApplyFailure::InvalidAction)?;
        }
        Ok((draft, replay))
    }

    pub(super) fn observe_category(
        &mut self,
        completed: &super::cluster_world_callback_work::CompletedCallbackWork,
    ) -> Result<(), ApplyFailure> {
        self.validate_completed(completed)?;
        let mut footprint = self.category_footprint(completed.footprint());
        let mut retained = Vec::with_capacity(self.drafts.len());
        for (group, draft) in self.drafts.drain(..) {
            if group.is_disjoint(&footprint) {
                retained.push((group, draft));
            } else {
                footprint.extend(group);
            }
        }
        self.drafts = retained;
        self.drafts.push((footprint, completed.draft().clone()));
        Ok(())
    }

    fn validate_completed(
        &self,
        completed: &super::cluster_world_callback_work::CompletedCallbackWork,
    ) -> Result<(), ApplyFailure> {
        if completed.action.id.actor != ActorId::Peer(self.source.server)
            || completed.action.id.incarnation != self.source.generation
            || completed.action.tick != self.tick
        {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }

    fn category_footprint(
        &self,
        mut footprint: BTreeSet<pumpkin_cluster::canonical_action::Stakeholder>,
    ) -> BTreeSet<pumpkin_cluster::canonical_action::Stakeholder> {
        loop {
            let before = footprint.len();
            for (group, _) in &self.drafts {
                if !group.is_disjoint(&footprint) {
                    footprint.extend(group.iter().copied());
                }
            }
            if footprint.len() == before {
                return footprint;
            }
        }
    }

    pub(super) fn dependency_roots(&self) -> Vec<ActionRef> {
        self.categories
            .iter()
            .flat_map(|category| category.scheduled.iter())
            .chain(self.seed.iter().filter_map(|seed| match &seed.kind {
                SeedKind::Scheduled(fact) => Some(fact),
                _ => None,
            }))
            .filter_map(|fact| match fact.origin {
                ScheduledTickOrigin::Action { cause, .. } => Some(cause),
                _ => None,
            })
            .collect()
    }

    fn draft_for_root(&self, root: ChunkKey) -> Result<ActorActionDraft, ApplyFailure> {
        let mut initial = BTreeSet::new();
        for x in -1..=1 {
            for z in -1..=1 {
                initial.insert(pumpkin_cluster::canonical_action::Stakeholder::Chunk(
                    ChunkKey::new(
                        root.world,
                        root.x().checked_add(x).ok_or(ApplyFailure::InvalidAction)?,
                        root.z().checked_add(z).ok_or(ApplyFailure::InvalidAction)?,
                    ),
                ));
            }
        }
        let footprint = self.category_footprint(initial);
        let mut actions = self
            .drafts
            .iter()
            .filter(|(group, _)| !group.is_disjoint(&footprint))
            .flat_map(|(_, draft)| draft.actions().iter())
            .cloned()
            .collect::<Vec<_>>();
        actions.sort_by(|left, right| {
            if left.id.sequence == right.id.sequence {
                std::cmp::Ordering::Equal
            } else if left.id.sequence.wrapping_sub(right.id.sequence) < 32768 {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        });
        actions.dedup_by_key(|action| action.id);
        let mut draft = ActorActionDraft::new(
            ActorId::Peer(self.source.server),
            self.source.generation,
            self.tick,
            4096,
            16384,
        );
        for action in actions {
            draft
                .issue(self.fixed.as_ref(), action)
                .map_err(|_| ApplyFailure::InvalidAction)?;
        }
        Ok(draft)
    }

    pub(super) fn advance(
        &mut self,
        work: &mut super::cluster_world_callback_work::CallbackWorkOwner,
        clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
        primary: bool,
        mut budget: usize,
    ) -> Result<(), ApplyFailure> {
        if !work.has_capacity() {
            return Ok(());
        }
        while self.seed.is_none() && self.category < self.categories.len() && budget > 0 {
            budget -= 1;
            if let Some(position) = self.categories[self.category]
                .block_entities
                .front()
                .copied()
            {
                let chunk = self.categories[self.category].category.chunk;
                let draft = self.draft_for_root(chunk)?;
                let slot = self
                    .fixed
                    .original_source_slot()
                    .ok_or(ApplyFailure::InvalidAction)?;
                let captured =
                    super::cluster_inventory_block_tick::capture_block_entity_tick_with_view(
                        self.fixed.clone(),
                        &draft.view(self.fixed.as_ref()),
                        chunk,
                        position,
                        self.tick,
                        slot,
                    )?;
                self.categories[self.category].block_entities.pop_front();
                if let Some(seed) = captured {
                    self.seed = Some(Seed {
                        category: self.category,
                        position,
                        kind: SeedKind::BlockEntity(seed),
                    });
                }
                continue;
            }
            let row = &mut self.categories[self.category];
            if let Some(fact) = row.scheduled.pop_front() {
                self.seed = Some(Seed {
                    category: self.category,
                    position: fact.position,
                    kind: SeedKind::Scheduled(fact),
                });
            } else if let Some(random) = &mut row.random {
                if random.index == random.count {
                    self.category += 1;
                } else if let Some((position, seed)) = random.step() {
                    self.seed = Some(Seed {
                        category: self.category,
                        position,
                        kind: SeedKind::Random(seed),
                    });
                }
            } else {
                self.category += 1;
            }
        }
        let Some(seed) = &self.seed else {
            return Ok(());
        };
        let category = &self.categories[seed.category].category;
        let environment = self
            .fixed
            .original_environment(category.chunk.world)
            .ok_or(ApplyFailure::Capacity)?;
        let Some(reservation) = clock.reserve(
            ActorId::Peer(self.source.server),
            self.source.generation,
            self.tick,
            1,
        ) else {
            return Err(ApplyFailure::Capacity);
        };
        let reference = ActionRef {
            id: reservation.id(0, 8).unwrap(),
            tick: self.tick,
        };
        let position = pumpkin_util::math::position::BlockPos::new(
            seed.position.x,
            seed.position.y,
            seed.position.z,
        );
        let context = match seed.kind.clone() {
            SeedKind::BlockEntity(block_seed) => {
                let draft = self.draft_for_root(category.chunk)?;
                if let Err(reason) = work.stage_block_entity(
                    block_seed,
                    reference,
                    self.source,
                    category.world.clone(),
                    category.chunk,
                    position,
                    self.fixed.clone(),
                    draft,
                ) {
                    assert!(clock.release_unused(reservation));
                    return Err(reason);
                }
                self.seed = None;
                return Ok(());
            }
            SeedKind::Scheduled(fact) => EnvironmentCallbackContext {
                reference,
                producer: self.source,
                world: category.world.clone(),
                chunk: category.chunk,
                position,
                root: if fact.kind == pumpkin_cluster::canonical_action::ScheduledTickKind::Fluid {
                    super::cluster_world_callbacks::CallbackRoot::FluidScheduled {
                        fluid: fact.id,
                        rules: self.categories[seed.category].fluid_rules,
                    }
                } else {
                    super::cluster_world_callbacks::CallbackRoot::Scheduled
                },
                scheduled: Some(fact),
                fluid_rules_witness: (fact.kind
                    == pumpkin_cluster::canonical_action::ScheduledTickKind::Fluid)
                    .then_some(self.categories[seed.category].fluid_rules_witness),
            },
            SeedKind::Random(seed) => EnvironmentCallbackContext::random(
                reference,
                self.source,
                category.world.clone(),
                category.chunk,
                position,
                seed,
                environment.as_ref(),
            ),
        };
        let loot = crate::world::loot::LootContextParameters {
            world_time: environment.time.world_age as u64,
            is_raining: Some(environment.weather.raining),
            is_thundering: Some(environment.weather.thundering),
            ..Default::default()
        };
        let root = pumpkin_cluster::canonical_action::Stakeholder::Chunk(category.chunk);
        let draft = self
            .drafts
            .iter()
            .find(|(group, _)| group.contains(&root))
            .map_or_else(
                || {
                    ActorActionDraft::new(
                        ActorId::Peer(self.source.server),
                        self.source.generation,
                        self.tick,
                        4096,
                        16384,
                    )
                },
                |(_, draft)| draft.clone(),
            );
        if let Err(reason) =
            work.stage_environment(context, self.fixed.clone(), draft, loot, primary)
        {
            assert!(clock.release_unused(reservation));
            return Err(reason);
        }
        self.seed = None;
        Ok(())
    }
}

pub(super) struct FrozenEnvironmentSourceInput {
    capture_pending: bool,
    seen_policy_revision: usize,
    original: Arc<CallbackTickSnapshot>,
    source: PeerGeneration,
    tick: TickStamp,
    categories: Vec<EnvironmentCategory>,
    executors: BTreeMap<ChunkKey, PeerGeneration>,
}

impl FrozenEnvironmentSourceInput {
    pub(super) fn original_snapshot(&self) -> &Arc<CallbackTickSnapshot> {
        &self.original
    }

    pub(super) fn new(
        original: Arc<CallbackTickSnapshot>,
        source: PeerGeneration,
        tick: TickStamp,
        categories: Vec<EnvironmentCategory>,
        executors: BTreeMap<ChunkKey, PeerGeneration>,
    ) -> Result<Self, ApplyFailure> {
        if original
            .original_source_slot()
            .is_none_or(|slot| slot as u16 != tick.0)
            || categories.len() > 4096
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let seen_policy_revision = original.world_rule_archive_revision();
        Ok(Self {
            original,
            source,
            tick,
            categories,
            executors,
            capture_pending: true,
            seen_policy_revision,
        })
    }
    pub(super) fn ready(&self) -> bool {
        self.capture_pending
    }
    pub(super) fn refresh_policy_archive(&mut self) -> Result<(), ApplyFailure> {
        let revision = self.original.published_world_rule_archive_revision();
        if revision == self.seen_policy_revision {
            return Ok(());
        }
        self.seen_policy_revision = revision;
        if let Some(original) = self.original.extend_world_rule_archive()? {
            self.original = original;
            self.capture_pending = true;
        }
        Ok(())
    }
    pub(super) fn tick(&self) -> TickStamp {
        self.tick
    }
    pub(super) fn capture(
        &mut self,
        server: Arc<super::Server>,
        owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    ) -> Result<EnvironmentSourceBank, ApplyFailure> {
        self.capture_pending = false;
        EnvironmentSourceBank::capture_from_snapshot(
            self.original.clone(),
            server,
            owners,
            self.source,
            self.tick,
            self.categories.clone(),
            &self.executors,
        )
    }
    pub(super) fn with_historical_chunks(
        &mut self,
        cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut],
    ) -> Result<(), ApplyFailure> {
        self.original = self.original.with_historical_chunks(cuts)?;
        self.capture_pending = true;
        Ok(())
    }
}
