use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pumpkin_cluster::canonical_action::{
    ActionContext, ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Effect, Target,
    TruthView, Value,
};
use pumpkin_cluster::protocol::ChunkKey;

use super::Server;
use super::cluster_chunk_state::OwnedChunkStore;

#[derive(Debug, Clone)]
pub(super) enum OriginalReliableVelocity {
    Living(pumpkin_cluster::combat_command::VelocityValues),
    EntityBody { velocity: [i64; 3], revision: u64 },
}

#[derive(Debug, Clone)]
pub(super) struct OriginalMotionSample {
    pub contact: pumpkin_cluster::item_contact::ContactObservation,
    pub velocity_micro: [i64; 3],
    pub canonical_velocity: OriginalReliableVelocity,
    pub previous_position: Option<[u64; 3]>,
}

#[derive(Clone)]
pub(super) struct CallbackTickSnapshot {
    backing_usage: Option<(usize, usize)>,
    original_directory: Option<Arc<pumpkin_cluster::chunks::Directory>>,
    original_local_source: Option<pumpkin_cluster::realm_completion::PeerGeneration>,
    players_by_chunk: Arc<BTreeMap<ChunkKey, BTreeSet<pumpkin_cluster::canonical_action::LivePlayerLease>>>,
    source_leases_by_gid: Arc<BTreeMap<pumpkin_cluster::identity::GlobalPlayerId, pumpkin_cluster::canonical_action::LivePlayerLease>>,
    phase_backing: Option<Arc<Self>>,
    original_phase: Option<Arc<pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness>>,
    historical_player_contacts: Arc<BTreeMap<pumpkin_cluster::canonical_action::LivePlayerLease, pumpkin_cluster::item_contact::ContactObservation>>,
    player_install_generations: Arc<BTreeMap<pumpkin_cluster::canonical_action::LivePlayerLease, u64>>,
    original_inventory_roots: Arc<BTreeSet<pumpkin_cluster::inventory::InventoryTarget>>,
    source_screens: Arc<std::collections::HashMap<pumpkin_cluster::inventory::InventoryScreenSession, pumpkin_cluster::inventory::InventoryScreenSnapshot>>,
    permission_policy: Option<Arc<super::cluster_permissions::FrozenPermissionPolicy>>,
    is_phase_backing: bool,
    original_tick: pumpkin_cluster::time::TickStamp,
    original_source_slot: Option<u64>,
    original_primary: Option<pumpkin_cluster::realm_completion::PeerGeneration>,
    recipes: Arc<Vec<pumpkin_protocol::codec::recipe::DynamicRecipe>>,
    world_rules: Arc<pumpkin_cluster::world_rules::WorldRulesHistory>,
    world_borders: Arc<pumpkin_cluster::world_time::WorldBorderHistory>,
    mornings: Arc<pumpkin_cluster::world_time::WorldMorningHistory>,
    scheduled_executors: Arc<BTreeMap<ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration>>,
    player_associations: Arc<BTreeMap<pumpkin_cluster::canonical_action::LivePlayerLease, (ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration)>>,
    entities_by_chunk: Arc<BTreeMap<ChunkKey, BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>>>,
    entity_occupancy_poses: Arc<BTreeMap<pumpkin_cluster::protocol::GlobalEntityId, (ChunkKey, pumpkin_cluster::actor_occupancy::ActorOccupancyPose)>>,
    entity_effective_spatial: Arc<BTreeMap<pumpkin_cluster::protocol::GlobalEntityId, pumpkin_cluster::entity_body::EntitySpatial>>,
    entity_samples: Arc<BTreeMap<pumpkin_cluster::protocol::GlobalEntityId, pumpkin_cluster::item_spatial::ItemSpatialSample>>,
    entity_previous_samples: Arc<BTreeMap<pumpkin_cluster::protocol::GlobalEntityId, pumpkin_cluster::item_spatial::ItemSpatialSample>>,
    issued_births: Arc<BTreeMap<pumpkin_cluster::protocol::GlobalEntityId, (pumpkin_cluster::canonical_action::ActionRef, pumpkin_cluster::entity_origin::EntityOriginIdentity)>>,
    peer_incarnations: Arc<std::collections::HashMap<pumpkin_cluster::identity::ServerId, u64>>,
    player_projections: Arc<BTreeMap<
        pumpkin_cluster::identity::GlobalPlayerId,
        (
            pumpkin_cluster::canonical_action::LivePlayerLease,
            super::cluster_world_actor::ActorSpatialState,
            ChunkKey,
            pumpkin_cluster::canonical_action::SpatialEpoch,
        ),
    >>,
    mob_rules:
        Arc<BTreeMap<pumpkin_cluster::protocol::WorldId, super::cluster_owned_mob_world::MobWorldRules>>,
    mob_environments: Arc<BTreeMap<pumpkin_cluster::protocol::WorldId, Arc<super::cluster_environment::EnvironmentView>>>,
    server: Arc<Server>,
    chunks: Arc<OwnedChunkStore>,
    versions: Arc<BTreeMap<Target, u64>>,
    sections: Arc<BTreeMap<(ChunkKey, i32), u64>>,
    facts: Arc<BTreeMap<Target, Option<Cell>>>,
    leases: Arc<Vec<pumpkin_cluster::canonical_action::LivePlayerLease>>,
    inventory_slots: Arc<BTreeMap<
        Target,
        (
            pumpkin_cluster::inventory::InventoryTarget,
            pumpkin_cluster::inventory::InvLoc,
        ),
    >>,
}

impl CallbackTickSnapshot {
    pub(super) fn world_rule_archive_revision(&self) -> usize { self.world_rules.revision() }
    pub(super) fn published_world_rule_archive_revision(&self) -> usize { self.server.cluster_world_rules.load().revision() }

    pub(super) fn extend_world_rule_archive(self: &Arc<Self>) -> Result<Option<Arc<Self>>, ApplyFailure> {
        let history = self.server.cluster_world_rules.load_full();
        if Arc::ptr_eq(&self.world_rules, &history) { return Ok(None); }
        if !history.is_extension_of(&self.world_rules) { return Err(ApplyFailure::InvalidAction); }
        if history.revision() == self.world_rules.revision() { return Ok(None); }
        let mut next = self.as_ref().clone();
        next.world_rules = history;
        Ok(Some(Arc::new(next)))
    }

    pub(super) fn location_light_plans(&self, location: pumpkin_cluster::world_command::BlockLocation) -> Result<Vec<pumpkin_cluster::world_action::WorldLightPlan>, ApplyFailure> { super::cluster_world_callback_light::location_light_plans(&self.server, location) }
    fn edit(snapshot: &mut Arc<Self>) -> &mut Self {
        let snapshot = Arc::make_mut(snapshot);
        snapshot.backing_usage = None;
        snapshot
    }


    pub(super) fn with_original_directory(mut self: Arc<Self>, directory: Arc<pumpkin_cluster::chunks::Directory>, local: pumpkin_cluster::realm_completion::PeerGeneration) -> Arc<Self> {
        if self.original_directory.as_ref().is_some_and(|original| Arc::ptr_eq(original,&directory)) && self.original_local_source == Some(local) { return self; }
        let next = Self::edit(&mut self);
        next.original_directory = Some(directory);
        next.original_local_source = Some(local);
        self
    }

    pub(super) fn original_directory(&self) -> Option<&Arc<pumpkin_cluster::chunks::Directory>> {
        self.original_directory.as_ref().or_else(|| self.phase_backing.as_ref()?.original_directory())
    }

    fn original_chunk_players(&self, key: ChunkKey) -> BTreeSet<pumpkin_cluster::canonical_action::LivePlayerLease> {
        let base = self.phase_backing.as_deref().unwrap_or(self);
        base.players_by_chunk.get(&key).into_iter().chain(self.players_by_chunk.get(&key)).flat_map(|players| players.iter().copied()).collect()
    }

    pub(super) fn original_frozen_holders(&self, key: ChunkKey) -> Result<std::collections::BTreeSet<pumpkin_cluster::realm_completion::PeerGeneration>, ApplyFailure> {
        let phase = self.original_phase.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let mut holders = phase.holders.get(&key).cloned().unwrap_or_default();
        let backing = self.phase_backing.as_deref().unwrap_or(self);
        if backing.chunks.contains(key) && let Some(local) = backing.original_local_source {
            if !phase.sources.contains(&local) { return Err(ApplyFailure::InvalidAction); }
            holders.insert(local);
        }
        if let Some(directory) = self.original_directory() {
            if let Some(peers) = directory.holders.get(&key) {
                for peer in peers {
                    let source = phase.sources.range(
                        pumpkin_cluster::realm_completion::PeerGeneration {server: pumpkin_cluster::identity::ServerId(*peer), generation: 0}
                        ..=pumpkin_cluster::realm_completion::PeerGeneration {server: pumpkin_cluster::identity::ServerId(*peer), generation: u64::MAX}
                    ).next().ok_or(ApplyFailure::InvalidAction)?;
                    holders.insert(*source);
                }
            }
        }
        Ok(holders)
    }

    pub(super) fn original_chunk_phase(&self, key: ChunkKey, leases: &[pumpkin_cluster::canonical_action::LivePlayerLease]) -> Result<pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness, ApplyFailure> {
        let phase = self.original_phase.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let mut players = BTreeMap::new();
        for lease in leases {
            let association = phase.players.get(lease).ok_or(ApplyFailure::InvalidAction)?;
            if association.0 != key { return Err(ApplyFailure::InvalidAction); }
            players.insert(*lease,*association);
        }
        Ok(pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness {
            full_source_slot: phase.full_source_slot, sources: phase.sources.clone(), primaries: phase.primaries.clone(),
            holders: BTreeMap::from([(key,self.original_frozen_holders(key)?)]), players,
        })
    }

    pub(super) fn original_chunk_player_frames(&self, key: ChunkKey) -> Result<Vec<pumpkin_cluster::actor_local_cut::ActorLocalPlayerFrame>, ApplyFailure> {
        self.original_chunk_players(key).into_iter().map(|lease| {
            let base = self.phase_backing.as_deref().unwrap_or(self);
            let (actual, spatial, association, epoch) = self.player_projections.get(&lease.gid).or_else(|| base.player_projections.get(&lease.gid)).ok_or(ApplyFailure::InvalidAction)?;
            if *actual != lease || *association != key { return Err(ApplyFailure::InvalidAction); }
            Ok(pumpkin_cluster::actor_local_cut::ActorLocalPlayerFrame { lease, association: *association, epoch: *epoch,
                encoded: postcard::to_stdvec(&spatial).map_err(|_| ApplyFailure::InvalidAction)? })
        }).collect()
    }

    pub(super) fn original_chunk_contacts(&self, key: ChunkKey) -> Result<Vec<pumpkin_cluster::item_contact::ContactObservation>, ApplyFailure> {
        self.original_chunk_players(key).into_iter().map(|lease| {
            let identity = pumpkin_cluster::item_contact::ContactIdentity::Player(lease);
            let original = if self.player_projections.contains_key(&lease.gid) { self } else { self.phase_backing.as_deref().unwrap_or(self) };
            let sample = original.original_motion(&identity, self.original_tick)?;
            if sample.contact.chunk != key { return Err(ApplyFailure::InvalidAction); }
            Ok(sample.contact)
        }).collect()
    }

    pub(super) fn inventory_source_recipes(&self) -> Arc<Vec<pumpkin_protocol::codec::recipe::DynamicRecipe>> { self.recipes.clone() }

    pub(super) fn inventory_source_screen(&self, session: pumpkin_cluster::inventory::InventoryScreenSession) -> Option<pumpkin_cluster::inventory::InventoryScreenSnapshot> {
        self.source_screens.get(&session).cloned()
    }
    pub(super) fn inventory_source_permission(&self) -> Option<&super::cluster_permissions::FrozenPermissionPolicy> {
        self.permission_policy.as_deref()
    }
    pub(super) fn inventory_source_lease(&self, gid: pumpkin_cluster::identity::GlobalPlayerId) -> Option<pumpkin_cluster::canonical_action::LivePlayerLease> {
        self.source_leases_by_gid.get(&gid).copied().or_else(|| self.phase_backing.as_ref()?.inventory_source_lease(gid))
    }
    pub(super) fn inventory_source_preview_trusted(&self, screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot) -> bool {
        let (Some(lease), Some(authority), Some(preview)) = (screen.preview_lease, screen.preview_authority.as_ref(), screen.preview.as_ref()) else { return false; };
        matches!(preview.target, pumpkin_cluster::inventory::InventoryTarget::Player(gid) if gid == lease.gid)
            && screen.preview_incarnation == Some(lease.incarnation)
            && authority.is_valid(lease, &screen.affected)
            && self.peer_incarnations.get(&authority.provider.server).copied() == Some(authority.provider.generation)
    }


    pub(super) fn player_install_generation(&self, lease: pumpkin_cluster::canonical_action::LivePlayerLease) -> Option<u64> {
        self.player_install_generations.get(&lease).copied()
    }

    pub(super) fn historical_player_contact(&self, lease: pumpkin_cluster::canonical_action::LivePlayerLease) -> Option<&pumpkin_cluster::item_contact::ContactObservation> {
        self.historical_player_contacts.get(&lease)
    }

    pub(super) fn with_historical_chunks(self: &Arc<Self>, cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut]) -> Result<Arc<Self>, ApplyFailure> {
        if self.is_phase_backing { return Err(ApplyFailure::InvalidAction); }
        let phase = self.original_phase.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let mut next = self.as_ref().clone();
        for cut in cuts {
            let key = cut.chunk();
            if cut.full_source_slot() != phase.full_source_slot || cut.phase().sources != phase.sources
                || cut.phase().primaries != phase.primaries
                || cut.phase().holders.iter().any(|(chunk, holders)| !self.original_frozen_holders(*chunk).is_ok_and(|expected| &expected == holders))
                || cut.phase().players.iter().any(|(lease, association)| phase.players.get(lease) != Some(association))
                || !self.original_frozen_holders(key)?.contains(&cut.provider())
                || next.chunks.contains(key) {
                return Err(ApplyFailure::InvalidAction);
            }
            if !Arc::make_mut(&mut next.chunks).merge_missing_actor_chunk(cut.original_store(), key) {
                return Err(ApplyFailure::InvalidAction);
            }
            for (target, cell) in cut.local_cells() {
                if next.facts.get(target).is_some_and(|existing| existing != cell) {
                    return Err(ApplyFailure::Condition(*target));
                }
                if let Some(cell) = cell {
                    if next.versions.get(target).is_some_and(|revision| *revision != cell.revision) {
                        return Err(ApplyFailure::Condition(*target));
                    }
                    Arc::make_mut(&mut next.versions).insert(*target, cell.revision);
                    if let Target::Chunk { key, part: ChunkPart::BlockSection(y), inventory: None } = target {
                        Arc::make_mut(&mut next.sections).insert((*key, *y), cell.revision);
                    }
                    if let Value::Inventory(item) = &cell.value {
                        use pumpkin_cluster::canonical_action::InventoryId;
                        use pumpkin_cluster::inventory::InventoryTarget;
                        let owner = match target {
                            Target::Chunk { key, part: ChunkPart::Block(position), inventory: Some(InventoryId::Block { .. }) } => Some(InventoryTarget::Block { chunk: *key, position: *position }),
                            Target::Chunk { part: ChunkPart::Entity(id), inventory: Some(InventoryId::Entity { .. }), .. } => Some(InventoryTarget::Entity(pumpkin_cluster::inventory::EntityInventoryId { origin: id.origin, local_id: id.local_id, generation: id.generation })),
                            Target::Player { lease: Some(lease), inventory: Some(InventoryId::Player { .. }), .. } => Some(InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation }),
                            Target::Player { lease: Some(lease), inventory: Some(InventoryId::Virtual { host, kind, session, .. }), .. } => Some(InventoryTarget::LeasedVirtual { host: *host, key: lease.key.0, player: lease.gid, incarnation: lease.incarnation, session: *session, kind: *kind }),
                            _ => None,
                        }.ok_or(ApplyFailure::InvalidAction)?;
                        if next.inventory_slots.get(target).is_some_and(|existing| *existing != (owner, item.loc)) { return Err(ApplyFailure::InvalidAction); }
                        Arc::make_mut(&mut next.inventory_slots).insert(*target, (owner, item.loc));
                    }
                }
                Arc::make_mut(&mut next.facts).entry(*target).or_insert_with(|| cell.clone());
            }
            for contact in cut.player_contacts() {
                let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = &contact.identity else { return Err(ApplyFailure::InvalidAction); };
                let lease = *lease;
                if contact.chunk != key || contact.simulation_tick != self.original_tick
                    || phase.players.get(&lease) != Some(&(key, contact.owner))
                    || next.player_projections.get(&lease.gid).is_some_and(|(actual, spatial, chunk, epoch)| *actual != lease || *chunk != key || *epoch != contact.epoch || spatial.frame.effective.position.map(f64::to_bits) != contact.position || spatial.frame.effective.source_seq != contact.observed_sequence)
                    || next.historical_player_contacts.get(&lease).is_some_and(|existing| existing != contact)
                    || next.player_associations.get(&lease).is_some_and(|association| *association != (key, contact.owner)) {
                    return Err(ApplyFailure::InvalidAction);
                }
                Arc::make_mut(&mut next.historical_player_contacts).entry(lease).or_insert_with(|| contact.clone());
                Arc::make_mut(&mut next.player_associations).entry(lease).or_insert((key, contact.owner));
                if !next.leases.contains(&lease) { Arc::make_mut(&mut next.leases).push(lease); }
            }
            for (lease, spatial, association, epoch) in cut.player_projections() {
                if next.player_projections.get(&lease.gid).is_some_and(|previous| previous != &(*lease, *spatial, *association, *epoch)) { return Err(ApplyFailure::InvalidAction); }
                Arc::make_mut(&mut next.player_projections).entry(lease.gid).or_insert((*lease, *spatial, *association, *epoch));
                Arc::make_mut(&mut next.players_by_chunk).entry(*association).or_default().insert(*lease);
                Arc::make_mut(&mut next.source_leases_by_gid).insert(lease.gid, *lease);
            }
            for sample in cut.entity_samples() {
                if sample.tick != self.original_tick || next.entity_effective_spatial.get(&sample.id).is_some_and(|spatial| spatial != &sample.spatial) {
                    return Err(ApplyFailure::InvalidAction);
                }
                if next.entity_samples.get(&sample.id).is_some_and(|original| original != sample) { return Err(ApplyFailure::InvalidAction); }
                Arc::make_mut(&mut next.entity_samples).entry(sample.id).or_insert_with(|| sample.clone());
                Arc::make_mut(&mut next.entities_by_chunk).entry(key).or_default().insert(sample.id);
                Arc::make_mut(&mut next.entity_effective_spatial).entry(sample.id).or_insert_with(|| sample.spatial.clone());
            }
        }
        Arc::make_mut(&mut next.leases).sort_unstable();
        Arc::make_mut(&mut next.leases).dedup();
        next.resource_usage()?;
        Ok(Arc::new(next))
    }


    pub(super) fn with_original_phase(mut self: Arc<Self>, phase: Arc<pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness>) -> Result<Arc<Self>, ApplyFailure> {
        if !phase.valid() || self.original_source_slot != Some(phase.full_source_slot)
            || self.original_phase.as_ref().is_some_and(|previous| previous.as_ref() != phase.as_ref())
            || self.leases.iter().any(|lease| !phase.players.contains_key(lease)) {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.original_phase.as_ref().is_some_and(|previous| Arc::ptr_eq(previous, &phase)) { return Ok(self); }
        let next = Self::edit(&mut self);
        Arc::make_mut(&mut next.source_leases_by_gid).extend(phase.players.keys().map(|lease| (lease.gid, *lease)));
        next.original_phase = Some(phase);
        let usage = self.backing_resource_usage()?;
        Self::edit(&mut self).backing_usage = Some(usage);
        Ok(self)
    }

    pub(super) fn original_phase(&self) -> Option<&Arc<pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness>> {
        self.original_phase.as_ref()
    }


    pub(super) fn resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        if self.is_phase_backing { return Ok((0, 0)); }
        let payload = (&self.facts, &self.versions, &self.sections, &self.leases, &self.inventory_slots,
            &self.player_associations, &self.player_projections, &self.historical_player_contacts, &self.player_install_generations, &self.scheduled_executors, &self.entity_occupancy_poses, &self.entity_effective_spatial, (&self.entity_samples, &self.entity_previous_samples, &self.issued_births), &self.entities_by_chunk, (&self.players_by_chunk, &self.source_leases_by_gid));
        let records = pumpkin_cluster::logical_records::count(&payload)?
            .checked_add(self.mob_rules.len())
            .ok_or(ApplyFailure::ResourceLimit)?;
        let bytes = postcard::experimental::serialized_size(&payload).map_err(|_| ApplyFailure::InvalidAction)?
            .checked_add(self.mob_rules.len().checked_mul(std::mem::size_of::<super::cluster_owned_mob_world::MobWorldRules>()).ok_or(ApplyFailure::ResourceLimit)?)
            .ok_or(ApplyFailure::ResourceLimit)?;
        pumpkin_cluster::canonical_action::check_logical_budget(records, bytes)?;
        Ok((records, bytes))
    }


    pub(super) fn backing_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        let base = self.phase_backing.as_deref().unwrap_or(self);
        if let Some(usage) = base.backing_usage { return Ok(usage); }
        let mut records = [base.facts.len(), base.versions.len(), base.sections.len(), base.leases.len(), base.inventory_slots.len(),
            base.player_projections.len(), base.player_associations.len(), base.entity_occupancy_poses.len(), base.entity_effective_spatial.len(), base.entity_samples.len(), base.entity_previous_samples.len(), base.issued_births.len(), base.entities_by_chunk.len(),
            base.historical_player_contacts.len(), base.player_install_generations.len(), base.source_screens.len(), base.players_by_chunk.len(), base.source_leases_by_gid.len()]
            .into_iter().try_fold(0usize, |count, next| count.checked_add(next).ok_or(ApplyFailure::ResourceLimit))?;
        let mut bytes = postcard::experimental::serialized_size(&(&base.facts, &base.versions, &base.sections, &base.leases,
            &base.inventory_slots, &base.player_associations, &base.historical_player_contacts, &base.player_install_generations,
            &base.scheduled_executors, &base.entity_occupancy_poses, &base.entity_effective_spatial, (&base.entity_samples, &base.entity_previous_samples, &base.issued_births), &base.entities_by_chunk, &base.original_phase, &base.original_inventory_roots, (&base.players_by_chunk, &base.source_leases_by_gid)))
            .map_err(|_| ApplyFailure::InvalidAction)?;
        for projection in base.player_projections.values() {
            bytes = bytes.checked_add(postcard::experimental::serialized_size(projection).map_err(|_| ApplyFailure::InvalidAction)?).ok_or(ApplyFailure::ResourceLimit)?;
        }
        for screen in base.source_screens.values() {
            let metadata = (screen.session, screen.sync_id, screen.state_id, screen.window_kind, screen.container_slots,
                &screen.cursor, &screen.affected, &screen.preview, screen.preview_incarnation, screen.preview_lease, &screen.preview_authority);
            records = records.checked_add(pumpkin_cluster::logical_records::count(&metadata)?).ok_or(ApplyFailure::ResourceLimit)?;
            bytes = bytes.checked_add(postcard::experimental::serialized_size(&metadata).map_err(|_| ApplyFailure::InvalidAction)?).ok_or(ApplyFailure::ResourceLimit)?;
            for slot in &screen.slots {
                let value = (slot.screen_slot, slot.slot, &slot.cell, slot.limit);
                records = records.checked_add(pumpkin_cluster::logical_records::count(&value)?).ok_or(ApplyFailure::ResourceLimit)?;
                bytes = bytes.checked_add(postcard::experimental::serialized_size(&value).map_err(|_| ApplyFailure::InvalidAction)?).ok_or(ApplyFailure::ResourceLimit)?;
            }
        }
        Ok((records, bytes))
    }

    pub(super) fn with_reserved_menu_inventory(mut self: Arc<Self>,
        source: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        admission: &pumpkin_cluster::canonical_menu::MenuVirtualInventoryAdmission,
    ) -> Result<Arc<Self>, ApplyFailure> {
        if !std::ptr::eq(self.server.as_ref(), source.server)
            || !self.leases.contains(&admission.lease)
            || admission.session == 0 || self.original_inventory_roots.contains(&admission.inventory_target()) { return Err(ApplyFailure::InvalidAction); }
        let entries = admission.entries();
        if entries.iter().any(|(target, _)| self.read(TruthView::Local, *target).is_some()) { return Err(ApplyFailure::InvalidAction); }
        let next = Self::edit(&mut self);
        for (target, item) in entries {
            Arc::make_mut(&mut next.facts).insert(target, None);
            Arc::make_mut(&mut next.inventory_slots).insert(target, (admission.inventory_target(), item.loc));
        }
        self.resource_usage()?;
        Ok(self)
    }

    pub(super) fn with_reserved_generated_menu_inventory(mut self: Arc<Self>,
        admission: &pumpkin_cluster::canonical_menu::MenuVirtualInventoryAdmission,
        reference: pumpkin_cluster::canonical_action::ActionRef,
        full_source_slot: u64,
    ) -> Result<Arc<Self>, ApplyFailure> {
        let identity = pumpkin_cluster::canonical_menu_identity::MenuPresentationIdentity::from_source(reference, full_source_slot)
            .ok_or(ApplyFailure::InvalidAction)?;
        if self.original_tick != reference.tick || self.original_source_slot != Some(full_source_slot)
            || admission.session != identity.session || reference.id.actor != pumpkin_cluster::actor::ActorId::Player(admission.lease.gid)
            || reference.id.incarnation != admission.lease.incarnation || !self.leases.contains(&admission.lease)
            || self.original_inventory_roots.contains(&admission.inventory_target()) {
            return Err(ApplyFailure::InvalidAction);
        }
        let entries = admission.entries();
        if entries.iter().any(|(target, _)| self.facts.contains_key(target) || self.read(TruthView::Local, *target).is_some()) {
            return Err(ApplyFailure::InvalidAction);
        }
        let next = Self::edit(&mut self);
        for (target, item) in entries {
            Arc::make_mut(&mut next.facts).insert(target, None);
            Arc::make_mut(&mut next.inventory_slots).insert(target, (admission.inventory_target(), item.loc));
        }
        self.resource_usage()?;
        Ok(self)
    }

    pub(super) fn original_tick(&self) -> pumpkin_cluster::time::TickStamp { self.original_tick }

    pub(super) fn original_source_slot(&self) -> Option<u64> { self.original_source_slot }

    pub(super) fn original_environment(&self, world: pumpkin_cluster::protocol::WorldId)
        -> Option<Arc<super::cluster_environment::EnvironmentView>> {
        super::cluster_environment::environment_at_source(&self.server, world, self.original_primary?, self.original_source_slot?)
    }

    fn bind_issued_births(this: &mut Arc<Self>, cause: pumpkin_cluster::canonical_action::ActionRef,
        identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity]) -> Result<(), ApplyFailure> {
        if cause.tick != this.original_tick || this.original_source_slot.is_none_or(|slot| slot as u16 != cause.tick.0)
            || identities.iter().any(|identity| this.issued_births.get(&identity.id).is_some_and(|actual| actual != &(cause, identity.clone()))) {
            return Err(ApplyFailure::InvalidAction);
        }
        Arc::make_mut(&mut Self::edit(this).issued_births).extend(identities.iter().map(|identity| (identity.id, (cause, identity.clone()))));
        Ok(())
    }

    pub(super) fn with_reserved_projectile_births(mut self: Arc<Self>,
        allocator: &pumpkin_cluster::entity_origin::EntityOriginAllocator,
        cause: pumpkin_cluster::canonical_action::ActionRef,
        intent: &pumpkin_cluster::projectile_tick::ProjectileTickIntent,
    ) -> Result<Arc<Self>, ApplyFailure> {
        if !intent.valid(cause.id, cause.tick) || allocator.source() != intent.projectile.owner {
            return Err(ApplyFailure::InvalidAction);
        }
        let identities = allocator.reservation(cause).ok_or(ApplyFailure::InvalidAction)?;
        if identities != intent.births.as_slice() { return Err(ApplyFailure::InvalidAction); }
        let targets = intent.birth_targets();
        if self.facts.len().saturating_add(targets.len()) > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS
            || targets.iter().any(|target| self.read(TruthView::Local, *target).is_some()) {
            return Err(ApplyFailure::InvalidAction);
        }
        Self::bind_issued_births(&mut self, cause, identities)?;
        Arc::make_mut(&mut Self::edit(&mut self).facts).extend(targets.into_iter().map(|target| (target, None)));
        self.resource_usage()?;
        Ok(self)
    }
    pub(super) fn world_border_at(&self, world: pumpkin_cluster::protocol::WorldId, tick: pumpkin_cluster::time::TickStamp) -> Option<pumpkin_cluster::world_time::WorldBorderWitness> {
        if tick != self.original_tick { return None; }
        let issuer = self.original_primary?;
        let slot = self.original_source_slot?;
        self.world_borders.current(world, issuer, slot).map(|snapshot| snapshot.witness(slot))
    }

    pub(super) fn world_spawn_at(&self, world: pumpkin_cluster::protocol::WorldId, tick: pumpkin_cluster::time::TickStamp) -> Option<&pumpkin_cluster::world_time::SpawnState> {
        if tick != self.original_tick { return None; }
        let issuer = self.original_primary?;
        let slot = self.original_source_slot?;
        self.world_borders.current(world, issuer, slot).map(|snapshot| &snapshot.spawn)
    }

    pub(super) fn world_rules_at(&self, world: pumpkin_cluster::protocol::WorldId, tick: pumpkin_cluster::time::TickStamp)
        -> Option<(pumpkin_cluster::world_rules::WorldRulesWitness, &pumpkin_cluster::world_rules::WorldRulesSnapshot)> {
        if tick != self.original_tick { return None; }
        let issuer = self.original_primary?;
        let slot = self.original_source_slot?;
        self.world_rules.snapshots().filter(|snapshot| snapshot.rules.world == world && snapshot.rules.issuer == issuer)
            .filter(|snapshot| self.world_rules.resolve(pumpkin_cluster::world_rules::WorldRulesWitness { rules: snapshot.rules, slot }, tick).is_ok())
            .max_by_key(|snapshot| snapshot.rules.epoch)
            .map(|snapshot| (pumpkin_cluster::world_rules::WorldRulesWitness { rules: snapshot.rules, slot }, snapshot))
    }

    pub(super) fn with_reserved_births(
        mut self: Arc<Self>,
        allocator: &pumpkin_cluster::entity_origin::EntityOriginAllocator,
        cause: pumpkin_cluster::canonical_action::ActionRef,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        planned: &[(ChunkKey, pumpkin_cluster::protocol::GlobalEntityId)],
    ) -> Result<Arc<Self>, ApplyFailure> {
        let reservations = allocator.reservation(cause).ok_or(ApplyFailure::InvalidAction)?;
        if allocator.source() != source || planned.len() > reservations.len()
            || self.facts.len().saturating_add(planned.len()) > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS
        { return Err(ApplyFailure::InvalidAction); }
        let mut targets = BTreeMap::new();
        for &(chunk, entity) in planned {
            if !reservations.iter().any(|identity| identity.id == entity && identity.source == source)
                || targets.insert(Target::Chunk { key: chunk, part: ChunkPart::Entity(entity), inventory: None }, None).is_some()
            { return Err(ApplyFailure::InvalidAction); }
        }
        if targets.keys().any(|target| self.facts.get(target).is_some_and(Option::is_some)) {
            return Err(ApplyFailure::InvalidAction);
        }
        Self::bind_issued_births(&mut self, cause, reservations)?;
        Arc::make_mut(&mut Self::edit(&mut self).facts).extend(targets);
        self.resource_usage()?;
        Ok(self)
    }
    pub(super) fn with_reserved_admissions(
        mut self: Arc<Self>,
        allocator: &pumpkin_cluster::entity_origin::EntityOriginAllocator,
        cause: pumpkin_cluster::canonical_action::ActionRef,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        admissions: &[pumpkin_cluster::mob_breeding::MobBirthAdmission],
    ) -> Result<Arc<Self>, ApplyFailure> {
        let identities = allocator.reservation(cause).ok_or(ApplyFailure::InvalidAction)?;
        if allocator.source() != source || admissions.len() > identities.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut targets = BTreeMap::new();
        for admission in admissions {
            let body = &admission.body;
            if body.birth.action != cause || body.birth.source != source
                || admission.inventory.imported_revision != 0 || !admission.inventory.valid()
                || !body.belongs_to(body.id, admission.chunk)
                || !identities.iter().any(|identity| identity.id == body.id
                    && identity.uuid == body.birth.uuid && identity.source == source) {
                return Err(ApplyFailure::InvalidAction);
            }
            let expected = super::cluster_owned_mob_birth::inventory_layout(body)?;
            let actual = admission.inventory.cells.iter().map(|cell| match &cell.value {
                Value::Inventory(item) => {
                    if !item.is_empty() {
                        let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
                            .map_err(|_| ApplyFailure::InvalidAction)?;
                        if item.count > super::cluster_owned_mob_birth::inventory_slot_limit(body, &stack, item.loc)? {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                    Ok(item.loc)
                },
                _ => Err(ApplyFailure::InvalidAction),
            }).collect::<Result<Vec<_>, _>>()?;
            if expected.into_iter().collect::<BTreeSet<_>>() != actual.into_iter().collect::<BTreeSet<_>>() {
                return Err(ApplyFailure::InvalidAction);
            }
            for target in admission.targets() {
                if targets.insert(target, None).is_some()
                    || self.facts.get(&target).is_some_and(Option::is_some) {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        if self.facts.len().saturating_add(targets.len()) > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS { return Err(ApplyFailure::ResourceLimit); }
        Self::bind_issued_births(&mut self, cause, identities)?;
        Arc::make_mut(&mut Self::edit(&mut self).facts).extend(targets);
        self.resource_usage()?;
        Ok(self)
    }
    pub(super) fn mob_environment(&self, world: pumpkin_cluster::protocol::WorldId)
        -> Option<Arc<super::cluster_environment::EnvironmentView>> {
        self.original_environment(world)
    }
    pub(super) fn with_scheduled_executors(
        mut self: Arc<Self>,
        executors: &BTreeMap<ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration>,
    ) -> Arc<Self> {
        let executors = executors.iter().filter(|(chunk, _)| self.is_phase_backing || self.chunks.contains(**chunk))
            .map(|(chunk, peer)| (*chunk, *peer)).collect::<BTreeMap<_, _>>();
        if *self.scheduled_executors == executors { return self; }
        Self::edit(&mut self).scheduled_executors = Arc::new(executors);
        self
    }
    pub(super) fn with_player_associations(mut self: Arc<Self>,
        associations: BTreeMap<pumpkin_cluster::canonical_action::LivePlayerLease, (ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration)>,
    ) -> Arc<Self> {
        if *self.player_associations == associations { return self; }
        Self::edit(&mut self).player_associations = Arc::new(associations);
        self
    }
    pub(super) fn with_entity_occupancy_poses(mut self: Arc<Self>, spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial) -> Result<Arc<Self>, ApplyFailure> {
        let mut poses = BTreeMap::new();
        let mut effective = BTreeMap::new();
        let mut samples = BTreeMap::new();
        let mut previous_samples = BTreeMap::new();
        let mut entities_by_chunk: BTreeMap<ChunkKey, BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>> = BTreeMap::new();
        for (chunk, cell) in self.chunks.local_entity_bodies() {
            let Value::Entity(body) = &cell.value else { return Err(ApplyFailure::InvalidAction); };
            let Some(sample) = spatial.get(body.id).filter(|sample| sample.matches_body(body)) else { continue; };
            if sample.tick.is_after(self.original_tick) { return Err(ApplyFailure::InvalidAction); }
            let projected = spatial.project(body);
            effective.insert(body.id, projected.spatial.clone());
            samples.insert(body.id, sample.clone());
            if let Some(previous) = spatial.previous_sample(body.id).filter(|previous| previous.matches_body(body) && sample.tick.is_after(previous.tick)) {
                previous_samples.insert(body.id, previous.clone());
            }
            entities_by_chunk.entry(chunk).or_default().insert(body.id);
            let Some(Cell { value: Value::ActorOccupancy(root), .. }) = self.chunks.occupancy_root(false, chunk) else { continue; };
            let Some(entry) = root.entries.get(&pumpkin_cluster::actor::ActorId::Entity(body.id)) else { continue; };
            if entry.identity != pumpkin_cluster::actor_occupancy::OccupancyIdentity::Entity(body.birth.clone())
                || entry.spatial_epoch != body.spatial_epoch { return Err(ApplyFailure::InvalidAction); }
            let sequence = sample.sequence.0;
            let pose = pumpkin_cluster::actor_occupancy::ActorOccupancyPose { entry: entry.clone(), position: projected.spatial.position, sample_tick: sample.tick, simulation_sequence: sequence };
            if entry.association != chunk || entry.owner != body.owner || !pose.valid() { continue; }
            poses.insert(body.id, (chunk, pose));
        }
        let encoded = postcard::experimental::serialized_size(&poses).map_err(|_| ApplyFailure::InvalidAction)?
            .checked_add(postcard::experimental::serialized_size(&effective).map_err(|_| ApplyFailure::InvalidAction)?)
            .and_then(|bytes| postcard::experimental::serialized_size(&(&samples, &previous_samples)).ok().and_then(|extra| bytes.checked_add(extra)))
            .ok_or(ApplyFailure::ResourceLimit)?;
        if !self.is_phase_backing { pumpkin_cluster::canonical_action::check_logical_budget(effective.len(), encoded)?; }
        let next = Self::edit(&mut self);
        next.entity_occupancy_poses = Arc::new(poses);
        next.entity_effective_spatial = Arc::new(effective);
        next.entity_samples = Arc::new(samples);
        next.entity_previous_samples = Arc::new(previous_samples);
        next.entities_by_chunk = Arc::new(entities_by_chunk);
        self.resource_usage()?;
        Ok(self)
    }

    pub(super) fn captured_entity_bodies(&self) -> impl Iterator<Item = (ChunkKey, &pumpkin_cluster::entity_body::EntityBody)> {
        self.chunks.local_entity_bodies().filter_map(|(chunk, cell)| match &cell.value {
            Value::Entity(body) => Some((chunk, body.as_ref())), _ => None,
        })
    }

    pub(super) fn original_motion(&self, identity: &pumpkin_cluster::item_contact::ContactIdentity,
        tick: pumpkin_cluster::time::TickStamp) -> Result<OriginalMotionSample, ApplyFailure> {
        use pumpkin_cluster::actor::ActorId;
        use pumpkin_cluster::item_contact::{ContactIdentity, ContactObservation};
        use pumpkin_cluster::living_property::LivingProperty;
        if tick != self.original_tick { return Err(ApplyFailure::InvalidAction); }
        let to_micro = |velocity: [f64; 3]| -> Result<[i64; 3], ApplyFailure> {
            let mut result = [0; 3];
            for (target, value) in result.iter_mut().zip(velocity) {
                let value = (value * 1_000_000.0).round();
                if !value.is_finite() || value < i64::MIN as f64 || value >= i64::MAX as f64 { return Err(ApplyFailure::InvalidAction); }
                *target = value as i64;
            }
            Ok(result)
        };
        let (contact, velocity_micro, body_velocity) = match identity {
            ContactIdentity::Player(lease) => {
                let (actual, spatial, chunk, epoch) = self.player_projections.get(&lease.gid).ok_or(ApplyFailure::InvalidAction)?;
                if actual != lease { return Err(ApplyFailure::InvalidAction); }
                let (association, owner) = self.player_associations.get(lease).copied().ok_or(ApplyFailure::InvalidAction)?;
                if association != *chunk { return Err(ApplyFailure::InvalidAction); }
                let target = Target::Chunk { key: *chunk, part: ChunkPart::Player(lease.gid), inventory: None };
                let cell = self.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                let Value::Living(living) = cell.value else { return Err(ApplyFailure::Type(target)); };
                let dimensions = super::cluster_living_metadata::player_dimensions(&living, super::cluster_living_metadata::player_pose(&living));
                let contact = ContactObservation::player(*lease, owner, *epoch, *chunk, spatial.frame.effective,
                    f64::from(dimensions.width) / 2.0, f64::from(dimensions.height), tick).ok_or(ApplyFailure::InvalidAction)?;
                (contact, to_micro(spatial.frame.effective.velocity)?, None)
            }
            ContactIdentity::Entity { id, birth } => {
                let chunk = self.chunks.entity_body_chunk(false, *id).ok_or(ApplyFailure::InvalidAction)?;
                let target = Target::Chunk { key: chunk, part: ChunkPart::Entity(*id), inventory: None };
                let cell = self.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                let Value::Entity(body) = &cell.value else { return Err(ApplyFailure::Type(target)); };
                if body.birth != *birth { return Err(ApplyFailure::InvalidAction); }
                let spatial = self.entity_effective_spatial.get(id).unwrap_or(&body.spatial);
                let shape = super::cluster_world_canonical::actor_collision_shape(target, &cell.value)?;
                let minimum = shape.offsets[0].map(f64::from_bits);
                let maximum = shape.offsets[1].map(f64::from_bits);
                let sequence = self.entity_occupancy_poses.get(id).map_or(0, |(_, pose)| pose.simulation_sequence);
                let contact = ContactObservation::entity(&body, chunk, spatial,
                    (maximum[0] - minimum[0]) / 2.0, maximum[1] - minimum[1], tick,
                    pumpkin_cluster::identity::PlayerSeq(sequence)).ok_or(ApplyFailure::InvalidAction)?;
                (contact, spatial.velocity_micro, (!matches!(&body.payload, pumpkin_cluster::entity_body::EntityPayload::Living(_))).then_some((body.spatial.velocity_micro, cell.revision)))
            }
        };
        let actor = match identity { ContactIdentity::Player(lease) => ActorId::Player(lease.gid),
            ContactIdentity::Entity { id, .. } => ActorId::Entity(*id) };
        let canonical_velocity = if let Some((velocity, revision)) = body_velocity {
            OriginalReliableVelocity::EntityBody { velocity, revision }
        } else {
            let mut velocity = [0; 3];
            let mut revisions = [0; 3];
            for (axis, property) in [LivingProperty::VelocityX, LivingProperty::VelocityY, LivingProperty::VelocityZ].into_iter().enumerate() {
                let target = Target::Chunk { key: contact.chunk, part: ChunkPart::LivingProperty { actor, property }, inventory: None };
                let cell = self.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                let Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Signed(value)) = cell.value else { return Err(ApplyFailure::Type(target)); };
                velocity[axis] = value;
                revisions[axis] = cell.revision;
            }
            OriginalReliableVelocity::Living(pumpkin_cluster::combat_command::VelocityValues { velocity, revisions })
        };
        let previous_position = match identity {
            ContactIdentity::Player(lease) => self.player_projections.get(&lease.gid).and_then(|(_, spatial, _, _)| spatial.previous_position),
            ContactIdentity::Entity { id, birth } => self.original_entity_previous_sample(*id)
                .filter(|sample| sample.birth == *birth && sample.epoch == contact.epoch && tick.is_after(sample.tick))
                .map(|sample| sample.spatial.position),
        };
        Ok(OriginalMotionSample { contact, velocity_micro,
            canonical_velocity, previous_position })
    }

    pub(super) fn combat_target(&self, identity: &pumpkin_cluster::item_contact::ContactIdentity)
        -> Result<super::cluster_combat_directory::CombatClientTarget, ApplyFailure> {
        use pumpkin_cluster::item_contact::{ContactIdentity, ContactObservation};
        match identity {
            ContactIdentity::Player(lease) => {
                let contact = self.original_motion(identity, self.original_tick)?.contact;
                let target = contact.target();
                let cell = self.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                let Value::Living(living) = cell.value else { return Err(ApplyFailure::Type(target)); };
                let (actual, spatial, chunk, epoch) = self.player_projections.get(&lease.gid)
                    .ok_or(ApplyFailure::Condition(target))?;
                if actual != lease || *chunk != contact.chunk || *epoch != contact.epoch {
                    return Err(ApplyFailure::Condition(target));
                }
                Ok(super::cluster_combat_directory::CombatClientTarget {
                    tags: Arc::new(living.tags.clone()), contact,
                    kind: pumpkin_data::entity::EntityType::PLAYER.id,
                    game_mode: living.avatar.as_ref().map(|avatar| avatar.game_mode),
                    yaw: spatial.frame.effective.yaw.to_bits(), on_ground: spatial.frame.effective.on_ground,
                })
            }
            ContactIdentity::Entity { id, birth } => {
                let chunk = self.chunks.entity_body_chunk(false, *id).ok_or(ApplyFailure::InvalidAction)?;
                let target = Target::Chunk { key: chunk, part: ChunkPart::Entity(*id), inventory: None };
                let cell = self.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                let Value::Entity(body) = &cell.value else { return Err(ApplyFailure::Type(target)); };
                if body.id != *id || body.birth != *birth { return Err(ApplyFailure::Condition(target)); }
                let spatial = self.entity_effective_spatial(*id).unwrap_or(&body.spatial);
                let shape = super::cluster_world_canonical::actor_collision_shape(target, &cell.value)?;
                let minimum = shape.offsets[0].map(f64::from_bits);
                let maximum = shape.offsets[1].map(f64::from_bits);
                let sequence = self.entity_occupancy_poses.get(id).map_or(0, |(_, pose)| pose.simulation_sequence);
                let contact = ContactObservation::entity(body, chunk, spatial,
                    (maximum[0] - minimum[0]) / 2.0, maximum[1] - minimum[1], self.original_tick,
                    pumpkin_cluster::identity::PlayerSeq(sequence)).ok_or(ApplyFailure::InvalidAction)?;
                Ok(super::cluster_combat_directory::CombatClientTarget {
                    tags: Arc::new(body.tags.clone()), contact, kind: body.kind, game_mode: None,
                    yaw: spatial.yaw, on_ground: spatial.on_ground,
                })
            }
        }
    }

    pub(super) fn combat_targets(&self, world: pumpkin_cluster::protocol::WorldId,
        bounds: [[f64; 2]; 2]) -> Result<Vec<super::cluster_combat_directory::CombatClientTarget>, ApplyFailure> {
        use pumpkin_cluster::actor_occupancy::OccupancyIdentity;
        use pumpkin_cluster::item_contact::ContactIdentity;
        if !(0..2).all(|axis| bounds[0][axis].is_finite() && bounds[1][axis].is_finite()
            && bounds[0][axis] < bounds[1][axis] && bounds[0][axis].abs() <= 30_000_000.0
            && bounds[1][axis].abs() <= 30_000_000.0) { return Err(ApplyFailure::InvalidAction); }
        let minimum = bounds[0].map(|coordinate| (coordinate / 16.0).floor() as i32);
        let maximum = bounds[1].map(|coordinate| (coordinate / 16.0).floor() as i32);
        let count = i64::from(maximum[0] - minimum[0] + 1)
            .checked_mul(i64::from(maximum[1] - minimum[1] + 1)).ok_or(ApplyFailure::ResourceLimit)?;
        if count > 64 { return Err(ApplyFailure::ResourceLimit); }
        let mut entries = BTreeMap::new();
        for x in minimum[0]..=maximum[0] {
            for z in minimum[1]..=maximum[1] {
                let witness = self.capture_actor_occupancy(ChunkKey::new(world, x, z), Vec::new())?;
                for pose in witness.poses {
                    let entry = pose.entry;
                    if [0, 1].into_iter().any(|axis| f64::from_bits(entry.shape.offsets[0][axis]) >= f64::from_bits(entry.shape.offsets[1][axis])) { continue; }
                    if entries.get(&entry.actor).is_some_and(|previous| previous != &entry) {
                        return Err(ApplyFailure::Condition(entry.body_target()));
                    }
                    entries.insert(entry.actor, entry);
                }
            }
        }
        entries.into_values().map(|entry| {
            let identity = match entry.identity {
                OccupancyIdentity::Player(lease) => ContactIdentity::Player(lease),
                OccupancyIdentity::Entity(birth) => {
                    let pumpkin_cluster::actor::ActorId::Entity(id) = entry.actor else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    ContactIdentity::Entity { id, birth }
                }
            };
            self.combat_target(&identity)
        }).collect()
    }

    pub(super) fn original_entity_previous_sample(&self, id: pumpkin_cluster::protocol::GlobalEntityId) -> Option<&pumpkin_cluster::item_spatial::ItemSpatialSample> {
        self.entity_previous_samples.get(&id).or_else(|| self.phase_backing.as_ref().and_then(|base| base.entity_previous_samples.get(&id)))
    }

    pub(super) fn original_entity_sample(&self, id: pumpkin_cluster::protocol::GlobalEntityId) -> Option<&pumpkin_cluster::item_spatial::ItemSpatialSample> {
        self.entity_samples.get(&id).or_else(|| self.phase_backing.as_ref().and_then(|base| base.entity_samples.get(&id)))
    }

    pub(super) fn entity_effective_spatial(&self, id: pumpkin_cluster::protocol::GlobalEntityId) -> Option<&pumpkin_cluster::entity_body::EntitySpatial> {
        self.entity_effective_spatial.get(&id)
    }

    pub(super) fn entity_occupancy_pose(&self, id: pumpkin_cluster::protocol::GlobalEntityId) -> Option<&pumpkin_cluster::actor_occupancy::ActorOccupancyPose> {
        self.entity_occupancy_poses.get(&id).map(|(_, pose)| pose)
    }

    pub(super) fn capture_actor_occupancy(&self, key: ChunkKey, blocks: Vec<pumpkin_cluster::actor_occupancy::CollisionBox>) -> Result<pumpkin_cluster::actor_occupancy::ActorOccupancyWitness, ApplyFailure> {
        let target = Target::Chunk { key, part: ChunkPart::ActorOccupancy, inventory: None };
        let Some(Cell { revision, value: Value::ActorOccupancy(root) }) = self.chunks.occupancy_root(false, key) else { return Err(ApplyFailure::Condition(target)); };
        if !root.valid() || !root.is_complete() { return Err(ApplyFailure::Condition(target)); }
        let mut poses = Vec::with_capacity(root.entries.len());
        for entry in root.entries.iter() {
            let pose = match &entry.identity {
                pumpkin_cluster::actor_occupancy::OccupancyIdentity::Player(lease) => {
                    let Some((actual, spatial, chunk, epoch)) = self.player_projections.get(&lease.gid) else { return Err(ApplyFailure::Condition(entry.body_target())); };
                    if actual != lease || *chunk != entry.association || *epoch != entry.spatial_epoch || self.player_association(*lease) != Some((entry.association, entry.owner))
                        || !entry.covers_chunk(key) { return Err(ApplyFailure::Condition(entry.body_target())); }
                    pumpkin_cluster::actor_occupancy::ActorOccupancyPose { entry: entry.clone(), position: spatial.frame.effective.position.map(f64::to_bits), sample_tick: spatial.frame.effective.tick, simulation_sequence: spatial.frame.effective.source_seq.0 }
                }
                pumpkin_cluster::actor_occupancy::OccupancyIdentity::Entity(birth) => {
                    let pumpkin_cluster::actor::ActorId::Entity(id) = entry.actor else { return Err(ApplyFailure::InvalidAction); };
                    let Some((chunk, pose)) = self.entity_occupancy_poses.get(&id) else { return Err(ApplyFailure::Condition(entry.body_target())); };
                    if *chunk != entry.association || !entry.covers_chunk(key) || pose.entry != *entry || !matches!(&pose.entry.identity, pumpkin_cluster::actor_occupancy::OccupancyIdentity::Entity(actual) if actual == birth) { return Err(ApplyFailure::Condition(entry.body_target())); }
                    pose.clone()
                }
            };
            poses.push(pose);
        }
        let witness = pumpkin_cluster::actor_occupancy::ActorOccupancyWitness { key, revision: *revision, poses, blocks };
        if !witness.valid() { return Err(ApplyFailure::InvalidAction); }
        Ok(witness)
    }

    pub(super) fn player_relation_participant(&self, lease: pumpkin_cluster::canonical_action::LivePlayerLease)
        -> Option<pumpkin_cluster::actor_relation::ActorRelationParticipant> {
        let (actual, _, chunk, epoch) = self.player_projections.get(&lease.gid)?;
        if *actual != lease { return None; }
        let owner = if let Some((association, owner)) = self.player_associations.get(&lease) {
            if association != chunk { return None; }
            *owner
        } else {
            pumpkin_cluster::realm_completion::PeerGeneration { server: lease.gid.server,
                generation: *self.peer_incarnations.get(&lease.gid.server)? }
        };
        Some(pumpkin_cluster::actor_relation::ActorRelationParticipant {
            identity: pumpkin_cluster::item_contact::ContactIdentity::Player(lease), chunk: *chunk, owner, epoch: *epoch })
    }

    pub(super) fn player_association(&self, lease: pumpkin_cluster::canonical_action::LivePlayerLease)
        -> Option<(ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration)> {
        self.player_associations.get(&lease).copied()
    }
    pub(super) fn player_projection(
        &self,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
    ) -> Option<&super::cluster_world_actor::ActorSpatialState> {
        self.player_projections
            .get(&gid)
            .map(|(_, spatial, _, _)| spatial)
    }
    pub(super) fn peer_incarnations(
        &self,
    ) -> &Arc<std::collections::HashMap<pumpkin_cluster::identity::ServerId, u64>> {
        &self.peer_incarnations
    }
    pub(super) fn player_contacts(
        &self,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<Vec<pumpkin_cluster::item_contact::ContactObservation>, ApplyFailure> {
        self.player_projections
            .values()
            .map(|(lease, spatial, chunk, epoch)| {
                let generation = if lease.gid.server == source.server {
                    source.generation
                } else {
                    *self
                        .peer_incarnations
                        .get(&lease.gid.server)
                        .ok_or(ApplyFailure::InvalidAction)?
                };
                let height = if spatial.crawling || spatial.swimming || spatial.gliding {
                    0.6
                } else if spatial.sneaking {
                    1.5
                } else {
                    1.8
                };
                let target = Target::Chunk { key: *chunk, part: ChunkPart::Player(lease.gid), inventory: None };
                let cell = self.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
                let Value::Living(living) = cell.value else { return Err(ApplyFailure::Type(target)); };
                let scale = f64::from(super::cluster_living_metadata::player_scale(&living));
                pumpkin_cluster::item_contact::ContactObservation::player(
                    *lease,
                    pumpkin_cluster::realm_completion::PeerGeneration {
                        server: lease.gid.server,
                        generation,
                    },
                    *epoch,
                    *chunk,
                    spatial.frame.effective,
                    0.3 * scale,
                    height * scale,
                    tick,
                )
                .ok_or(ApplyFailure::InvalidAction)
            })
            .collect()
    }
    pub(super) fn original_store(&self) -> &Arc<OwnedChunkStore> {
        &self.chunks
    }
    pub(super) fn observed_players(
        &self,
    ) -> impl Iterator<
        Item = (
            pumpkin_cluster::canonical_action::LivePlayerLease,
            ChunkKey,
            &pumpkin_cluster::item_action::ActorLivingState,
        ),
    > {
        self.player_projections
            .values()
            .filter_map(|(lease, _, key, _)| {
                let cell = self.facts.get(&Target::Chunk {
                    key: *key,
                    part: ChunkPart::Player(lease.gid),
                    inventory: None,
                })?;
                let Some(Cell {
                    value: Value::Living(living),
                    ..
                }) = cell
                else {
                    return None;
                };
                Some((*lease, *key, living.as_ref()))
            })
    }
    pub(super) fn captured_inventory_slot(
        &self,
        target: Target,
    ) -> Option<(pumpkin_cluster::inventory::InventoryTarget, pumpkin_cluster::inventory::InvLoc)> {
        if let Some(slot) = self.inventory_slots.get(&target) { return Some(*slot); }
        let base = self.phase_backing.as_deref()?;
        let slot = base.inventory_slots.get(&target)?;
        base.original_inventory_roots.contains(&slot.0).then_some(*slot)
    }
    pub(super) fn captured_inventory_targets(
        &self,
    ) -> impl Iterator<
        Item = (
            Target,
            pumpkin_cluster::inventory::InventoryTarget,
            pumpkin_cluster::inventory::InvLoc,
        ),
    > + '_ {
        self.inventory_slots
            .iter()
            .map(|(target, (inventory, location))| (*target, *inventory, *location))
    }
    pub(super) fn mob_rules(
        &self,
        world: pumpkin_cluster::protocol::WorldId,
    ) -> Option<super::cluster_owned_mob_world::MobWorldRules> {
        let (_, original) = self.world_rules_at(world, self.original_tick)?;
        let mut rules = self.mob_rules.get(&world).copied()?;
        rules.mob_griefing = original.mob_griefing;
        rules.difficulty = original.difficulty;
        rules.minimum_y = original.min_y;
        rules.maximum_y = original.min_y.checked_add(original.height as i32)?;
        Some(rules)
    }
    pub(super) fn capture_actor_phase(
        server: Arc<Server>,
        source: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<Arc<Self>, ApplyFailure> {
        if !std::ptr::eq(server.as_ref(), source.server) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut facts = BTreeMap::new();
        let mut leases = Vec::new();
        let peer_incarnations = server.cluster_peer_incarnations.load_full();
        let mut player_projections = BTreeMap::new();
        for (gid, projection, association, epoch) in
            source.actors.player_observations(source.living)
        {
            let chunk = source
                .actors
                .phase_spatial(gid, false)
                .ok_or(ApplyFailure::InvalidAction)?
                .chunk;
            let lease = source
                .inventory
                .player_lease_for_gid(gid)
                .or_else(|| source.actors.remote_lease(gid))
                .filter(|lease| source.validate_live_player_lease(*lease))
                .ok_or(ApplyFailure::InvalidAction)?;
            if gid.server.0 == server.advanced_config.cluster.server_id
                && !source
                    .inventory
                    .player_install_generation(gid)
                    .is_some_and(|generation| {
                        server
                            .cluster_controller
                            .active_for_simulation(lease, generation)
                    })
            {
                continue;
            }
            let target = Target::Chunk {
                key: chunk,
                part: ChunkPart::Player(gid),
                inventory: None,
            };
            let cell = source
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            facts.insert(target, Some(cell));
            leases.push(lease);
            player_projections.insert(gid, (lease, projection, association, epoch));
        }
        let mut inventory_slots = BTreeMap::new();
        for inventory in source.inventory.actor_phase_inventory_snapshots() {
            for item in inventory.cells {
                let slot =
                    pumpkin_cluster::inventory::InventorySlotRef::new(inventory.target, item.loc);
                let target = super::cluster_world_inventory_actions::binding_target(
                    source.inventory,
                    None,
                    slot,
                )
                .ok_or(ApplyFailure::InvalidAction)?;
                let cell = source
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?;
                facts.insert(target, Some(cell));
                inventory_slots.insert(target, (inventory.target, item.loc));
            }
        }
        for (lease, _, chunk, _) in player_projections.values() {
            let body_target = Target::Chunk { key: *chunk, part: ChunkPart::Player(lease.gid), inventory: None };
            let Some(Some(Cell { value: Value::Living(living), .. })) = facts.get(&body_target) else { continue; };
            let Some((reference, cause)) = living.menu.last_close.zip(living.combat.timing.death_cause) else { continue; };
            let Some(close) = source.inventory.retained_death_close(*lease, reference, cause) else { continue; };
            if !living.menu.closed_sessions.iter().any(|closed| closed.action == reference && closed.session == close.session.session && closed.window_kind == close.window_kind) {
                return Err(ApplyFailure::InvalidAction);
            }
            let menu = living.menu.clone();
            for binding in close.bindings() {
                if source.read(TruthView::Local, binding.target).is_some() { continue; }
                if !source.inventory.certifies_retained_closed_slot(*lease, reference, cause, &menu, &binding) {
                    return Err(ApplyFailure::Condition(binding.target));
                }
                facts.insert(binding.target, None);
                inventory_slots.insert(binding.target, (binding.slot.target, binding.slot.loc));
            }
        }
        let versions = source
            .versions
            .iter()
            .filter_map(|(&(view, target), &revision)| {
                (view == TruthView::Local && matches!(target, Target::Chunk { .. }))
                    .then_some((target, revision))
            })
            .collect();
        let mob_environments = BTreeMap::new();
        let mob_rules = server
            .worlds
            .load()
            .iter()
            .map(|world| {
                (
                    super::cluster_world_delta::world_id(world),
                    super::cluster_owned_mob_world::MobWorldRules {
                        mob_griefing: false,
                        difficulty: 0,
                        minimum_y: world.get_bottom_y(),
                        maximum_y: world.get_top_y(),
                        sea_level: world.sea_level,
                    },
                )
            })
            .collect();
        let mut sections = BTreeMap::new();
        for key in source.blocks.owned_chunks() {
            let palette = source
                .blocks
                .snapshot(key)
                .ok_or(ApplyFailure::InvalidAction)?;
            for index in 0..palette.block_palettes().len() {
                let y = palette
                    .min_y()
                    .div_euclid(16)
                    .checked_add(index.try_into().map_err(|_| ApplyFailure::InvalidAction)?)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let target = Target::Chunk {
                    key,
                    part: ChunkPart::BlockSection(y),
                    inventory: None,
                };
                let cell = source
                    .blocks
                    .block_section(false, key, y)
                    .ok_or(ApplyFailure::Condition(target))?;
                if !matches!(&cell.value, Value::BlockSection(values) if values.len() == 4096) {
                    return Err(ApplyFailure::Type(target));
                }
                sections.insert((key, y), cell.revision);
            }
        }
        let snapshot = Arc::new(Self {
            original_directory: None,
            original_local_source: None,
            players_by_chunk: Arc::new(player_projections.values().fold(BTreeMap::new(), |mut chunks: BTreeMap<ChunkKey, BTreeSet<_>>, (lease, _, chunk, _)| { chunks.entry(*chunk).or_default().insert(*lease); chunks })),
            source_leases_by_gid: Arc::new(leases.iter().map(|lease| (lease.gid, *lease)).collect()),
            backing_usage: None,
            entities_by_chunk: Arc::new(BTreeMap::new()),
            phase_backing: None,
            original_inventory_roots: Arc::new(source.inventory.original_inventory_root_targets()),
            source_screens: server.cluster_inventory_screens.load_full(),
            permission_policy: super::cluster_permissions::freeze_permission_policy(&server).map(Arc::new),
            original_phase: None,
            historical_player_contacts: Arc::new(BTreeMap::new()),
            player_install_generations: Arc::new(leases.iter().filter_map(|lease| source.inventory.player_install_generation(lease.gid).map(|generation| (*lease, generation))).collect()),
            is_phase_backing: true,
            original_tick: tick,
            original_source_slot: super::cluster_world_rules::slot_for_tick(tick),
            original_primary: super::cluster_world_rules::primary(&server),
            recipes: server.recipe_manager.cluster_dynamic_recipes_snapshot(),
            world_rules: server.cluster_world_rules.load_full(),
            world_borders: server.cluster_world_borders.load_full(),
            mornings: server.cluster_mornings.load_full(),
            scheduled_executors: Arc::new(BTreeMap::new()),
            player_associations: Arc::new(BTreeMap::new()),
            entity_occupancy_poses: Arc::new(BTreeMap::new()),
            entity_effective_spatial: Arc::new(BTreeMap::new()),
            entity_samples: Arc::new(BTreeMap::new()),
            entity_previous_samples: Arc::new(BTreeMap::new()),
            issued_births: Arc::new(BTreeMap::new()),
            peer_incarnations,
            player_projections: Arc::new(player_projections),
            mob_rules: Arc::new(mob_rules),
            mob_environments: Arc::new(mob_environments),
            server,
            chunks: Arc::new(
                source
                    .blocks
                    .callback_subset(&source.blocks.owned_chunks().collect()),
            ),
            versions: Arc::new(versions),
            sections: Arc::new(sections),
            facts: Arc::new(facts),
            leases: Arc::new(leases),
            inventory_slots: Arc::new(inventory_slots),
        });
        Ok(snapshot)
    }

    pub(super) fn original_palettes(
        &self,
        chunk: pumpkin_cluster::protocol::ChunkKey,
    ) -> Option<Arc<super::cluster_chunk_state::OwnedChunkBlocks>> {
        self.chunks.snapshot(chunk)
    }

    pub(super) fn capture(
        server: Arc<Server>,
        source: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        tick: pumpkin_cluster::time::TickStamp,
        chunks: &BTreeSet<ChunkKey>,
        scope: &[Target],
        leases: &[pumpkin_cluster::canonical_action::LivePlayerLease],
        inventory_bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding],
    ) -> Result<Arc<Self>, ApplyFailure> {
        pumpkin_cluster::canonical_action::check_logical_budget(inventory_bindings.len().saturating_add(leases.len()).saturating_add(chunks.len()).saturating_add(scope.len()), 0)?;
        if leases
                .iter()
                .any(|lease| !source.validate_live_player_lease(*lease))
            || !std::ptr::eq(server.as_ref(), source.server)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut sections = BTreeMap::new();
        for &key in chunks {
            let Some(palette) = source.blocks.snapshot(key) else {
                continue;
            };
            for index in 0..palette.block_palettes().len() {
                if sections.len() >= pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS {
                    return Err(ApplyFailure::InvalidAction);
                }
                let y = palette
                    .min_y()
                    .div_euclid(16)
                    .checked_add(index.try_into().map_err(|_| ApplyFailure::InvalidAction)?)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let target = Target::Chunk {
                    key,
                    part: ChunkPart::BlockSection(y),
                    inventory: None,
                };
                let Some(cell) = source.read(TruthView::Local, target) else {
                    continue;
                };
                if !matches!(cell.value, Value::BlockSection(ref values) if values.len() == 4096) {
                    return Err(ApplyFailure::Type(target));
                }
                sections.insert((key, y), cell.revision);
            }
        }
        let mut facts: BTreeMap<_, _> = scope
            .iter()
            .filter(|&&target| source.available(TruthView::Local, target))
            .map(|&target| (target, source.read(TruthView::Local, target)))
            .collect();
        let mut inventory_slots = BTreeMap::new();
        for binding in inventory_bindings {
            if super::cluster_world_inventory_actions::binding_target(
                source.inventory,
                None,
                binding.slot,
            ) != Some(binding.target)
            {
                return Err(ApplyFailure::Inventory(binding.target));
            }
            if source.available(TruthView::Local, binding.target) {
                facts.insert(
                    binding.target,
                    source.read(TruthView::Local, binding.target),
                );
            }
            inventory_slots.insert(binding.target, (binding.slot.target, binding.slot.loc));
        }
        let versions = source
            .versions
            .iter()
            .filter_map(|(&(view, target), &revision)| {
                (view == TruthView::Local
                    && matches!(target, Target::Chunk { key, .. } if chunks.contains(&key)))
                .then_some((target, revision))
            })
            .collect();
        let snapshot = Arc::new(Self {
            original_directory: None,
            original_local_source: None,
            players_by_chunk: Arc::new(BTreeMap::new()),
            source_leases_by_gid: Arc::new(leases.iter().map(|lease| (lease.gid, *lease)).collect()),
            backing_usage: None,
            entities_by_chunk: Arc::new(BTreeMap::new()),
            phase_backing: None,
            original_inventory_roots: Arc::new(source.inventory.original_inventory_root_targets()),
            source_screens: server.cluster_inventory_screens.load_full(),
            permission_policy: super::cluster_permissions::freeze_permission_policy(&server).map(Arc::new),
            original_phase: None,
            historical_player_contacts: Arc::new(BTreeMap::new()),
            player_install_generations: Arc::new(leases.iter().filter_map(|lease| source.inventory.player_install_generation(lease.gid).map(|generation| (*lease, generation))).collect()),
            is_phase_backing: false,
            original_tick: tick,
            original_source_slot: super::cluster_world_rules::slot_for_tick(tick),
            original_primary: super::cluster_world_rules::primary(&server),
            recipes: server.recipe_manager.cluster_dynamic_recipes_snapshot(),
            world_rules: server.cluster_world_rules.load_full(),
            world_borders: server.cluster_world_borders.load_full(),
            mornings: server.cluster_mornings.load_full(),
            scheduled_executors: Arc::new(BTreeMap::new()),
            player_associations: Arc::new(BTreeMap::new()),
            entity_occupancy_poses: Arc::new(BTreeMap::new()),
            entity_effective_spatial: Arc::new(BTreeMap::new()),
            entity_samples: Arc::new(BTreeMap::new()),
            entity_previous_samples: Arc::new(BTreeMap::new()),
            issued_births: Arc::new(BTreeMap::new()),
            peer_incarnations: server.cluster_peer_incarnations.load_full(),
            player_projections: Arc::new(BTreeMap::new()),
            mob_rules: Arc::new(BTreeMap::new()),
            mob_environments: Arc::new(BTreeMap::new()),
            server,
            chunks: Arc::new(source.blocks.callback_subset(chunks)),
            versions: Arc::new(versions),
            sections: Arc::new(sections),
            facts: Arc::new(facts),
            leases: Arc::new(leases.to_vec()),
            inventory_slots: Arc::new(inventory_slots),
        });
        snapshot.resource_usage()?;
        Ok(snapshot)
    }

    pub(super) fn with_scope(self: &Arc<Self>,
        source: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        scope: &[Target], leases: &[pumpkin_cluster::canonical_action::LivePlayerLease],
        bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding],
    ) -> Result<Arc<Self>, ApplyFailure> {
        let chunks = scope.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }).collect();
        self.with_scope_in_chunks(source, &chunks, scope, leases, bindings)
    }

    pub(super) fn with_scope_in_chunks(self: &Arc<Self>,
        source: &super::cluster_world_canonical::OwnedWorldActionState<'_>, chunks: &BTreeSet<ChunkKey>,
        scope: &[Target], leases: &[pumpkin_cluster::canonical_action::LivePlayerLease],
        bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding],
    ) -> Result<Arc<Self>, ApplyFailure> {
        if !std::ptr::eq(self.server.as_ref(), source.server) { return Err(ApplyFailure::InvalidAction); }
        self.select_scope(chunks, scope, leases, bindings)
    }

    pub(super) fn select_scope(self: &Arc<Self>, chunks: &BTreeSet<ChunkKey>, scope: &[Target],
        leases: &[pumpkin_cluster::canonical_action::LivePlayerLease],
        bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding],
    ) -> Result<Arc<Self>, ApplyFailure> {
        pumpkin_cluster::canonical_action::check_logical_budget(scope.len().saturating_add(bindings.len()).saturating_add(leases.len()), 0)?;
        let base = self.phase_backing.as_ref().unwrap_or(self);
        if leases.iter().any(|lease| !base.validate_live_player_lease(*lease)) { return Err(ApplyFailure::InvalidAction); }
        let mut targets = scope.iter().copied().collect::<BTreeSet<_>>();
        targets.extend(bindings.iter().map(|binding| binding.target));
        let mut selected_leases = leases.iter().copied().collect::<BTreeSet<_>>();
        for target in &targets {
            if let Target::Player { lease: Some(lease), .. } = target { selected_leases.insert(*lease); }
            let body = pumpkin_cluster::derived_living::actor_body_target(*target).unwrap_or(*target);
            if let Target::Chunk { part: ChunkPart::Player(gid), .. } = body
                && let Some((lease, ..)) = base.player_projections.get(&gid) { selected_leases.insert(*lease); }
        }
        for binding in bindings {
            match self.inventory_slots.get(&binding.target).or_else(|| base.inventory_slots.get(&binding.target)) {
                Some(actual) if *actual == (binding.slot.target, binding.slot.loc) => {}
                Some(_) => return Err(ApplyFailure::Inventory(binding.target)),
                None => return Err(ApplyFailure::MissingStakeholder(binding.target.stakeholder())),
            }
        }
        let mut facts = BTreeMap::new();
        let mut inventory_slots = BTreeMap::new();
        for target in &targets {
            let original = if self.available(TruthView::Local, *target) { self } else { base };
            if original.available(TruthView::Local, *target) { facts.insert(*target, original.read(TruthView::Local, *target)); }
            if let Some(slot) = self.inventory_slots.get(target).or_else(|| base.inventory_slots.get(target)) { inventory_slots.insert(*target, *slot); }
        }
        let mut selected_chunks = chunks.clone();
        selected_chunks.extend(targets.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }));
        let player_projections = selected_leases.iter().filter_map(|lease| self.player_projections.get(&lease.gid).or_else(|| base.player_projections.get(&lease.gid)).filter(|(actual, ..)| actual == lease).map(|projection| (lease.gid, *projection))).collect::<BTreeMap<_, _>>();
        selected_chunks.extend(player_projections.values().map(|(_, _, chunk, _)| *chunk));
        let worlds = selected_chunks.iter().map(|chunk| chunk.world).collect::<BTreeSet<_>>();
        let entities_by_chunk = selected_chunks.iter().map(|chunk| {
            let ids = base.entities_by_chunk.get(chunk).into_iter().chain(self.entities_by_chunk.get(chunk)).flat_map(|ids| ids.iter().copied()).collect::<BTreeSet<_>>();
            (*chunk, ids)
        }).collect::<BTreeMap<_, _>>();
        let entity_ids = entities_by_chunk.values().flat_map(|ids| ids.iter().copied()).collect::<BTreeSet<_>>();
        let entity_occupancy_poses = entity_ids.iter().filter_map(|id| self.entity_occupancy_poses.get(id).or_else(|| base.entity_occupancy_poses.get(id)).map(|pose| (*id, pose.clone()))).collect::<BTreeMap<_, _>>();
        let mut selected_store = base.chunks.callback_subset(&selected_chunks);
        for chunk in &selected_chunks {
            if !selected_store.contains(*chunk) && self.chunks.contains(*chunk) && !selected_store.merge_missing_actor_chunk(&self.chunks, *chunk) { return Err(ApplyFailure::InvalidAction); }
        }
        let snapshot = Arc::new(Self {
            original_directory: base.original_directory.clone(), original_local_source: base.original_local_source,
            players_by_chunk: Arc::new(player_projections.values().fold(BTreeMap::new(), |mut chunks: BTreeMap<ChunkKey, BTreeSet<_>>, (lease, _, chunk, _)| { chunks.entry(*chunk).or_default().insert(*lease); chunks })),
            source_leases_by_gid: Arc::new(selected_leases.iter().map(|lease| (lease.gid, *lease)).collect()),
            backing_usage: None,
            player_install_generations: Arc::new(selected_leases.iter().filter_map(|lease| self.player_install_generations.get(lease).or_else(|| base.player_install_generations.get(lease)).map(|generation| (*lease, *generation))).collect()),
            original_inventory_roots: base.original_inventory_roots.clone(),
            source_screens: base.source_screens.clone(), permission_policy: base.permission_policy.clone(),
            phase_backing: Some(base.clone()), original_phase: base.original_phase.clone(), historical_player_contacts: Arc::new(selected_leases.iter().filter_map(|lease| self.historical_player_contacts.get(lease).or_else(|| base.historical_player_contacts.get(lease)).map(|contact| (*lease, contact.clone()))).collect()), is_phase_backing: false,
            original_tick: self.original_tick, original_source_slot: self.original_source_slot, original_primary: self.original_primary,
            recipes: self.recipes.clone(), world_rules: self.world_rules.clone(), world_borders: self.world_borders.clone(), mornings: self.mornings.clone(),
            scheduled_executors: Arc::new(selected_chunks.iter().filter_map(|chunk| self.scheduled_executors.get(chunk).or_else(|| base.scheduled_executors.get(chunk)).map(|peer| (*chunk, *peer))).collect()),
            player_associations: Arc::new(selected_leases.iter().filter_map(|lease| self.player_associations.get(lease).or_else(|| base.player_associations.get(lease)).map(|value| (*lease, *value))).collect()),
            entity_effective_spatial: Arc::new(entity_ids.iter().filter_map(|id| self.entity_effective_spatial.get(id).or_else(|| base.entity_effective_spatial.get(id)).map(|spatial| (*id, spatial.clone()))).collect()),
            entity_samples: Arc::new(entity_ids.iter().filter_map(|id| self.entity_samples.get(id).or_else(|| base.entity_samples.get(id)).map(|sample| (*id, sample.clone()))).collect()),
            entity_previous_samples: Arc::new(entity_ids.iter().filter_map(|id| self.entity_previous_samples.get(id).or_else(|| base.entity_previous_samples.get(id)).map(|sample| (*id, sample.clone()))).collect()),
            issued_births: Arc::new(entity_ids.iter().filter_map(|id| self.issued_births.get(id).or_else(|| base.issued_births.get(id)).map(|birth| (*id, birth.clone()))).collect()),
            entities_by_chunk: Arc::new(entities_by_chunk),
            entity_occupancy_poses: Arc::new(entity_occupancy_poses), peer_incarnations: self.peer_incarnations.clone(), player_projections: Arc::new(player_projections),
            mob_rules: Arc::new(worlds.iter().filter_map(|world| base.mob_rules.get(world).map(|rules| (*world, *rules))).collect()),
            mob_environments: Arc::new(worlds.iter().filter_map(|world| base.mob_environments.get(world).map(|environment| (*world, environment.clone()))).collect()),
            server: self.server.clone(), chunks: Arc::new(selected_store),
            versions: Arc::new(targets.iter().filter_map(|target| self.versions.get(target).or_else(|| base.versions.get(target)).map(|version| (*target, *version))).collect()),
            sections: Arc::new(selected_chunks.iter().flat_map(|chunk| base.sections.range((*chunk, i32::MIN)..=(*chunk, i32::MAX)).chain(self.sections.range((*chunk, i32::MIN)..=(*chunk, i32::MAX))).map(|(key, version)| (*key, *version))).collect()),
            facts: Arc::new(facts), leases: Arc::new(selected_leases.into_iter().collect()), inventory_slots: Arc::new(inventory_slots),
        });
        snapshot.resource_usage()?;
        Ok(snapshot)
    }

    pub(super) fn expand_history(
        &self,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        tick: pumpkin_cluster::time::TickStamp,
        targets: &[Target],
    ) -> Result<Arc<Self>, ApplyFailure> {
        if self.facts.len().saturating_add(targets.len()) > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut next = self.clone();
        for &target in targets {
            if self.available(TruthView::Local, target) {
                continue;
            }
            let cell = graphs
                .symbolic_anchor_for(tick, target)
                .map_err(|_| ApplyFailure::Condition(target))?;
            Arc::make_mut(&mut next.facts).insert(target, cell);
        }
        let next = Arc::new(next);
        next.resource_usage()?;
        Ok(next)
    }
}

impl ActionState for CallbackTickSnapshot {
    fn world_tool_light_plans(&self, location: pumpkin_cluster::world_command::BlockLocation, operation: pumpkin_cluster::world_command::ToolOperation, current: u16) -> Result<Vec<pumpkin_cluster::world_action::WorldLightPlan>, ApplyFailure> { super::cluster_world_tool_transform::command_light_plans(&self.server, location, operation, current) }
    fn world_tool_companion(&self, location: pumpkin_cluster::world_command::BlockLocation, current: u16) -> Result<Option<pumpkin_cluster::world_command::ToolCompanion>, ApplyFailure> { super::cluster_world_tool_transform::command_companion(location, current) }
    fn world_tool_attributes(&self) -> Result<pumpkin_cluster::world_command::ToolAttributes, ApplyFailure> {
        Ok(super::cluster_world_tool_transform::command_attributes())
    }
    fn world_tool_transform(&self, operation: pumpkin_cluster::world_command::ToolOperation, current: u16, item: u16) -> Result<Option<u16>, ApplyFailure> {
        super::cluster_world_tool_transform::command_transform(operation, current, item)
    }
    fn world_tool_eye_height(&self, living: &pumpkin_cluster::item_action::ActorLivingState) -> Result<f64, ApplyFailure> {
        Ok(f64::from(super::cluster_living_metadata::player_eye_height(living)))
    }

    fn wake_rest_stat(&self) -> Result<(u16, u32), ApplyFailure> { Ok(super::cluster_sleep_validation::rest_stat()) }

    fn validate_wake_beds(&self, world: pumpkin_cluster::protocol::WorldId, sleeping: pumpkin_cluster::item_action::PlayerSleepingState,
        beds: &[pumpkin_cluster::sleep_action::BedRelease]) -> Result<(), ApplyFailure> {
        super::cluster_sleep_validation::validate_beds(world, sleeping, beds)
    }

    fn validate_morning(&self, world: pumpkin_cluster::protocol::WorldId, morning: pumpkin_cluster::world_time::MorningTransition, source: pumpkin_cluster::canonical_action::ActionRef) -> Result<(), ApplyFailure> {
        if source.tick == self.original_tick && self.original_source_slot.is_some_and(|slot| morning.slot <= slot) && self.mornings.resolve(world, morning) { Ok(()) } else { Err(ApplyFailure::InvalidAction) }
    }

    fn actor_occupancy_cut(&self, target: Target, value: &Value) -> Result<(pumpkin_cluster::realm_completion::PeerGeneration, pumpkin_cluster::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        use pumpkin_cluster::actor_occupancy::ActorOccupancyCut;
        let (owner, cut) = match (target, value) {
            (Target::Chunk { key, part: ChunkPart::Player(gid), inventory: None }, Value::Living(living)) => {
                let base = self.phase_backing.as_deref().unwrap_or(self);
                let (lease, spatial, association, epoch) = self.player_projections.get(&gid).or_else(|| base.player_projections.get(&gid)).ok_or(ApplyFailure::Condition(target))?;
                let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
                let (expected, owner) = self.player_association(*lease).ok_or(ApplyFailure::Condition(target))?;
                if *association != key || expected != key || avatar.spatial_epoch != *epoch
                    || self.original_tick.distance_since(spatial.frame.effective.tick) >= 32768 { return Err(ApplyFailure::Condition(target)); }
                (owner, ActorOccupancyCut { position: spatial.frame.effective.position.map(f64::to_bits),
                    sample_tick: spatial.frame.effective.tick, simulation_sequence: spatial.frame.effective.source_seq })
            }
            (Target::Chunk { key, part: ChunkPart::Entity(id), inventory: None }, Value::Entity(body)) => {
                let sample = self.original_entity_sample(id).ok_or(ApplyFailure::Condition(target))?;
                let phase = self.original_phase.as_ref().ok_or(ApplyFailure::Condition(target))?;
                let holders = self.original_frozen_holders(key)?;
                let primary = holders.intersection(&phase.primaries).copied().next();
                if !body.belongs_to(id, key) || !sample.matches_body(body) || self.original_tick.distance_since(sample.tick) >= 32768
                    || !sample.authorized_issuer(body, primary.unwrap_or(body.owner)) || !phase.sources.contains(&sample.issuer) { return Err(ApplyFailure::Condition(target)); }
                (body.owner, ActorOccupancyCut { position: sample.spatial.position, sample_tick: sample.tick, simulation_sequence: sample.sequence })
            }
            _ => return Err(ApplyFailure::Type(target)),
        };
        if owner.generation == 0 || !cut.valid() { return Err(ApplyFailure::InvalidAction); }
        Ok((owner, cut))
    }

    fn machine_genesis(&self, chunk: ChunkKey) -> Result<[u8; 32], ApplyFailure> {
        self.chunks.genesis(chunk).ok_or(ApplyFailure::Condition(Target::Chunk {key:chunk,
            part:ChunkPart::Whole,inventory:None}))?.identity()
    }

    fn actor_collision_shape(&self, target: Target, value: &Value)
        -> Result<pumpkin_cluster::actor_occupancy::CollisionShape, ApplyFailure> {
        super::cluster_world_canonical::actor_collision_shape(target, value)
    }
    fn validate_world_border(&self, witness: pumpkin_cluster::world_time::WorldBorderWitness, source: pumpkin_cluster::canonical_action::ActionRef) -> Result<(), ApplyFailure> {
        self.world_borders.resolve(witness, source.tick).map(|_| ()).ok_or(ApplyFailure::InvalidAction)
    }

    fn action_source_slot(&self, reference: pumpkin_cluster::canonical_action::ActionRef) -> Option<u64> {
        (reference.tick == self.original_tick).then_some(self.original_source_slot).flatten()
    }
    fn player_advancement_outcome(&self, event: &pumpkin_cluster::player_advancement::AdvancementEvent,
        state: &pumpkin_cluster::player_advancement::PlayerAdvancementState, achieved_millis: u64)
        -> Result<pumpkin_cluster::player_advancement::AdvancementOutcome, ApplyFailure> {
        super::cluster_advancements::outcome(state, event, achieved_millis).ok_or(ApplyFailure::InvalidAction)
    }

    fn recipe_known(&self, id: &str) -> bool {
        super::cluster_advancements::recipe_displays(&self.recipes).contains_key(id)
    }
    fn entity_placement_matches(&self, intent: &pumpkin_cluster::entity_placement::EntityPlacementIntent) -> bool { let Ok(rules) = self.world_rules(intent.rules, intent.body.birth.action.tick) else { return false; }; super::cluster_entity_placement::factory_matches(intent, &rules) }
    fn falling_damage_kind(&self, state: u16) -> Option<pumpkin_cluster::combat_action::DamageKind> {
        let block = pumpkin_data::BlockStateId::new(state)?.to_block();
        Some(if [&pumpkin_data::Block::ANVIL, &pumpkin_data::Block::CHIPPED_ANVIL,
            &pumpkin_data::Block::DAMAGED_ANVIL].contains(&block) {
            pumpkin_cluster::combat_action::DamageKind::Anvil
        } else { pumpkin_cluster::combat_action::DamageKind::FallingBlock })
    }
    fn primed_tnt_block(&self, state: u16) -> bool {

        pumpkin_data::BlockStateId::new(state).is_some_and(|state| state.to_block() == &pumpkin_data::Block::TNT)
    }
    fn primed_tnt_identity(&self) -> Option<(u16, u16)> {
        Some((pumpkin_data::entity::EntityType::TNT.id, pumpkin_data::Block::TNT.default_state.id.as_u16()))
    }
    fn issuer_entity_admission_authority(&self, reference: pumpkin_cluster::canonical_action::ActionRef,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        use pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority;
        if !witness.valid() || reference != witness.admission.body.birth.action
            || self.peer_incarnations.get(&witness.issuer.server) != Some(&witness.issuer.generation) {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.action_source_slot(reference) != Some(witness.source_slot) {
            return Err(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(witness.actor_chunk)));
        }
        if let Some(lease) = witness.lease {
            let target = Target::Chunk { key: witness.actor_chunk, part: ChunkPart::Player(lease.gid), inventory: None };
            if !self.validate_live_player_lease(lease) || !matches!(self.read(TruthView::Local, target),
                Some(Cell { value: Value::Living(living), .. }) if living.combat.actor == pumpkin_cluster::actor::ActorId::Player(lease.gid)) {
                return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
            }
        }
        match &witness.authority {
            IssuerAdmissionAuthority::Command { .. } => super::cluster_command_issuance::validate(reference, witness, self.action_source_slot(reference)),
            IssuerAdmissionAuthority::Primary { rules } => {
                self.world_rules.resolve(*rules, reference.tick)?;
                if self.original_primary != Some(rules.rules.issuer)
                    || self.scheduled_executors.get(&witness.admission.chunk) != Some(&witness.issuer) {
                    return Err(ApplyFailure::InvalidAction);
                }
                Ok(())
            }
        }
    }

    fn issuer_entity_initialization_matches(&self, witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        super::cluster_owned_mob_admission::validate_initialization(witness)
    }

    fn world_rules(&self, witness: pumpkin_cluster::world_rules::WorldRulesWitness, tick: pumpkin_cluster::time::TickStamp)
        -> Result<pumpkin_cluster::world_rules::WorldRulesSnapshot, ApplyFailure> {
        self.world_rules.resolve(witness, tick).cloned()
    }

    fn resolve_world_respawn(
        &self,
        plan: pumpkin_cluster::world_respawn::WorldRespawnPlan,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Option<pumpkin_cluster::world_respawn::WorldRespawnResolution>, ApplyFailure> {
        super::cluster_world_respawn::resolve(plan, values)
    }

    fn scheduled_executor(
        &self,
        chunk: ChunkKey,
    ) -> Result<pumpkin_cluster::realm_completion::PeerGeneration, ApplyFailure> {
        self.scheduled_executors
            .get(&chunk)
            .copied()
            .ok_or(ApplyFailure::MissingStakeholder(
                pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk),
            ))
    }
    fn block_segment_obstructs(
        &self,
        state: u16,
        position: pumpkin_cluster::protocol::BlockPos,
        start: [u64; 3],
        end: [u64; 3],
    ) -> Result<bool, ApplyFailure> {
        super::cluster_world_explosion::block_segment_obstructs(state, position, start, end)
    }
    fn borrow(&self, view: TruthView, target: Target) -> Option<std::borrow::Cow<'_, Cell>> {
        use std::borrow::Cow;
        if view != TruthView::Local {
            return None;
        }
        if let Some(cell) = self.facts.get(&target) {
            return cell.as_ref().map(Cow::Borrowed);
        }
        if let Target::Chunk {
            key,
            part,
            inventory: None,
        } = target
        {
            let cell = match part {
                ChunkPart::Entity(id) => self.chunks.entity_body(false, key, id),
                ChunkPart::Whole => self.chunks.scheduled_queue(false, key),
                ChunkPart::BlockEntity(position) => self.chunks.block_entity(false, key, position),
                ChunkPart::LightSection(y) => self.chunks.light_section(false, key, y),
                _ => return self.read(view, target).map(Cow::Owned),
            };
            return cell.map(Cow::Borrowed);
        }
        self.read(view, target).map(Cow::Owned)
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        pumpkin_data::attributes::Attributes::ALL
            .iter()
            .map(|attribute| u16::from(attribute.id))
            .collect()
    }

    fn inventory_equipment_facts(
        &self,
        cells: &[(
            pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot,
            pumpkin_cluster::inventory::InventoryCell,
        )],
    ) -> Result<pumpkin_cluster::combat_action::CombatEquipmentFacts, ApplyFailure> {
        super::cluster_world_inventory_actions::inventory_equipment_facts(cells)
    }

    fn fall_damage_factor(&self, state: u16, sneaking: bool) -> Result<u16, ApplyFailure> {
        super::cluster_living_environment::fall_damage_factor(state, sneaking)
    }
    fn environmental_contact(&self, intent: &pumpkin_cluster::combat_action::EnvironmentalDamageIntent, values: &std::collections::BTreeMap<Target,Cell>)
        -> Result<pumpkin_cluster::combat_action::EnvironmentalContactFacts,ApplyFailure> { super::cluster_living_environment::contact(self,intent,values) }
    fn environmental_hazard(
        &self,
        kind: pumpkin_cluster::combat_action::EnvironmentalHazardKind,
        block_state: u16,
    ) -> Result<Option<pumpkin_cluster::combat_action::EnvironmentalHazardFacts>, ApplyFailure>
    {
        super::cluster_living_environment::hazard(kind, block_state)
    }

    fn status_effect_facts(
        &self,
        effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
    ) -> Result<pumpkin_cluster::combat_action::CombatStatusFacts, ApplyFailure> {
        if !effects.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(crate::entity::combat::normalized_status(
            effects.visible.iter(),
            false,
        ))
    }

    fn status_effect_modifiers(
        &self,
        effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
    ) -> Result<Vec<pumpkin_cluster::combat_action::CombatAttributeModifier>, ApplyFailure> {
        super::cluster_item_status::modifiers(effects)
    }

    fn mob_breeding_advancements(&self, kind: u16, credit: &pumpkin_cluster::mob_breeding::MobBreedingCredit,
        state: &pumpkin_cluster::player_advancement::PlayerAdvancementState)
        -> Result<pumpkin_cluster::player_advancement::PlayerAdvancementState, ApplyFailure> {
        super::cluster_owned_mob_breeding::advancement_outcome(kind, credit, state)
    }
    fn derive_item_use(
        &self,
        intent: &pumpkin_cluster::item_use::ItemUseIntent,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: pumpkin_cluster::time::TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        super::cluster_world_item_actions::derive_canonical_use(self, intent, id, tick, values)
    }
    fn entity_equipment_compatible(
        &self,
        kind: u16,
        slots: &[(pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot, pumpkin_cluster::inventory::InventoryCell)],
    ) -> Result<(), ApplyFailure> {
        super::cluster_world_inventory_actions::entity_equipment_compatible(kind, slots)
    }

    fn validate_block_inventory_layout(&self, block: Target, state: u16, block_entity: &Cell,
        bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding]) -> Result<(), ApplyFailure> {
        super::cluster_inventory_block_roots::validate_layout(block, state, block_entity, bindings)
    }
    fn entity_inventory_storage_capacity(
        &self,
        kind: u16,
        facets: &[(pumpkin_cluster::mob_species_property::MobSpeciesProperty, pumpkin_cluster::mob_species_property::MobSpeciesPropertyValue)],
        storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        super::cluster_world_inventory_actions::entity_inventory_storage_capacity(kind, facets, storage_slots)
    }

    fn entity_inventory_layout(
        &self,
        body: &pumpkin_cluster::entity_body::EntityBody,
    ) -> Result<Vec<pumpkin_cluster::inventory::InvLoc>, ApplyFailure> {
        super::cluster_owned_mob_birth::inventory_layout(body)
    }

    fn entity_inventory_limit(
        &self,
        body: &pumpkin_cluster::entity_body::EntityBody,
        item: &pumpkin_cluster::inventory::InventoryCell,
        target: Target,
    ) -> Result<u8, ApplyFailure> {
        super::cluster_world_inventory_actions::entity_inventory_slot_limit(body, item, target)
    }

    fn inventory_limit(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
        destination: Target,
    ) -> Result<u8, ApplyFailure> {
        if let Target::Chunk {
            part: pumpkin_cluster::canonical_action::ChunkPart::Entity(entity),
            inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Entity { inv, slot }),
            ..
        } = destination
        {
            return super::cluster_world_inventory_actions::inventory_slot_limit(
                source,
                pumpkin_cluster::inventory::InventoryTarget::Entity(entity.into()),
                pumpkin_cluster::inventory::InvLoc::new(inv, slot),
                destination,
            );
        }
        let &(address, loc) = self
            .inventory_slots
            .get(&destination)
            .ok_or(ApplyFailure::Inventory(destination))?;
        super::cluster_world_inventory_actions::inventory_slot_limit(
            source,
            address,
            loc,
            destination,
        )
    }

    fn crossbow_items(&self, intent: &pumpkin_cluster::crossbow_items::CrossbowItems, id: pumpkin_cluster::canonical_action::ActionId, tick: pumpkin_cluster::time::TickStamp,
        values: &BTreeMap<Target, Cell>) -> Result<BTreeMap<Target, pumpkin_cluster::inventory::InventoryCell>, ApplyFailure> {
        super::cluster_item_launch::crossbow_items(intent, id, tick, values)
    }
    fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
        super::cluster_vehicle_world::activator(state)
    }
    fn vehicle_drop_item(&self, body: &pumpkin_cluster::entity_body::EntityBody)
        -> Result<pumpkin_cluster::inventory::InventoryCell, ApplyFailure> {
        super::cluster_vehicle_birth::vehicle_drop_item(body)
    }
    fn issued_item_birth(&self, identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
        cause: pumpkin_cluster::canonical_action::ActionRef, owner: pumpkin_cluster::realm_completion::PeerGeneration,
        spatial: pumpkin_cluster::entity_body::EntitySpatial, cell: &pumpkin_cluster::inventory::InventoryCell, pickup_delay: u16)
        -> Result<pumpkin_cluster::entity_body::EntityBody, ApplyFailure> {
        let position = spatial.position.map(f64::from_bits);
        if position.iter().any(|value| !value.is_finite() || value.abs() > 30_000_000.0) { return Err(ApplyFailure::InvalidAction); }
        let chunk = ChunkKey::new(spatial.world, (position[0] / 16.0).floor() as i32, (position[2] / 16.0).floor() as i32);
        let unavailable = || ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk));
        let base = self.phase_backing.as_deref().unwrap_or(self);
        if self.action_source_slot(cause).is_none_or(|slot| slot as u16 != cause.tick.0)
            || self.issued_births.get(&identity.id).or_else(|| base.issued_births.get(&identity.id)) != Some(&(cause, identity.clone())) {
            return Err(unavailable());
        }
        let phase = self.original_phase().ok_or_else(unavailable)?;
        if !phase.sources.contains(&identity.source) { return Err(unavailable()); }
        let source_owner = match cause.id.actor {
            pumpkin_cluster::actor::ActorId::Player(gid) => {
                let lease = self.inventory_source_lease(gid).ok_or_else(unavailable)?;
                if lease.incarnation != cause.id.incarnation { return Err(ApplyFailure::InvalidAction); }
                self.player_association(lease).ok_or_else(unavailable)?.1
            }
            pumpkin_cluster::actor::ActorId::Entity(id) => {
                if id.generation != cause.id.incarnation { return Err(ApplyFailure::InvalidAction); }
                let source_chunk = base.chunks.entity_body_chunk(false, id).ok_or_else(unavailable)?;
                let target = Target::Chunk { key: source_chunk, part: ChunkPart::Entity(id), inventory: None };
                let source = base.read(TruthView::Local, target).ok_or_else(unavailable)?;
                let Value::Entity(body) = source.value else { return Err(ApplyFailure::Type(target)); };
                if !body.belongs_to(id, source_chunk) { return Err(ApplyFailure::InvalidAction); }
                body.owner
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        if source_owner != identity.source || self.scheduled_executor(chunk)? != owner || identity.id.local_id == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let target = Target::Chunk { key: chunk, part: ChunkPart::Entity(identity.id), inventory: None };
        if self.facts.get(&target) != Some(&None) { return Err(unavailable()); }
        let body = super::cluster_item_birth::reserved_item_body(cause, identity, owner, spatial, cell, pickup_delay)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if !body.valid_at(cause.tick) { return Err(ApplyFailure::InvalidAction); }
        Ok(body)
    }
    fn dropped_item_body(&self, template: &pumpkin_cluster::entity_body::EntityBody, source: &pumpkin_cluster::inventory::InventoryCell)
        -> Result<pumpkin_cluster::entity_body::EntityBody, ApplyFailure> {
        super::cluster_item_birth::project_drop_template(template, source)
    }
    fn death_inventory_drop(&self, template: &pumpkin_cluster::entity_body::EntityBody, source: &pumpkin_cluster::inventory::InventoryCell)
        -> Result<Option<pumpkin_cluster::entity_body::EntityBody>, ApplyFailure> {
        super::cluster_item_birth::death_inventory_drop(template, source)
    }
    fn derive_cloud_potion(&self, intent: &pumpkin_cluster::area_cloud::AreaCloudIntent, victim: &pumpkin_cluster::item_contact::ContactObservation,
        id: pumpkin_cluster::canonical_action::ActionId, tick: pumpkin_cluster::time::TickStamp, values: &BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>) -> Result<pumpkin_cluster::area_cloud::CloudPotionOutcome, pumpkin_cluster::canonical_action::ApplyFailure> { super::cluster_area_cloud::potion(self, intent, victim, id, tick, values) }
    fn derive_projectile_tick(
&self, intent: &pumpkin_cluster::projectile_tick::ProjectileTickIntent, id: pumpkin_cluster::canonical_action::ActionId, tick: pumpkin_cluster::time::TickStamp,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>) -> Result<Vec<pumpkin_cluster::canonical_action::Effect>, ApplyFailure> {
        super::cluster_projectile_tick::derive(self, intent, id, tick, values)
    }
    fn mending_repair(&self, source: &pumpkin_cluster::inventory::InventoryCell, experience: u32)
        -> Result<(pumpkin_cluster::canonical_inventory::ItemTransformOutput, u32), ApplyFailure> {
        super::cluster_world_item_actions::derive_mending_repair(source, experience)
    }
    fn item_cooldown_group(&self, source: &pumpkin_cluster::inventory::InventoryCell) -> Result<String, ApplyFailure> {
        super::cluster_world_item_actions::item_cooldown_group(source)
    }
    fn derive_block_inventory_tick(&self, intent: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>) -> Result<pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
        super::cluster_inventory_block_tick::derive(self, intent, values)
    }
    fn machine_tick_footprint(&self, identity: pumpkin_cluster::action_identity::MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<pumpkin_cluster::action_command::ActionFootprint, ApplyFailure> {
        super::cluster_inventory_machine::footprint(&self.server, self, identity, values)
    }
    fn derive_machine_tick(&self, identity: pumpkin_cluster::action_identity::MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<Vec<pumpkin_cluster::canonical_action::AppliedWrite>, ApplyFailure> {
        super::cluster_inventory_machine::derive(&self.server, self, identity, values)
    }
    fn menu_inventory_transform(&self, kind: pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind,
        selection: &pumpkin_cluster::canonical_menu::MenuSelection, inputs: &[pumpkin_cluster::inventory::InventoryCell])
        -> Result<pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure> {
        super::cluster_inventory_menu::derive_menu_inventory_transform(kind, selection, inputs)
    }
    fn item_transform(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
        operation: &pumpkin_cluster::canonical_inventory::ItemTransformOperation,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<pumpkin_cluster::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        super::cluster_world_item_actions::derive_item_transform(source, operation, id, tick)
    }
    fn validate_live_player_lease(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> bool {
        self.inventory_source_lease(lease.gid) == Some(lease)
    }
    fn available(&self, view: TruthView, target: Target) -> bool {
        if view != TruthView::Local {
            return false;
        }
        if self.facts.contains_key(&target) {
            return true;
        }
        match target {
            Target::Chunk {
                key,
                part: ChunkPart::BlockSection(y),
                inventory: None,
            } => self.sections.contains_key(&(key, y)),
            Target::Chunk {
                key,
                part: ChunkPart::LightSection(y),
                inventory: None,
            } => self.chunks.light_section(false, key, y).is_some(),
            Target::Chunk {
                key,
                part: ChunkPart::Block(position),
                inventory: None,
            } => self.read(view, Target::Chunk {key,part:ChunkPart::Block(position),inventory:None}).is_some(),
            Target::Chunk {
                key,
                part: ChunkPart::Whole | ChunkPart::BlockEntity(_) | ChunkPart::Entity(_),
                inventory: None,
            } => self.chunks.contains(key),
            _ => self.read(view, target).is_some(),
        }
    }

    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        if view != TruthView::Local {
            return None;
        }
        if let Some(value) = self.facts.get(&target) {
            return value.clone();
        }
        if let Some(body) = pumpkin_cluster::derived_living::actor_body_target(target) {
            return pumpkin_cluster::derived_living::actor_facet_cell(
                &self.read(view, body)?,
                target,
            );
        }
        let Target::Chunk {
            key,
            part,
            inventory: None,
        } = target
        else {
            return None;
        };
        match part {
            ChunkPart::Block(position) => {
                if let Ok(state) = self.chunks.block(key, position) {
                    return Some(Cell {revision:self.versions.get(&target).copied().unwrap_or(0),value:Value::Block(state.as_u16())});
                }
                let section = Target::Chunk {key,part:ChunkPart::BlockSection(position.y.div_euclid(16)),inventory:None};
                let cell = self.facts.get(&section)?.as_ref()?;
                let Value::BlockSection(states) = &cell.value else { return None };
                let index = ((position.y.rem_euclid(16) << 8) | (position.z.rem_euclid(16) << 4) | position.x.rem_euclid(16)) as usize;
                Some(Cell {revision:0,value:Value::Block(*states.get(index)?)})
            },
            ChunkPart::BlockSection(y) => {
                let snapshot = self.chunks.snapshot(key)?;
                let index: usize = y
                    .checked_sub(snapshot.min_y().div_euclid(16))?
                    .try_into()
                    .ok()?;
                let palette = snapshot.block_palettes().get(index)?;
                let states = (0..4096)
                    .map(|index| {
                        palette
                            .get(index & 15, index >> 8, (index >> 4) & 15)
                            .as_u16()
                    })
                    .collect();
                Some(Cell {
                    revision: *self.sections.get(&(key, y))?,
                    value: Value::BlockSection(states),
                })
            }
            ChunkPart::ActorOccupancy => self.chunks.occupancy_root(false, key).cloned(),
            ChunkPart::Whole => self.chunks.scheduled_queue(false, key).cloned(),
            ChunkPart::BlockEntity(position) => {
                self.chunks.block_entity(false, key, position).cloned()
            }
            ChunkPart::LightSection(y) => self.chunks.light_section(false, key, y).cloned(),
            ChunkPart::Entity(id) => self.chunks.entity_body(false, key, id).cloned(),
            _ => None,
        }
    }

    fn derive_world_light(
        &self,
        plan: pumpkin_cluster::world_action::WorldLightPlan,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        super::cluster_world_callback_light::derive_symbolic_light(&self.server, plan, values)
    }

    fn commit(
        &mut self,
        _: ActionContext,
        _: TruthView,
        _: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}

impl pumpkin_cluster::action_command::OriginalCommandState for CallbackTickSnapshot {
    fn original_source_slot(&self) -> Option<u64> { self.original_source_slot }

    fn lookup_original(&self, target: Target) -> Result<Option<std::borrow::Cow<'_, Cell>>, ApplyFailure> {
        if !self.available(TruthView::Local, target) {
            return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
        }
        Ok(self.borrow(TruthView::Local, target))
    }
}
