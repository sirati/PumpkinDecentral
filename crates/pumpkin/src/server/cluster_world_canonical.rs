use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{
    Action, ActionContext, ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Effect,
    InventoryId, Target, TruthView, Value,
};
use pumpkin_cluster::inventory::{
    InvLedger, InvLoc, InventoryMutation, InventoryStateSnapshot, InventoryTarget,
    SemanticInventoryOp,
};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;

use super::Server;
use super::cluster_chunk_state::{BlockChange, OwnedChunkStore, PreparedBlockReplacement};
use super::cluster_world_actor::{
    OwnedActors, OwnedLivingActors, PreparedLivingReplacement, PreparedSpatialReplacement,
};
use super::cluster_world_inventory::InventoryOwner;

pub(super) fn valid_entity_body(
    body: &pumpkin_cluster::entity_body::EntityBody,
    chunk: ChunkKey,
    id: pumpkin_cluster::protocol::GlobalEntityId,
) -> bool {
    use pumpkin_cluster::entity_body::EntityPayload;
    use pumpkin_data::entity::EntityType;
    if !body.belongs_to(id, chunk) || EntityType::from_raw(body.kind).is_none() {
        return false;
    }
    match &body.payload {
        EntityPayload::Object(object) => {
            super::cluster_object_metadata::kind_matches(object, body.kind)
                && super::cluster_object_metadata::valid_content(object)
        }
        EntityPayload::Item(item) => {
            if body.kind != EntityType::ITEM.id {
                return false;
            }
            let Some(kind) = pumpkin_data::item::Item::from_id(item.item) else {
                return false;
            };
            let stack = if item.components.is_empty() {
                pumpkin_data::item_stack::ItemStack::static_new_java(item.count, kind)
            } else {
                let Ok(stack) = super::cluster_invsee::item_stack_from_inventory_cell(
                    &pumpkin_cluster::inventory::InventoryCell {
                        loc: InvLoc { inv: 0, slot: 0 },
                        item: item.item,
                        count: item.count,
                        nbt: item.components.clone(),
                    },
                ) else {
                    return false;
                };
                stack
            };
            item.count <= stack.get_max_stack_size()
        }
        EntityPayload::Experience(_) => body.kind == EntityType::EXPERIENCE_ORB.id,
        EntityPayload::Projectile(projectile) => {
            use pumpkin_cluster::entity_body::ProjectileKind;
            let expected = match projectile.kind {
                ProjectileKind::Arrow => EntityType::ARROW,
                ProjectileKind::SpectralArrow => EntityType::SPECTRAL_ARROW,
                ProjectileKind::Trident => EntityType::TRIDENT,
                ProjectileKind::Snowball => EntityType::SNOWBALL,
                ProjectileKind::Egg => EntityType::EGG,
                ProjectileKind::EnderPearl => EntityType::ENDER_PEARL,
                ProjectileKind::ExperienceBottle => EntityType::EXPERIENCE_BOTTLE,
                ProjectileKind::SplashPotion => EntityType::SPLASH_POTION,
                ProjectileKind::LingeringPotion => EntityType::LINGERING_POTION,
                ProjectileKind::Firework => EntityType::FIREWORK_ROCKET,
            };
            body.kind == expected.id
                && super::cluster_invsee::item_stack_from_inventory_cell(&projectile.carried_item)
                    .is_ok()
        }
        EntityPayload::Living(living) => {
            let Some(kind) = EntityType::from_raw(body.kind) else {
                return false;
            };
            kind.id != EntityType::PLAYER.id
                && (kind.id == EntityType::ARMOR_STAND.id) == living.stand.is_some()
                && kind.attributes.iter().any(|(attribute, _)| {
                    *attribute == pumpkin_data::attributes::Attributes::MAX_HEALTH
                })
                && living.belongs_to(pumpkin_cluster::actor::ActorId::Entity(id))
                && living.mob.as_ref().is_none_or(|mob| {
                    super::cluster_owned_mob_metadata::species_matches_kind(&mob.species, body.kind)
                })
                && living
                    .merchant
                    .as_ref()
                    .is_none_or(|merchant| match merchant.scheme {
                        pumpkin_cluster::canonical_merchant::MerchantScheme::Villager => {
                            kind.id == EntityType::VILLAGER.id
                        }
                        pumpkin_cluster::canonical_merchant::MerchantScheme::WanderingTrader => {
                            kind.id == EntityType::WANDERING_TRADER.id
                        }
                    })
                && living.combat.velocity_micro == body.spatial.velocity_micro
        }
        EntityPayload::Vehicle(vehicle) => {
            use pumpkin_cluster::entity_body::VehicleKind;
            let Some(kind) = EntityType::from_raw(body.kind) else {
                return false;
            };
            match vehicle.kind {
                VehicleKind::Boat => {
                    kind.resource_name.ends_with("_boat")
                        && !kind.resource_name.ends_with("_chest_boat")
                        || kind.resource_name.ends_with("_raft")
                            && !kind.resource_name.ends_with("_chest_raft")
                }
                VehicleKind::ChestBoat => {
                    kind.resource_name.ends_with("_chest_boat")
                        || kind.resource_name.ends_with("_chest_raft")
                }
                VehicleKind::Minecart => kind.resource_name == "minecart",
                VehicleKind::ChestMinecart => kind.resource_name == "chest_minecart",
                VehicleKind::FurnaceMinecart => kind.resource_name == "furnace_minecart",
                VehicleKind::HopperMinecart => kind.resource_name == "hopper_minecart",
                VehicleKind::TntMinecart => kind.resource_name == "tnt_minecart",
                VehicleKind::SpawnerMinecart => kind.resource_name == "spawner_minecart",
                VehicleKind::CommandBlockMinecart => kind.resource_name == "command_block_minecart",
            }
        }
    }
}

fn valid_scheduled_queue(
    key: ChunkKey,
    queue: &[pumpkin_cluster::canonical_action::ScheduledTickFact],
) -> bool {
    use pumpkin_cluster::canonical_action::ScheduledTickKind;
    let mut identities = BTreeSet::new();
    queue.iter().all(|tick| {
        tick.position.x.div_euclid(16) == key.x()
            && tick.position.z.div_euclid(16) == key.z()
            && (-3..=3).contains(&tick.priority)
            && match tick.kind {
                ScheduledTickKind::Block => pumpkin_data::BlockId::new(tick.id).is_some(),
                ScheduledTickKind::Fluid => pumpkin_data::fluid::Fluid::from_id(tick.id).is_some(),
            }
            && identities.insert((tick.kind, tick.id, tick.position))
    })
}

#[cfg(test)]
mod scheduled_queue_tests {
    use super::valid_scheduled_queue;
    use pumpkin_cluster::canonical_action::{ScheduledTickFact, ScheduledTickKind};
    use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
    use pumpkin_cluster::time::TickStamp;

    #[test]
    fn owner_rejects_malformed_scheduled_queue_before_commit() {
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let valid = ScheduledTickFact {
            origin: pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary,
            executor: pumpkin_cluster::realm_completion::PeerGeneration {
                server: pumpkin_cluster::identity::ServerId(0),
                generation: 1,
            },
            kind: ScheduledTickKind::Block,
            id: 1,
            position: BlockPos { x: 0, y: 64, z: 0 },
            due: TickStamp(7),
            priority: 0,
            source_order: 2,
        };
        assert!(valid_scheduled_queue(key, &[valid]));
        assert!(!valid_scheduled_queue(key, &[valid, valid]));
        assert!(!valid_scheduled_queue(
            key,
            &[ScheduledTickFact {
                position: BlockPos {
                    x: 16,
                    ..valid.position
                },
                ..valid
            }]
        ));
        assert!(!valid_scheduled_queue(
            key,
            &[ScheduledTickFact {
                priority: 4,
                ..valid
            }]
        ));
        assert!(!valid_scheduled_queue(
            key,
            &[ScheduledTickFact {
                id: u16::MAX,
                ..valid
            }]
        ));
    }
}

pub(super) fn take_chunk_versions(
    versions: &mut BTreeMap<(TruthView, Target), u64>,
    chunks: &BTreeSet<ChunkKey>,
    players: &[pumpkin_cluster::canonical_action::LivePlayerLease],
) -> BTreeMap<(TruthView, Target), u64> {
    let mut keys = BTreeSet::new();
    let players = players
        .iter()
        .map(|lease| lease.key)
        .collect::<BTreeSet<_>>();
    for view in [TruthView::Local, TruthView::Ground] {
        for &chunk in chunks {
            let lower = Target::Chunk {
                key: chunk,
                part: ChunkPart::Whole,
                inventory: None,
            };
            keys.extend(
                versions
                    .range((view, lower)..)
                    .take_while(|((at, target), _)| {
                        *at == view && matches!(target, Target::Chunk { key, .. } if *key == chunk)
                    })
                    .map(|(key, _)| *key),
            );
        }
        for &player in &players {
            let lower = Target::Player {
                key: player,
                lease: None,
                inventory: None,
            };
            keys.extend(
                versions
                    .range((view, lower)..)
                    .take_while(|((at, target), _)| {
                        *at == view
                            && matches!(target, Target::Player { key, .. } if *key == player)
                    })
                    .map(|(key, _)| *key),
            );
        }
    }
    keys.into_iter()
        .filter_map(|key| versions.remove(&key).map(|revision| (key, revision)))
        .collect()
}

pub(super) fn return_chunk_versions(
    versions: &mut BTreeMap<(TruthView, Target), u64>,
    mut unit: BTreeMap<(TruthView, Target), u64>,
) -> Result<(), BTreeMap<(TruthView, Target), u64>> {
    if unit.keys().any(|key| versions.contains_key(key)) {
        return Err(unit);
    }
    versions.append(&mut unit);
    Ok(())
}

#[cfg(test)]
mod chunk_version_tests {
    use super::*;
    use pumpkin_cluster::canonical_action::{LivePlayerLease, PlayerKey};
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::{BlockPos, WorldId};

    #[test]
    fn chunk_version_extraction_moves_exact_ranges_and_returns_atomically() {
        let world = WorldId::new(0, 7);
        let chunk = ChunkKey::new(world, 0, 0);
        let other = ChunkKey::new(world, 8, 0);
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(17)),
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: PlayerSlot(2),
            },
            incarnation: 3,
        };
        let own = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 80, z: 1 }),
            inventory: None,
        };
        let foreign = Target::Chunk {
            key: other,
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        };
        let player = Target::Player {
            key: lease.key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv: 0, slot: 1 }),
        };
        let original = BTreeMap::from([
            ((TruthView::Local, own), 5),
            ((TruthView::Ground, own), 4),
            ((TruthView::Local, foreign), 9),
            ((TruthView::Local, player), 6),
        ]);
        let mut versions = original.clone();
        let unit = take_chunk_versions(&mut versions, &BTreeSet::from([chunk]), &[lease]);
        assert_eq!(unit.len(), 3);
        assert_eq!(versions, BTreeMap::from([((TruthView::Local, foreign), 9)]));
        let conflicting = BTreeMap::from([((TruthView::Local, foreign), 10)]);
        assert_eq!(
            return_chunk_versions(&mut versions, conflicting.clone()),
            Err(conflicting)
        );
        assert_eq!(versions[&(TruthView::Local, foreign)], 9);
        return_chunk_versions(&mut versions, unit).unwrap();
        assert_eq!(versions, original);
    }
}

pub(super) struct OwnedWorldActionState<'a> {
    pub server: &'a Server,
    pub blocks: &'a mut OwnedChunkStore,
    pub actors: &'a mut OwnedActors,
    pub living: &'a mut OwnedLivingActors,
    pub inventory: &'a mut InventoryOwner,
    pub versions: &'a mut BTreeMap<(TruthView, Target), u64>,
}

struct PreparedCommit {
    view: TruthView,
    blocks: PreparedBlockReplacement,
    inventory: Option<InvLedger>,
    living: PreparedLivingReplacement,
    living_lifecycles: Vec<(
        ActorId,
        Option<pumpkin_cluster::item_action::ActorLivingState>,
        Option<super::cluster_world_actor::ActorSpatialState>,
    )>,
    spatial: PreparedSpatialReplacement,
    scheduled: Vec<(ChunkKey, Cell)>,
    block_entities: Vec<(ChunkKey, pumpkin_cluster::protocol::BlockPos, Option<Cell>)>,
    light_sections: Vec<(ChunkKey, i32, Option<Cell>)>,
    occupancy: Vec<(ChunkKey, Cell)>,
    block_sections: Vec<(ChunkKey, i32, Cell)>,
    entity_bodies: Vec<(
        ChunkKey,
        pumpkin_cluster::protocol::GlobalEntityId,
        Option<Cell>,
    )>,
    versions: Vec<(Target, u64)>,
    removed_versions: Vec<Target>,
    publication: Option<CanonicalInventoryPublication>,
    inventory_admissions: Vec<pumpkin_cluster::canonical_action::LivePlayerLease>,
}

pub(super) struct CanonicalInventoryPublication {
    pub snapshots: Vec<InventoryStateSnapshot>,
    pub operations: Vec<SemanticInventoryOp>,
    pub before_revisions: Vec<(InventoryTarget, u64)>,
    pub relocated_targets: Vec<(InventoryTarget, ChunkKey, ChunkKey)>,
}

impl OwnedWorldActionState<'_> {
    fn expand_living_properties(
        &self,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(Vec<AppliedWrite>, Vec<(Target, u64)>), ApplyFailure> {
        let mut physical = writes
            .iter()
            .filter(|write| {
                pumpkin_cluster::derived_living::actor_body_target(write.target).is_none()
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut versions = Vec::new();
        let body_targets = writes
            .iter()
            .filter_map(|write| {
                pumpkin_cluster::derived_living::actor_body_target(write.target).or_else(|| {
                    pumpkin_cluster::frame_inventory_projection::body_target(write.target)
                })
            })
            .collect::<BTreeSet<_>>();
        for target in body_targets {
            let before = self.read(view, target);
            let after = pumpkin_cluster::derived_living::project_body_writes(
                target,
                before.as_ref(),
                writes,
            )?;
            if let Some(existing) = physical.iter_mut().find(|write| write.target == target) {
                existing.after = after;
            } else {
                physical.push(AppliedWrite {
                    target,
                    before,
                    after,
                });
            }
        }
        for write in writes {
            let Some(body_target) =
                pumpkin_cluster::derived_living::actor_body_target(write.target)
            else {
                continue;
            };
            if self.read(view, write.target) != write.before {
                return Err(ApplyFailure::Condition(write.target));
            }
            let Some(after) = &write.after else {
                if physical
                    .iter()
                    .any(|body| body.target == body_target && body.after.is_none())
                {
                    continue;
                }
                return Err(ApplyFailure::Type(write.target));
            };
            versions.push((write.target, after.revision));
        }
        Ok((physical, versions))
    }

    fn prepare_ground_actions(
        &mut self,
        staged: &pumpkin_cluster::action_solver::StagedGroundDeltas,
    ) -> Result<PreparedCommit, ApplyFailure> {
        if !staged.validate() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut touched_chunks = std::collections::BTreeSet::new();
        let mut touched_players = std::collections::BTreeSet::new();
        let mut touched_targets = std::collections::BTreeSet::new();
        for (_, writes) in &staged.actions {
            for write in writes {
                if write.target.inventory().is_some() {
                    continue;
                }
                touched_targets.insert(write.target);
                if let Target::Chunk { key, part, .. } = write.target {
                    touched_chunks.insert(key);
                    if let ChunkPart::Player(actor) = part {
                        touched_players.insert(actor);
                    }
                    if let ChunkPart::LivingProperty {
                        actor: ActorId::Player(actor),
                        ..
                    } = part
                    {
                        touched_players.insert(actor);
                    }
                    if let ChunkPart::ActorRelation {
                        actor: ActorId::Player(actor),
                        ..
                    } = part
                    {
                        touched_players.insert(actor);
                    }
                }
            }
        }
        let mut scratch_blocks = self.blocks.clone_subset(&touched_chunks);
        let mut scratch_actors = self.actors.clone_player_subset(&touched_players);
        let mut scratch_living = self.living.clone_player_subset(&touched_players);
        let mut scratch_versions = touched_targets
            .iter()
            .filter_map(|target| {
                self.versions
                    .get(&(TruthView::Ground, *target))
                    .copied()
                    .map(|version| ((TruthView::Ground, *target), version))
            })
            .collect();
        let mut touched_blocks = std::collections::BTreeSet::new();
        let physical = {
            let mut scratch = OwnedWorldActionState {
                server: self.server,
                blocks: &mut scratch_blocks,
                actors: &mut scratch_actors,
                living: &mut scratch_living,
                inventory: &mut *self.inventory,
                versions: &mut scratch_versions,
            };
            for (_, writes) in &staged.actions {
                let physical = writes
                    .iter()
                    .filter(|write| write.target.inventory().is_none())
                    .cloned()
                    .collect::<Vec<_>>();
                for write in &physical {
                    if let Target::Chunk {
                        key,
                        part: ChunkPart::Block(_),
                        ..
                    } = write.target
                    {
                        touched_blocks.insert(key);
                    }
                }
                let prepared = scratch.prepare_commit(TruthView::Ground, &physical, false)?;
                scratch.apply_prepared(prepared);
            }
            staged
                .writes
                .iter()
                .filter(|write| write.target.inventory().is_none())
                .map(|write| {
                    let after = scratch.read(TruthView::Ground, write.target);
                    if after != write.after {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    Ok(AppliedWrite {
                        target: write.target,
                        before: write.before.clone(),
                        after,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let physical = physical
            .into_iter()
            .filter(|write| write.before != write.after)
            .collect::<Vec<_>>();
        let mut prepared = self.prepare_commit(TruthView::Ground, &physical, true)?;
        prepared.blocks = scratch_blocks
            .prepared_ground_chunks(&touched_blocks)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if staged.actions.iter().any(|(_, writes)| {
            writes.iter().any(|write| {
                write.target.inventory().is_some()
                    && (write.before.is_none() || write.after.is_none())
            })
        }) {
            let rows = staged
                .actions
                .iter()
                .map(|(reference, writes)| (*reference, writes.as_slice()))
                .collect::<Vec<_>>();
            let (candidate, versions, admissions) =
                self.prepare_inventory_action_steps(TruthView::Ground, &rows)?;
            prepared.inventory = Some(candidate);
            prepared.versions.extend(versions);
            prepared.inventory_admissions = admissions;
            return Ok(prepared);
        }
        let mut ledger = prepared.inventory.take();
        let mut versions = BTreeMap::new();
        for (_, writes) in &staged.actions {
            let mut operations = Vec::new();
            let mut expected_revisions = BTreeMap::new();
            let mut locations = std::collections::BTreeSet::new();
            for write in writes {
                if write.target.inventory().is_none() {
                    continue;
                }
                let (target, loc) = self
                    .inventory_target(Some(TruthView::Ground), write.target)
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let (Some(before), Some(after)) = (&write.before, &write.after) else {
                    return Err(ApplyFailure::Inventory(write.target));
                };
                let (Value::Inventory(before_cell), Value::Inventory(after_cell)) =
                    (&before.value, &after.value)
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                if before_cell.loc != loc
                    || after_cell.loc != loc
                    || !locations.insert((target, loc.inv, loc.slot))
                    || loc.inv != pumpkin_cluster::inventory::INV_SELECTED
                        && !after_cell.is_empty()
                        && after_cell.count > self.inventory_limit(after_cell, write.target)?
                {
                    return Err(ApplyFailure::Inventory(write.target));
                }
                let candidate =
                    ledger.get_or_insert_with(|| self.inventory.phase_candidate(TruthView::Ground));
                let snapshot = candidate
                    .snapshot(target)
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let actual = snapshot
                    .cells
                    .iter()
                    .find(|cell| cell.loc == loc)
                    .cloned()
                    .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc));
                let revision = versions
                    .get(&write.target)
                    .copied()
                    .or_else(|| {
                        self.versions
                            .get(&(TruthView::Ground, write.target))
                            .copied()
                    })
                    .unwrap_or(0);
                if *before
                    != (Cell {
                        revision,
                        value: Value::Inventory(actual),
                    })
                {
                    return Err(ApplyFailure::Inventory(write.target));
                }
                expected_revisions.insert(target, snapshot.revision);
                operations.push(SemanticInventoryOp {
                    target,
                    before: before_cell.clone(),
                    after: after_cell.clone(),
                });
                versions.insert(write.target, after.revision);
            }
            if !operations.is_empty() {
                ledger
                    .as_mut()
                    .expect("inventory candidate exists")
                    .apply_mutation(InventoryMutation {
                        expected_revisions: expected_revisions.into_iter().collect(),
                        operations,
                    })
                    .map_err(|_| ApplyFailure::InvalidAction)?;
            }
        }
        if staged
            .writes
            .iter()
            .any(|write| write.target.inventory().is_some())
            && ledger.is_none()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(candidate) = ledger {
            for write in staged
                .writes
                .iter()
                .filter(|write| write.target.inventory().is_some())
            {
                let (target, loc) = self
                    .inventory_target(Some(TruthView::Ground), write.target)
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let snapshot = candidate
                    .snapshot(target)
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let actual = snapshot
                    .cells
                    .iter()
                    .find(|cell| cell.loc == loc)
                    .cloned()
                    .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc));
                let expected = write
                    .after
                    .as_ref()
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                if expected.value != Value::Inventory(actual)
                    || versions.get(&write.target) != Some(&expected.revision)
                {
                    return Err(ApplyFailure::Inventory(write.target));
                }
            }
            prepared.inventory = Some(candidate);
            prepared.versions.extend(versions);
        }
        Ok(prepared)
    }

    pub(super) fn commit_graph_joint(
        &mut self,
        ground: &pumpkin_cluster::action_solver::StagedGroundDeltas,
        local: &[AppliedWrite],
    ) -> Result<Option<CanonicalInventoryPublication>, ApplyFailure> {
        let prepared_ground = self.prepare_ground_actions(ground)?;
        let local = self.prepare_commit(TruthView::Local, local, true)?;
        self.apply_prepared(prepared_ground);
        for (action, writes) in &ground.actions {
            self.blocks.record_ground_entity_action(*action, writes);
        }
        Ok(self.apply_prepared(local))
    }

    pub(super) fn commit_local(
        &mut self,
        deltas: &pumpkin_cluster::action_graph::StagedLocalDeltas,
    ) -> Result<Option<CanonicalInventoryPublication>, ApplyFailure> {
        let births = deltas.steps.iter().any(|step| {
            step.writes.iter().any(|write| {
                write.target.inventory().is_some()
                    && (write.before.is_none() || write.after.is_none())
            })
        });
        let local = if births {
            let rows = deltas
                .steps
                .iter()
                .map(|step| (step.reference, step.writes.as_slice()))
                .collect::<Vec<_>>();
            let (candidate, versions, admissions) =
                self.prepare_inventory_action_steps(TruthView::Local, &rows)?;
            let physical = deltas
                .writes
                .iter()
                .filter(|write| write.target.inventory().is_none())
                .cloned()
                .collect::<Vec<_>>();
            let mut local = self.prepare_commit(TruthView::Local, &physical, true)?;
            let targets = versions
                .iter()
                .filter_map(|(target, _)| match target {
                    Target::Player {
                        lease: Some(lease),
                        inventory: Some(InventoryId::Player { .. }),
                        ..
                    } => {
                        let leased = InventoryTarget::LeasedPlayer {
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                        };
                        if candidate.snapshot(leased).is_some() {
                            Some(leased)
                        } else {
                            self.inventory_target(Some(TruthView::Local), *target)
                                .map(|(target, _)| target)
                        }
                    }
                    Target::Player {
                        lease: Some(lease),
                        inventory:
                            Some(InventoryId::Virtual {
                                host,
                                kind,
                                session,
                                ..
                            }),
                        ..
                    } => {
                        let leased = InventoryTarget::LeasedVirtual {
                            host: *host,
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                            session: *session,
                            kind: *kind,
                        };
                        if candidate.snapshot(leased).is_some() {
                            Some(leased)
                        } else {
                            self.inventory_target(Some(TruthView::Local), *target)
                                .map(|(target, _)| target)
                        }
                    }
                    _ => self
                        .inventory_target(Some(TruthView::Local), *target)
                        .map(|(target, _)| target),
                })
                .collect::<BTreeSet<_>>();
            local.publication = Some(CanonicalInventoryPublication {
                snapshots: targets
                    .into_iter()
                    .filter_map(|target| candidate.snapshot(target))
                    .collect(),
                operations: Vec::new(),
                before_revisions: Vec::new(),
                relocated_targets: Vec::new(),
            });
            local.inventory = Some(candidate);
            local.versions.extend(versions);
            local.inventory_admissions = admissions;
            local
        } else {
            self.prepare_commit(TruthView::Local, &deltas.writes, true)?
        };
        Ok(self.apply_prepared(local))
    }

    fn prepare_inventory_action_steps(
        &self,
        view: TruthView,
        rows: &[(
            pumpkin_cluster::canonical_action::ActionRef,
            &[AppliedWrite],
        )],
    ) -> Result<
        (
            InvLedger,
            Vec<(Target, u64)>,
            Vec<pumpkin_cluster::canonical_action::LivePlayerLease>,
        ),
        ApplyFailure,
    > {
        use pumpkin_cluster::canonical_action::SpatialEpoch;
        let mut candidate = self.inventory.phase_candidate(view);
        let mut versions = self
            .versions
            .iter()
            .filter_map(|((at, target), revision)| (*at == view).then_some((*target, *revision)))
            .collect::<BTreeMap<_, _>>();
        let mut updated = BTreeMap::new();
        let mut admissions = Vec::new();
        for (reference, writes) in rows {
            let entity_lifecycles = writes
                .iter()
                .filter_map(|write| match write.target {
                    Target::Chunk {
                        key,
                        part: ChunkPart::Entity(entity),
                        inventory: Some(InventoryId::Entity { .. }),
                    } if write.before.is_none() || write.after.is_none() => Some((key, entity)),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let mut handled = BTreeSet::new();
            let menu_roots = writes
                .iter()
                .filter_map(|write| {
                    (write.before.is_none() || write.after.is_none())
                        .then(|| self.inventory.canonical_menu_admission_slot(write.target))
                        .flatten()
                        .map(|root| (root.admission.inventory_target(), root))
                })
                .collect::<BTreeMap<_, _>>();
            for (address, root) in menu_roots {
                if root.reference != *reference {
                    return Err(ApplyFailure::InvalidAction);
                }
                let entries = root.admission.entries();
                let slots = entries
                    .iter()
                    .map(|(target, _)| {
                        writes
                            .iter()
                            .find(|write| write.target == *target)
                            .ok_or(ApplyFailure::Inventory(*target))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let removing = slots.iter().all(|write| write.after.is_none());
                if removing {
                    let snapshot = candidate
                        .snapshot(address)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    for write in &slots {
                        let Some(Cell {
                            value: Value::Inventory(item),
                            revision,
                        }) = &write.before
                        else {
                            return Err(ApplyFailure::Inventory(write.target));
                        };
                        if snapshot.cells.iter().find(|actual| actual.loc == item.loc) != Some(item)
                            || *revision != versions.get(&write.target).copied().unwrap_or(0)
                        {
                            return Err(ApplyFailure::Inventory(write.target));
                        }
                    }
                    candidate.remove_target(address);
                } else {
                    if candidate.snapshot(address).is_some() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    for ((target, empty), write) in entries.iter().zip(&slots) {
                        if write.before.is_some()
                            || !write.after.as_ref().is_some_and(|cell| {
                                cell.revision == 0 && cell.value == Value::Inventory(empty.clone())
                            })
                        {
                            return Err(ApplyFailure::Inventory(*target));
                        }
                        updated.insert(*target, 0);
                        versions.insert(*target, 0);
                    }
                    candidate
                        .install_owned_snapshot(
                            InventoryStateSnapshot {
                                target: address,
                                revision: 0,
                                cells: entries.iter().map(|(_, item)| item.clone()).collect(),
                            },
                            candidate.target_chunks(InventoryTarget::LeasedPlayer {
                                key: root.admission.lease.key.0,
                                player: root.admission.lease.gid,
                                incarnation: root.admission.lease.incarnation,
                            }).ok_or(ApplyFailure::InvalidAction)?.to_vec(),
                        )
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                }
                handled.extend(entries.iter().map(|(target, _)| *target));
            }
            for (key, entity) in entity_lifecycles {
                let root = self
                    .inventory
                    .canonical_entity_admission_root(entity)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if root.reference == *reference && root.chunk != key {
                    return Err(ApplyFailure::InvalidAction);
                }
                let body_target = Target::Chunk {
                    key,
                    part: ChunkPart::Entity(entity),
                    inventory: None,
                };
                let body_write = writes
                    .iter()
                    .find(|write| write.target == body_target)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let slots = writes.iter().filter(|write| matches!(write.target,
                    Target::Chunk { key: chunk, part: ChunkPart::Entity(id), inventory: Some(InventoryId::Entity { .. }) }
                        if chunk == key && id == entity)).collect::<Vec<_>>();
                let expected = root
                    .inventory
                    .entries(key, entity)
                    .map(|(target, _)| target)
                    .collect::<BTreeSet<_>>();
                if slots.len() != expected.len()
                    || slots
                        .iter()
                        .map(|write| write.target)
                        .collect::<BTreeSet<_>>()
                        != expected
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let target = InventoryTarget::Entity(entity.into());
                if body_write.after.is_none() {
                    let Some(Cell {
                        value: Value::Entity(body),
                        ..
                    }) = &body_write.before
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if body.birth != root.body.birth || body.kind != root.body.kind {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let snapshot = candidate
                        .snapshot(target)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    for write in &slots {
                        let Some(Cell {
                            value: Value::Inventory(item),
                            revision,
                        }) = &write.before
                        else {
                            return Err(ApplyFailure::Inventory(write.target));
                        };
                        let actual = snapshot
                            .cells
                            .iter()
                            .find(|cell| cell.loc == item.loc)
                            .cloned()
                            .unwrap_or_else(|| {
                                pumpkin_cluster::inventory::InventoryCell::empty(item.loc)
                            });
                        if write.after.is_some()
                            || *item != actual
                            || *revision != versions.get(&write.target).copied().unwrap_or(0)
                        {
                            return Err(ApplyFailure::Inventory(write.target));
                        }
                        handled.insert(write.target);
                        updated.insert(write.target, *revision);
                    }
                    candidate
                        .remove_target(target)
                        .ok_or(ApplyFailure::InvalidAction)?;
                } else {
                    let Some(Cell {
                        value: Value::Entity(body),
                        ..
                    }) = &body_write.after
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if body_write.before.is_some()
                        || body.birth != root.body.birth
                        || body.kind != root.body.kind
                        || candidate.snapshot(target).is_some()
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let mut cells = Vec::new();
                    let mut highest_revision = 0;
                    for write in &slots {
                        let Some(Cell {
                            value: Value::Inventory(item),
                            revision,
                        }) = &write.after
                        else {
                            return Err(ApplyFailure::Inventory(write.target));
                        };
                        if write.before.is_some() {
                            return Err(ApplyFailure::Inventory(write.target));
                        }
                        if root.reference == *reference
                            && root
                                .inventory
                                .entries(key, entity)
                                .find(|(target, _)| *target == write.target)
                                .is_none_or(|(_, cell)| Some(cell) != write.after.as_ref())
                        {
                            return Err(ApplyFailure::Inventory(write.target));
                        }
                        cells.push(item.clone());
                        highest_revision = highest_revision.max(*revision);
                        versions.insert(write.target, *revision);
                        updated.insert(write.target, *revision);
                        handled.insert(write.target);
                    }
                    candidate
                        .install_owned_snapshot(
                            InventoryStateSnapshot {
                                target,
                                revision: root
                                    .inventory
                                    .imported_revision
                                    .checked_add(highest_revision)
                                    .ok_or(ApplyFailure::InvalidAction)?,
                                cells,
                            },
                            vec![key],
                        )
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                }
            }
            let births = writes
                .iter()
                .filter(|write| {
                    write.target.inventory().is_some()
                        && write.before.is_none()
                        && !handled.contains(&write.target)
                })
                .collect::<Vec<_>>();
            if !births.is_empty() {
                if let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(entity),
                    inventory: Some(InventoryId::Entity { .. }),
                } = births[0].target
                {
                    let root = self
                        .inventory
                        .canonical_entity_admission_root(entity)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let body = Target::Chunk {
                        key,
                        part: ChunkPart::Entity(entity),
                        inventory: None,
                    };
                    if root.reference != *reference
                        || root.chunk != key
                        || births.len() != root.inventory.cells.len()
                        || !writes.iter().any(|write| {
                            write.target == body
                                && write.before.is_none()
                                && write.after.as_ref().is_some_and(|cell| {
                                    cell.value == Value::Entity((root.body.clone()).into())
                                })
                        })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    for (target, cell) in root.inventory.entries(key, entity) {
                        if !births.iter().any(|write| {
                            write.target == target && write.after.as_ref() == Some(cell)
                        }) {
                            return Err(ApplyFailure::Inventory(target));
                        }
                        updated.insert(target, cell.revision);
                        versions.insert(target, cell.revision);
                    }
                    let target = InventoryTarget::Entity(entity.into());
                    if candidate.snapshot(target).is_some() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    candidate
                        .install_owned_snapshot(
                            InventoryStateSnapshot {
                                target,
                                revision: root.inventory.imported_revision,
                                cells: root
                                    .inventory
                                    .cells
                                    .iter()
                                    .filter_map(|cell| match &cell.value {
                                        Value::Inventory(item) => Some(item.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                            },
                            vec![key],
                        )
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                } else {
                    let Some(lease) = births.first().and_then(|write| match write.target {
                        Target::Player { lease, .. } => lease,
                        _ => None,
                    }) else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    let root = self
                        .inventory
                        .canonical_admission_root(lease)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let body = Target::Chunk {
                        key: root.chunk,
                        part: ChunkPart::Player(lease.gid),
                        inventory: None,
                    };
                    if root.reference != *reference
                        || births.len() != root.inventory.targets(lease).count()
                        || !writes.iter().any(|write| {
                            write.target == body
                                && write.before.is_none()
                                && write.after.as_ref().is_some_and(|cell| {
                                    cell.value == Value::Living((root.initial.clone()).into())
                                })
                        })
                        || root.initial.avatar.as_ref().is_none_or(|avatar| {
                            avatar.spatial_epoch
                                != (SpatialEpoch::Admission {
                                    lease,
                                    admitted_at: reference.tick,
                                })
                        })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    for (target, cell) in root.inventory.entries(lease) {
                        if !births.iter().any(|write| {
                            write.target == target && write.after.as_ref() == Some(cell)
                        }) {
                            return Err(ApplyFailure::Inventory(target));
                        }
                        updated.insert(target, cell.revision);
                        versions.insert(target, cell.revision);
                    }
                    let target = InventoryTarget::LeasedPlayer {
                        key: lease.key.0,
                        player: lease.gid,
                        incarnation: lease.incarnation,
                    };
                    if candidate.snapshot(target).is_some() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    candidate
                        .install_owned_snapshot(
                            InventoryStateSnapshot {
                                target,
                                revision: root.inventory.imported_revision,
                                cells: root
                                    .inventory
                                    .cells
                                    .iter()
                                    .filter_map(|cell| match &cell.value {
                                        Value::Inventory(item) => Some(item.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                            },
                            vec![root.chunk],
                        )
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                    for supplemental in &root.inventory.supplemental {
                        let target = InventoryTarget::LeasedVirtual {
                            host: lease.gid.server,
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                            session: 0,
                            kind: supplemental.kind,
                        };
                        if candidate.snapshot(target).is_some() {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        candidate
                            .install_owned_snapshot(
                                InventoryStateSnapshot {
                                    target,
                                    revision: supplemental.imported_revision,
                                    cells: supplemental
                                        .cells
                                        .iter()
                                        .filter_map(|cell| match &cell.value {
                                            Value::Inventory(item) => Some(item.clone()),
                                            _ => None,
                                        })
                                        .collect(),
                                },
                                vec![root.chunk],
                            )
                            .map_err(|_| ApplyFailure::InvalidAction)?;
                    }
                    admissions.push(lease);
                }
            }
            let mut operations = Vec::new();
            let mut expected = BTreeMap::new();
            for write in writes.iter().filter(|write| {
                write.target.inventory().is_some()
                    && write.before.is_some()
                    && !handled.contains(&write.target)
            }) {
                let (target, loc) = match write.target {
                    Target::Player {
                        lease: Some(lease),
                        inventory: Some(InventoryId::Player { inv, slot }),
                        ..
                    } => {
                        let leased = InventoryTarget::LeasedPlayer {
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                        };
                        (
                            if candidate.snapshot(leased).is_some() {
                                leased
                            } else {
                                self.inventory_target(Some(view), write.target)
                                    .ok_or(ApplyFailure::Inventory(write.target))?
                                    .0
                            },
                            InvLoc::new(inv, slot),
                        )
                    }
                    Target::Player {
                        lease: Some(lease),
                        inventory:
                            Some(InventoryId::Virtual {
                                host,
                                kind,
                                session,
                                inv,
                                slot,
                            }),
                        ..
                    } => {
                        let leased = InventoryTarget::LeasedVirtual {
                            host,
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                            session,
                            kind,
                        };
                        (
                            if candidate.snapshot(leased).is_some() {
                                leased
                            } else {
                                self.inventory_target(Some(view), write.target)
                                    .ok_or(ApplyFailure::Inventory(write.target))?
                                    .0
                            },
                            InvLoc::new(inv, slot),
                        )
                    }
                    _ => self
                        .inventory_target(Some(view), write.target)
                        .ok_or(ApplyFailure::Inventory(write.target))?,
                };
                let snapshot = candidate
                    .snapshot(target)
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let before = write
                    .before
                    .as_ref()
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let after = write
                    .after
                    .as_ref()
                    .ok_or(ApplyFailure::Inventory(write.target))?;
                let (Value::Inventory(before_item), Value::Inventory(after_item)) =
                    (&before.value, &after.value)
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                let actual = snapshot
                    .cells
                    .iter()
                    .find(|item| item.loc == loc)
                    .cloned()
                    .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc));
                if before.value != Value::Inventory(actual)
                    || before.revision != versions.get(&write.target).copied().unwrap_or(0)
                    || before_item.loc != loc
                    || after_item.loc != loc
                {
                    return Err(ApplyFailure::Inventory(write.target));
                }
                expected.insert(target, snapshot.revision);
                operations.push(SemanticInventoryOp {
                    target,
                    before: before_item.clone(),
                    after: after_item.clone(),
                });
                versions.insert(write.target, after.revision);
                updated.insert(write.target, after.revision);
            }
            if !operations.is_empty() {
                candidate
                    .apply_mutation(InventoryMutation {
                        expected_revisions: expected.into_iter().collect(),
                        operations,
                    })
                    .map_err(|_| ApplyFailure::InvalidAction)?;
            }
        }
        Ok((candidate, updated.into_iter().collect(), admissions))
    }

    pub(super) fn inventory_snapshots_for_action(
        &self,
        action: &Action,
    ) -> Result<Vec<InventoryStateSnapshot>, ApplyFailure> {
        if action
            .player_leases
            .iter()
            .filter(|lease| {
                action.targets().into_iter().any(|target| {
                    matches!(target,
                Target::Player { key, inventory: Some(_), .. } if key == lease.key)
                })
            })
            .any(|lease| !self.inventory.live_player_lease_matches(*lease))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut targets = std::collections::BTreeSet::new();
        for target in action.targets() {
            if target.inventory().is_none() {
                continue;
            }
            let (address, _) = self
                .inventory_target(Some(TruthView::Local), target)
                .ok_or(ApplyFailure::Inventory(target))?;
            targets.insert(address);
        }
        targets
            .into_iter()
            .map(|target| {
                self.inventory
                    .phase_snapshot(TruthView::Local, target)
                    .ok_or(ApplyFailure::InvalidAction)
            })
            .collect()
    }

    pub(super) fn commit_joint(
        &mut self,
        ground: &[AppliedWrite],
        local: &[AppliedWrite],
    ) -> Result<Option<CanonicalInventoryPublication>, ApplyFailure> {
        let ground = self.prepare_commit(TruthView::Ground, ground, true)?;
        let local = self.prepare_commit(TruthView::Local, local, true)?;
        self.apply_prepared(ground);
        Ok(self.apply_prepared(local))
    }

    fn apply_prepared(
        &mut self,
        prepared: PreparedCommit,
    ) -> Option<CanonicalInventoryPublication> {
        self.blocks.install_replacement(prepared.blocks);
        if let Some(inventory) = prepared.inventory {
            self.inventory
                .install_phase_mutation(prepared.view, inventory);
        }
        for lease in prepared.inventory_admissions {
            self.inventory.activate_player_admission(
                lease,
                self.server,
                prepared.view == TruthView::Local,
            );
        }
        self.living.install_replacement(prepared.living);
        for (actor, state, spatial) in prepared.living_lifecycles {
            self.living
                .install_lifecycle(actor, prepared.view == TruthView::Ground, state);
            if let ActorId::Player(gid) = actor {
                self.actors.install_spatial_lifecycle(
                    gid,
                    prepared.view == TruthView::Ground,
                    spatial,
                );
            }
        }
        self.actors.install_replacement(prepared.spatial);
        for (chunk, cell) in prepared.scheduled {
            self.blocks
                .install_scheduled_phase(prepared.view == TruthView::Ground, chunk, cell);
        }
        for (chunk, position, cell) in prepared.block_entities {
            self.blocks.install_block_entity_phase(
                prepared.view == TruthView::Ground,
                chunk,
                position,
                cell,
            );
        }
        for (chunk, cell) in prepared.occupancy {
            assert!(self.blocks.install_occupancy_phase(
                prepared.view == TruthView::Ground,
                chunk,
                cell
            ));
        }
        for (chunk, section_y, cell) in prepared.light_sections {
            self.blocks.install_light_section_phase(
                prepared.view == TruthView::Ground,
                chunk,
                section_y,
                cell,
            );
        }
        for (chunk, section_y, cell) in prepared.block_sections {
            self.blocks.install_block_section_phase(
                prepared.view == TruthView::Ground,
                chunk,
                section_y,
                cell,
            );
        }
        // Delete old realm legs before installing destinations. The identity
        // map therefore never loses a newly installed destination to a later
        // old-realm deletion in a joint move.
        for (chunk, id, _) in prepared
            .entity_bodies
            .iter()
            .filter(|(_, _, cell)| cell.is_none())
        {
            self.blocks.install_entity_body_phase(
                prepared.view == TruthView::Ground,
                *chunk,
                *id,
                None,
            );
        }
        for (chunk, id, cell) in prepared
            .entity_bodies
            .into_iter()
            .filter(|(_, _, cell)| cell.is_some())
        {
            self.blocks.install_entity_body_phase(
                prepared.view == TruthView::Ground,
                chunk,
                id,
                cell,
            );
        }
        for target in prepared.removed_versions {
            self.versions.remove(&(prepared.view, target));
        }
        for (target, revision) in prepared.versions {
            self.versions.insert((prepared.view, target), revision);
        }
        prepared.publication
    }

    fn prepare_commit(
        &self,
        view: TruthView,
        writes: &[AppliedWrite],
        relocate_inventory: bool,
    ) -> Result<PreparedCommit, ApplyFailure> {
        let (physical_writes, property_versions) = self.expand_living_properties(view, writes)?;
        let writes = physical_writes.as_slice();
        let light_worlds = writes
            .iter()
            .filter_map(|write| match write.target {
                Target::Chunk {
                    key,
                    part: ChunkPart::LightSection(_),
                    inventory: None,
                } => Some(key.world),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        for world_id in light_worlds {
            let worlds = self.server.worlds.load_full();
            let world = worlds
                .iter()
                .find(|world| {
                    world.level.cluster_world_key == world_id.key
                        && world.level.cluster_dimension == world_id.dimension
                })
                .ok_or(ApplyFailure::InvalidAction)?;
            let world_writes = writes.iter().filter(|write| matches!(write.target, Target::Chunk { key, .. } if key.world == world_id)).cloned().collect::<Vec<_>>();
            super::cluster_world_callbacks::validate_derived_light_writes(
                view,
                self.blocks,
                &world_writes,
                world.level.lighting_config,
                world.dimension.has_skylight,
            )
            .map_err(|_| ApplyFailure::InvalidAction)?;
        }
        let mut block_changes = Vec::new();
        let mut block_sections = Vec::new();
        let mut touched_sections = BTreeSet::new();
        for write in writes {
            if let Target::Chunk {
                key,
                part: ChunkPart::Block(position),
                inventory: None,
            } = write.target
            {
                touched_sections.insert((key, position.y.div_euclid(16)));
            }
            if let Target::Chunk {
                key,
                part: ChunkPart::BlockSection(section_y),
                inventory: None,
            } = write.target
            {
                touched_sections.insert((key, section_y));
            }
        }
        for (key, section_y) in touched_sections {
            let target = Target::Chunk {
                key,
                part: ChunkPart::BlockSection(section_y),
                inventory: None,
            };
            let before = self
                .blocks
                .block_section(view == TruthView::Ground, key, section_y)
                .ok_or(ApplyFailure::Condition(target))?;
            let projected = pumpkin_cluster::block_section::project_writes(target, before, writes)?;
            let next = if let Some(write) = writes.iter().find(|write| write.target == target) {
                if write.before.as_ref() != Some(before) {
                    return Err(ApplyFailure::Condition(target));
                }
                let after = write.after.as_ref().ok_or(ApplyFailure::Type(target))?;
                if after.value != projected.value {
                    return Err(ApplyFailure::Condition(target));
                }
                after.clone()
            } else {
                projected
            };
            block_sections.push((key, section_y, next));
        }
        let mut inventory_changes = Vec::new();
        let mut living_changes = Vec::new();
        let mut living_lifecycles = Vec::new();
        let mut spatial_changes = Vec::new();
        let mut player_writes =
            BTreeMap::<pumpkin_cluster::identity::GlobalPlayerId, Vec<&AppliedWrite>>::new();
        let mut revisions = BTreeMap::new();
        let mut versions = Vec::new();
        let mut relocations = Vec::new();
        let mut scheduled = Vec::new();
        let mut block_entities = Vec::new();
        let mut light_sections = Vec::new();
        let mut occupancy = Vec::new();
        let mut entity_bodies = Vec::new();
        let mut entity_destinations = BTreeSet::new();
        let mut entity_uuids = BTreeSet::new();
        let mut removed_versions = Vec::new();
        for write in writes {
            if let Target::Chunk {
                key,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            } = write.target
            {
                if !self.blocks.contains(key) || self.read(view, write.target) != write.before {
                    return Err(ApplyFailure::InvalidAction);
                }
                let Some(
                    after @ Cell {
                        value: Value::ActorOccupancy(root),
                        ..
                    },
                ) = &write.after
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                let Some(Cell {
                    value: Value::ActorOccupancy(prior),
                    ..
                }) = self.blocks.occupancy_root(view == TruthView::Ground, key)
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                let resolved = root
                    .resolve_against(prior)
                    .ok_or(ApplyFailure::Type(write.target))?;
                let Some(Cell {
                    value: Value::ActorOccupancy(before),
                    ..
                }) = &write.before
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                if before.resolve_against(&resolved).as_ref() != Some(prior) {
                    return Err(ApplyFailure::InvalidAction);
                }
                let complete = Cell {
                    revision: after.revision,
                    value: Value::ActorOccupancy(resolved),
                };
                occupancy.push((key, complete));
                versions.push((write.target, after.revision));
                continue;
            }
            if let Target::Chunk {
                key,
                part: ChunkPart::BlockSection(section_y),
                inventory: None,
            } = write.target
            {
                if !block_sections
                    .iter()
                    .any(|(chunk, y, _)| *chunk == key && *y == section_y)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                continue;
            }
            if let Target::Chunk {
                key,
                part: ChunkPart::LightSection(section_y),
                inventory: None,
            } = write.target
            {
                if self.read(view, write.target) != write.before {
                    return Err(ApplyFailure::InvalidAction);
                }
                let Some(
                    after @ Cell {
                        value: Value::LightSection { sky, block },
                        ..
                    },
                ) = &write.after
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                if sky.len() != 2048 || block.len() != 2048 {
                    return Err(ApplyFailure::Type(write.target));
                }
                light_sections.push((key, section_y, Some(after.clone())));
                versions.push((write.target, after.revision));
                continue;
            }
            if write.target.inventory().is_some() {
                let (target, operation, revision) = self.inventory_write(view, write)?;
                let Some(after) = &write.after else {
                    return Err(ApplyFailure::Inventory(write.target));
                };
                if revisions
                    .insert(target, revision)
                    .is_some_and(|stored| stored != revision)
                {
                    return Err(ApplyFailure::Inventory(write.target));
                }
                inventory_changes.push(operation);
                versions.push((write.target, after.revision));
                continue;
            }
            if let Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            } = write.target
            {
                if !self.blocks.contains(key) || self.read(view, write.target) != write.before {
                    return Err(ApplyFailure::InvalidAction);
                }
                if let Some(after) = &write.after {
                    let Value::Entity(body) = &after.value else {
                        return Err(ApplyFailure::Type(write.target));
                    };
                    if !valid_entity_body(body, key, id)
                        || !self.blocks.entity_birth_matches(id, &body.birth)
                    {
                        return Err(ApplyFailure::Type(write.target));
                    }
                    if !entity_destinations.insert(id) || !entity_uuids.insert(body.birth.uuid) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if let Some(held) = self.blocks.entity_body_chunk(view == TruthView::Ground, id)
                        && held != key
                        && !writes.iter().any(|leg| {
                            leg.target
                                == Target::Chunk {
                                    key: held,
                                    part: ChunkPart::Entity(id),
                                    inventory: None,
                                }
                                && leg.after.is_none()
                        })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let existing = self
                        .blocks
                        .entity_body_chunk(view == TruthView::Ground, id)
                        .and_then(|held| {
                            self.blocks.entity_body(view == TruthView::Ground, held, id)
                        });
                    if let Some(Cell {
                        value: Value::Entity(before),
                        ..
                    }) = existing
                        && (before.birth != body.birth
                            || before.kind != body.kind
                            || std::mem::discriminant(&before.payload)
                                != std::mem::discriminant(&body.payload))
                    {
                        return Err(ApplyFailure::Type(write.target));
                    }
                    versions.push((write.target, after.revision));
                } else {
                    removed_versions.push(write.target);
                }
                entity_bodies.push((key, id, write.after.clone()));
                continue;
            }
            if let Target::Chunk {
                part: ChunkPart::Player(actor),
                inventory: None,
                ..
            } = write.target
            {
                player_writes.entry(actor).or_default().push(write);
                continue;
            }
            if let Target::Chunk {
                key,
                part: ChunkPart::Whole,
                inventory: None,
            } = write.target
            {
                if !self.blocks.contains(key) || self.read(view, write.target) != write.before {
                    return Err(ApplyFailure::InvalidAction);
                }
                let Some(
                    after @ Cell {
                        value: Value::ScheduledQueue(queue),
                        ..
                    },
                ) = &write.after
                else {
                    return Err(ApplyFailure::Type(write.target));
                };
                if !valid_scheduled_queue(key, queue) {
                    return Err(ApplyFailure::InvalidAction);
                }
                scheduled.push((key, after.clone()));
                versions.push((write.target, after.revision));
                continue;
            }
            if let Target::Chunk {
                key,
                part: ChunkPart::BlockEntity(position),
                inventory: None,
            } = write.target
            {
                if !self.blocks.contains(key) || self.read(view, write.target) != write.before {
                    return Err(ApplyFailure::InvalidAction);
                }
                let block_target = Target::Chunk {
                    key,
                    part: ChunkPart::Block(position),
                    inventory: None,
                };
                let state = writes
                    .iter()
                    .find(|candidate| candidate.target == block_target)
                    .and_then(|candidate| candidate.after.as_ref())
                    .and_then(|cell| match cell.value {
                        Value::Block(state) => Some(state),
                        _ => None,
                    })
                    .or_else(|| {
                        let block = if view == TruthView::Ground {
                            self.blocks.ground_block(key, position)
                        } else {
                            self.blocks.block(key, position)
                        };
                        block.ok().map(|state| state.as_u16())
                    })
                    .and_then(pumpkin_data::BlockStateId::new)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if let Some(after) = &write.after {
                    let Value::BlockEntity(bytes) = &after.value else {
                        return Err(ApplyFailure::Type(write.target));
                    };
                    if bytes.is_empty() || bytes.len() > 65_536 {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if !pumpkin_world::level::cluster_block_entity_document_matches(
                        pumpkin_util::math::position::BlockPos::new(
                            position.x, position.y, position.z,
                        ),
                        bytes,
                    ) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if !pumpkin_world::level::cluster_block_entity_document_type_matches(
                        state, bytes,
                    ) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    versions.push((write.target, after.revision));
                } else {
                    if pumpkin_data::BlockState::from_id(state).block_entity_type != u16::MAX {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    removed_versions.push(write.target);
                }
                block_entities.push((key, position, write.after.clone()));
                continue;
            }
            let Target::Chunk {
                key,
                part: ChunkPart::Block(position),
                inventory: None,
            } = write.target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let (Some(before), Some(after)) = (&write.before, &write.after) else {
                return Err(ApplyFailure::InvalidAction);
            };
            if self.read(view, write.target).as_ref() != Some(before) {
                return Err(ApplyFailure::InvalidAction);
            }
            let (Value::Block(old), Value::Block(new)) = (&before.value, &after.value) else {
                return Err(ApplyFailure::Type(write.target));
            };
            let (Some(old), Some(new)) = (
                pumpkin_data::BlockStateId::new(*old),
                pumpkin_data::BlockStateId::new(*new),
            ) else {
                return Err(ApplyFailure::Type(write.target));
            };
            let old_type = pumpkin_data::BlockState::from_id(old).block_entity_type;
            let new_type = pumpkin_data::BlockState::from_id(new).block_entity_type;
            if old_type != new_type {
                let target = Target::Chunk {
                    key,
                    part: ChunkPart::BlockEntity(position),
                    inventory: None,
                };
                let entity_write = writes.iter().find(|candidate| candidate.target == target);
                if (new_type == u16::MAX
                    && self
                        .blocks
                        .block_entity(view == TruthView::Ground, key, position)
                        .is_some()
                    && !entity_write.is_some_and(|candidate| candidate.after.is_none()))
                    || (new_type != u16::MAX
                        && !entity_write.is_some_and(|candidate| candidate.after.is_some()))
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            block_changes.push(BlockChange {
                chunk: key,
                position,
                before: old,
                after: new,
            });
            versions.push((write.target, after.revision));
        }
        for (actor, writes) in player_writes {
            if let [write] = writes.as_slice()
                && (write.before.is_none() || write.after.is_none())
            {
                if self.read(view, write.target) != write.before {
                    return Err(ApplyFailure::Condition(write.target));
                }
                let after = match &write.after {
                    Some(Cell {
                        value: Value::Living(living),
                        revision,
                    }) if living.belongs_to(ActorId::Player(actor)) => {
                        versions.push((write.target, *revision));
                        Some(living.as_ref().clone())
                    }
                    None => {
                        removed_versions.push(write.target);
                        None
                    }
                    _ => return Err(ApplyFailure::Type(write.target)),
                };
                let spatial = if after.is_some() {
                    Some(
                        self.actors
                            .prepared_spatial(actor)
                            .or_else(|| self.actors.phase_spatial(actor, view == TruthView::Ground))
                            .ok_or(ApplyFailure::InvalidAction)?,
                    )
                } else {
                    None
                };
                living_lifecycles.push((ActorId::Player(actor), after, spatial));
                continue;
            }
            let (living, spatial, revision) = self.prepare_player_writes(view, actor, &writes)?;
            let current = self.actors.get(actor).ok_or(ApplyFailure::InvalidAction)?;
            let old_chunk = match view {
                TruthView::Local => current.local.chunk,
                TruthView::Ground => current.ground.chunk,
            };
            if relocate_inventory && old_chunk != spatial.chunk {
                relocations.push((actor, old_chunk, spatial.chunk));
            }
            living_changes.push((ActorId::Player(actor), living));
            spatial_changes.push((actor, spatial));
            versions.push(revision);
        }
        let blocks = self
            .blocks
            .prepare_replacement(matches!(view, TruthView::Ground), &block_changes)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let living = self
            .living
            .prepare_replacement(matches!(view, TruthView::Ground), living_changes)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let spatial = self
            .actors
            .prepare_replacement(matches!(view, TruthView::Ground), spatial_changes)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let (inventory, publication) = if inventory_changes.is_empty() && relocations.is_empty() {
            (None, None)
        } else {
            let mut targets = revisions
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            let before_revisions = revisions
                .iter()
                .map(|(target, revision)| (*target, *revision))
                .collect();
            let publication_operations =
                (view == TruthView::Local).then(|| inventory_changes.clone());
            let mut candidate = if inventory_changes.is_empty() {
                self.inventory.phase_candidate(view)
            } else {
                self.inventory
                    .preview_phase_mutation(
                        view,
                        InventoryMutation {
                            expected_revisions: revisions.into_iter().collect(),
                            operations: inventory_changes,
                        },
                    )
                    .map_err(|_| ApplyFailure::InvalidAction)?
            };
            let mut relocated_targets = Vec::new();
            for (actor, old_chunk, new_chunk) in relocations {
                let lease = self
                    .inventory
                    .player_lease_for_gid(actor)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let matching = candidate
                    .targets_for_chunk(old_chunk)
                    .into_iter()
                    .filter(|target| match target {
                        InventoryTarget::Player(gid) => *gid == actor,
                        InventoryTarget::LeasedPlayer {
                            key,
                            player,
                            incarnation,
                        }
                        | InventoryTarget::LeasedVirtual {
                            key,
                            player,
                            incarnation,
                            ..
                        } => {
                            *player == actor
                                && *key == lease.key.0
                                && *incarnation == lease.incarnation
                        }
                        InventoryTarget::Virtual {
                            owner: ActorId::Player(gid),
                            ..
                        } => *gid == actor,
                        _ => false,
                    })
                    .collect::<Vec<_>>();
                if matching.is_empty() {
                    return Err(ApplyFailure::InvalidAction);
                }
                for target in matching {
                    let chunks = candidate
                        .target_chunks(target)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    if !chunks.contains(&old_chunk) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let next = if matches!(target, InventoryTarget::Virtual { owner: ActorId::Player(_), .. } | InventoryTarget::LeasedVirtual { .. }) {
                        vec![new_chunk]
                    } else {
                        chunks.iter().map(|chunk| if *chunk == old_chunk { new_chunk } else { *chunk }).collect()
                    };
                    candidate
                        .relocate_target(target, next)
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                    targets.insert(target);
                    relocated_targets.push((target, old_chunk, new_chunk));
                }
            }
            let publication = if view == TruthView::Local {
                let snapshots = targets
                    .into_iter()
                    .map(|target| {
                        candidate
                            .snapshot(target)
                            .ok_or(ApplyFailure::InvalidAction)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                super::cluster_invsee::validate_imported_inventory_snapshots(&snapshots)
                    .map_err(|_| ApplyFailure::InvalidAction)?;
                for snapshot in &snapshots {
                    match snapshot.target {
                        InventoryTarget::Player(_) => {
                            super::cluster_invsee::cluster_player_inventory_snapshot_from_state(
                                snapshot,
                            )
                            .map_err(|_| ApplyFailure::InvalidAction)?;
                        }
                        InventoryTarget::Virtual {
                            owner: ActorId::Player(_),
                            session: 0,
                            kind: 0x100,
                            ..
                        } => {
                            super::cluster_invsee::ender_chest_stacks_from_state(snapshot)
                                .map_err(|_| ApplyFailure::InvalidAction)?;
                        }
                        _ => {}
                    }
                }
                Some(CanonicalInventoryPublication {
                    snapshots,
                    operations: publication_operations.unwrap_or_default(),
                    before_revisions,
                    relocated_targets,
                })
            } else {
                None
            };
            (Some(candidate), publication)
        };
        versions.extend(property_versions);
        Ok(PreparedCommit {
            view,
            blocks,
            inventory,
            living,
            living_lifecycles,
            spatial,
            scheduled,
            block_entities,
            light_sections,
            occupancy,
            block_sections,
            entity_bodies,
            versions,
            removed_versions,
            publication,
            inventory_admissions: Vec::new(),
        })
    }

    fn prepare_player_writes(
        &self,
        view: TruthView,
        actor: pumpkin_cluster::identity::GlobalPlayerId,
        writes: &[&AppliedWrite],
    ) -> Result<
        (
            pumpkin_cluster::item_action::ActorLivingState,
            super::cluster_world_actor::ActorSpatialState,
            (Target, u64),
        ),
        ApplyFailure,
    > {
        let current = self.actors.get(actor).ok_or(ApplyFailure::InvalidAction)?;
        let spatial = match view {
            TruthView::Local => current.local,
            TruthView::Ground => current.ground,
        };
        let (source, destination) = match writes {
            [write] if write.before.is_some() && write.after.is_some() => (*write, *write),
            [first, second] => {
                let (source, destination) = if first.before.is_some()
                    && first.after.is_none()
                    && second.before.is_none()
                    && second.after.is_some()
                {
                    (*first, *second)
                } else if second.before.is_some()
                    && second.after.is_none()
                    && first.before.is_none()
                    && first.after.is_some()
                {
                    (*second, *first)
                } else {
                    return Err(ApplyFailure::InvalidAction);
                };
                (source, destination)
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let Target::Chunk { key: old_chunk, .. } = source.target else {
            return Err(ApplyFailure::InvalidAction);
        };
        let Target::Chunk { key: new_chunk, .. } = destination.target else {
            return Err(ApplyFailure::InvalidAction);
        };
        if spatial.chunk != old_chunk
            || self.read(view, source.target).as_ref() != source.before.as_ref()
            || source.target != destination.target && self.read(view, destination.target).is_some()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let Some(after) = destination.after.as_ref() else {
            return Err(ApplyFailure::InvalidAction);
        };
        let Value::Living(living) = &after.value else {
            return Err(ApplyFailure::Type(destination.target));
        };
        let Some(avatar) = living.avatar.as_ref() else {
            return Err(ApplyFailure::InvalidAction);
        };
        if !living.belongs_to(ActorId::Player(actor)) || avatar.world != new_chunk.world {
            return Err(ApplyFailure::InvalidAction);
        }
        let position = avatar.position.map(f64::from_bits);
        let expected_chunk = ChunkKey::new(
            avatar.world,
            (position[0].floor() as i32).div_euclid(16),
            (position[2].floor() as i32).div_euclid(16),
        );
        if expected_chunk != new_chunk || !self.blocks.contains(new_chunk) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut next = spatial;
        next.world = avatar.world;
        next.chunk = new_chunk;
        let position_block = pumpkin_cluster::protocol::BlockPos {
            x: position[0].floor() as i32,
            y: position[1].floor() as i32,
            z: position[2].floor() as i32,
        };
        next.biome_id = match view {
            TruthView::Local => self.blocks.rough_biome_id(new_chunk, position_block),
            TruthView::Ground => self.blocks.rough_ground_biome_id(new_chunk, position_block),
        };
        next.frame.effective.position = position;
        next.frame.effective.velocity = living
            .combat
            .velocity_micro
            .map(|value| value as f64 / 1_000_000.0);
        next.frame.effective.yaw = f32::from_bits(avatar.yaw);
        next.head_yaw = f32::from_bits(avatar.head_yaw);
        next.frame.effective.pitch = f32::from_bits(avatar.pitch);
        next.frame.effective.on_ground = avatar.on_ground;
        next.frame.effective.tick = avatar.movement_tick;
        next.frame.effective.source_seq = avatar.movement_sequence;
        next.game_mode = avatar.game_mode;
        if next.world != spatial.world
            || next.chunk != spatial.chunk
            || next.biome_id != spatial.biome_id
            || next.frame.effective.position != spatial.frame.effective.position
            || next.frame.effective.velocity != spatial.frame.effective.velocity
            || next.frame.effective.yaw != spatial.frame.effective.yaw
            || next.head_yaw != spatial.head_yaw
            || next.frame.effective.pitch != spatial.frame.effective.pitch
            || next.frame.effective.on_ground != spatial.frame.effective.on_ground
            || next.frame.effective.tick != spatial.frame.effective.tick
            || next.frame.effective.source_seq != spatial.frame.effective.source_seq
            || next.game_mode != spatial.game_mode
        {
            next.revision = next.revision.wrapping_add(1);
        }
        Ok((
            living.as_ref().clone(),
            next,
            (destination.target, after.revision),
        ))
    }

    fn inventory_target(
        &self,
        view: Option<TruthView>,
        target: Target,
    ) -> Option<(InventoryTarget, InvLoc)> {
        let id = match target {
            Target::Chunk {
                inventory: Some(id),
                ..
            }
            | Target::Player {
                inventory: Some(id),
                ..
            } => id,
            _ => return None,
        };
        let inventory = match (target, id) {
            (
                Target::Player {
                    key,
                    lease: Some(lease),
                    ..
                },
                InventoryId::Player { .. },
            ) => (lease.key == key)
                .then(|| self.inventory.player_inventory_address(view, lease))
                .flatten()?,
            (
                Target::Player {
                    key,
                    lease: Some(lease),
                    ..
                },
                InventoryId::Virtual {
                    host,
                    kind,
                    session,
                    ..
                },
            ) => {
                if lease.key != key {
                    return None;
                }
                self.inventory
                    .player_virtual_address(view, lease, host, session, kind)?
            }
            (
                Target::Chunk {
                    key,
                    part: ChunkPart::Block(position),
                    ..
                },
                InventoryId::Block { .. },
            ) => InventoryTarget::Block {
                chunk: key,
                position,
            },
            (
                Target::Chunk {
                    key,
                    part: ChunkPart::Block(position),
                    ..
                },
                InventoryId::Virtual {
                    host,
                    kind,
                    session,
                    ..
                },
            ) => InventoryTarget::BlockVirtual {
                host,
                chunk: key,
                position,
                session,
                kind,
            },
            (
                Target::Chunk {
                    part: ChunkPart::Entity(entity),
                    ..
                },
                InventoryId::Entity { .. },
            ) => InventoryTarget::Entity(entity.into()),
            (
                Target::Chunk {
                    part: ChunkPart::Entity(entity),
                    ..
                },
                InventoryId::Virtual {
                    host,
                    kind,
                    session,
                    ..
                },
            ) => InventoryTarget::Virtual {
                host,
                owner: ActorId::Entity(entity),
                session,
                kind,
            },
            _ => return None,
        };
        if let (Some(view), Target::Chunk { key, .. }) = (view, target) {
            if !self
                .inventory
                .phase_target_chunks(view, inventory)
                .is_some_and(|chunks| chunks.contains(&key))
            {
                return None;
            }
        }
        let (inv, slot) = id.location();
        Some((inventory, InvLoc { inv, slot }))
    }

    fn inventory_write(
        &self,
        view: TruthView,
        write: &AppliedWrite,
    ) -> Result<(InventoryTarget, SemanticInventoryOp, u64), ApplyFailure> {
        let (target, loc) = self
            .inventory_target(Some(view), write.target)
            .ok_or(ApplyFailure::Inventory(write.target))?;
        let (Some(before), Some(after)) = (&write.before, &write.after) else {
            return Err(ApplyFailure::Inventory(write.target));
        };
        let (Value::Inventory(before_cell), Value::Inventory(after_cell)) =
            (&before.value, &after.value)
        else {
            return Err(ApplyFailure::Type(write.target));
        };
        if before_cell.loc != loc || after_cell.loc != loc {
            return Err(ApplyFailure::Inventory(write.target));
        }
        if loc.inv != pumpkin_cluster::inventory::INV_SELECTED
            && !after_cell.is_empty()
            && after_cell.count > self.inventory_limit(after_cell, write.target)?
        {
            return Err(ApplyFailure::Inventory(write.target));
        }
        if self.read(view, write.target).as_ref() != Some(before) {
            return Err(ApplyFailure::Inventory(write.target));
        }
        let ledger_revision = self
            .inventory
            .phase_snapshot(view, target)
            .ok_or(ApplyFailure::Inventory(write.target))?
            .revision;
        Ok((
            target,
            SemanticInventoryOp {
                target,
                before: before_cell.clone(),
                after: after_cell.clone(),
            },
            ledger_revision,
        ))
    }
}

pub(super) fn actor_collision_shape(
    target: Target,
    value: &Value,
) -> Result<pumpkin_cluster::actor_occupancy::CollisionShape, ApplyFailure> {
    use pumpkin_cluster::entity_body::EntityPayload;
    let (minimum, maximum, blocks_building) = match value {
        Value::Living(living) => {
            let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
            let dimensions = super::cluster_living_metadata::player_dimensions(
                living,
                super::cluster_living_metadata::player_pose(living),
            );
            let radius = f64::from(dimensions.width) / 2.0;
            (
                [-radius, 0.0, -radius],
                [radius, f64::from(dimensions.height), radius],
                living.combat.health_milli != 0 && avatar.game_mode != 3,
            )
        }
        Value::Entity(body) => {
            let bounds = super::cluster_entity_registry::relative_bounds(body)
                .ok_or(ApplyFailure::Type(target))?;
            let blocks_building = super::cluster_entity_registry::object_blocks_building(body)
                .unwrap_or_else(|| match &body.payload {
                    EntityPayload::Living(living) => living.combat.health_milli != 0,
                    EntityPayload::Vehicle(_) => true,
                    EntityPayload::Item(_)
                    | EntityPayload::Experience(_)
                    | EntityPayload::Projectile(_)
                    | EntityPayload::Object(_) => false,
                });
            (
                [bounds.min.x, bounds.min.y, bounds.min.z],
                [bounds.max.x, bounds.max.y, bounds.max.z],
                blocks_building,
            )
        }
        _ => return Err(ApplyFailure::Type(target)),
    };
    let shape = pumpkin_cluster::actor_occupancy::CollisionShape {
        offsets: [minimum.map(f64::to_bits), maximum.map(f64::to_bits)],
        blocks_building,
        hanging: matches!(value, Value::Entity(body) if super::cluster_entity_registry::object_is_hanging(body)),
    };
    if !shape.valid() {
        return Err(ApplyFailure::Type(target));
    }
    Ok(shape)
}

impl ActionState for OwnedWorldActionState<'_> {
    fn world_tool_light_plans(
        &self,
        location: pumpkin_cluster::world_command::BlockLocation,
        operation: pumpkin_cluster::world_command::ToolOperation,
        current: u16,
    ) -> Result<Vec<pumpkin_cluster::world_action::WorldLightPlan>, ApplyFailure> {
        super::cluster_world_tool_transform::command_light_plans(
            &self.server,
            location,
            operation,
            current,
        )
    }
    fn world_tool_companion(
        &self,
        location: pumpkin_cluster::world_command::BlockLocation,
        current: u16,
    ) -> Result<Option<pumpkin_cluster::world_command::ToolCompanion>, ApplyFailure> {
        super::cluster_world_tool_transform::command_companion(location, current)
    }
    fn world_tool_attributes(
        &self,
    ) -> Result<pumpkin_cluster::world_command::ToolAttributes, ApplyFailure> {
        Ok(super::cluster_world_tool_transform::command_attributes())
    }
    fn world_tool_transform(
        &self,
        operation: pumpkin_cluster::world_command::ToolOperation,
        current: u16,
        item: u16,
    ) -> Result<Option<u16>, ApplyFailure> {
        super::cluster_world_tool_transform::command_transform(operation, current, item)
    }
    fn world_tool_eye_height(
        &self,
        living: &pumpkin_cluster::item_action::ActorLivingState,
    ) -> Result<f64, ApplyFailure> {
        Ok(f64::from(
            super::cluster_living_metadata::player_eye_height(living),
        ))
    }

    fn wake_rest_stat(&self) -> Result<(u16, u32), ApplyFailure> {
        Ok(super::cluster_sleep_validation::rest_stat())
    }

    fn validate_wake_beds(
        &self,
        world: pumpkin_cluster::protocol::WorldId,
        sleeping: pumpkin_cluster::item_action::PlayerSleepingState,
        beds: &[pumpkin_cluster::sleep_action::BedRelease],
    ) -> Result<(), ApplyFailure> {
        super::cluster_sleep_validation::validate_beds(world, sleeping, beds)
    }

    fn validate_morning(
        &self,
        world: pumpkin_cluster::protocol::WorldId,
        morning: pumpkin_cluster::world_time::MorningTransition,
        source: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Result<(), ApplyFailure> {
        if self.server.cluster_mornings.load().resolve(world, morning)
            && source.tick.distance_since(TickStamp(morning.slot as u16)) < 32768
        {
            Ok(())
        } else {
            Err(ApplyFailure::InvalidAction)
        }
    }

    fn actor_occupancy_cut(
        &self,
        target: Target,
        value: &Value,
    ) -> Result<
        (
            pumpkin_cluster::realm_completion::PeerGeneration,
            pumpkin_cluster::actor_occupancy::ActorOccupancyCut,
        ),
        ApplyFailure,
    > {
        use pumpkin_cluster::actor::ActorId;
        use pumpkin_cluster::actor_occupancy::OccupancyIdentity;
        let Target::Chunk {
            key,
            part,
            inventory: None,
        } = target
        else {
            return Err(ApplyFailure::Type(target));
        };
        let actor = match part {
            ChunkPart::Player(gid) => ActorId::Player(gid),
            ChunkPart::Entity(id) => ActorId::Entity(id),
            _ => return Err(ApplyFailure::Type(target)),
        };
        let Some(Cell {
            value: Value::ActorOccupancy(root),
            ..
        }) = self.blocks.occupancy_root(false, key)
        else {
            return Err(ApplyFailure::Condition(target));
        };
        if !root.valid() || !root.is_complete() {
            return Err(ApplyFailure::Condition(target));
        }
        let entry = root
            .entries
            .get(&actor)
            .ok_or(ApplyFailure::Condition(target))?;
        let shape = actor_collision_shape(target, value)?;
        if entry.body_target() != target
            || [0, 2].into_iter().any(|axis| {
                entry.shape.offsets[0][axis] != shape.offsets[0][axis]
                    || entry.shape.offsets[1][axis] != shape.offsets[1][axis]
            })
        {
            return Err(ApplyFailure::Condition(target));
        }
        let matches = match (&entry.identity, value) {
            (OccupancyIdentity::Player(lease), Value::Living(living)) => {
                lease.gid
                    == match actor {
                        ActorId::Player(gid) => gid,
                        _ => return Err(ApplyFailure::Type(target)),
                    }
                    && living
                        .avatar
                        .as_ref()
                        .is_some_and(|avatar| avatar.spatial_epoch == entry.spatial_epoch)
            }
            (OccupancyIdentity::Entity(birth), Value::Entity(body)) => {
                body.belongs_to(
                    match actor {
                        ActorId::Entity(id) => id,
                        _ => return Err(ApplyFailure::Type(target)),
                    },
                    key,
                ) && body.birth == *birth
                    && body.owner == entry.owner
                    && body.spatial_epoch == entry.spatial_epoch
            }
            _ => false,
        };
        if !matches {
            return Err(ApplyFailure::Condition(target));
        }
        Ok((entry.owner, entry.cut))
    }

    fn machine_genesis(&self, chunk: ChunkKey) -> Result<[u8; 32], ApplyFailure> {
        self.blocks.genesis(chunk).ok_or(ApplyFailure::Condition(Target::Chunk {key:chunk,
            part:ChunkPart::Whole,inventory:None}))?.identity()
    }

    fn actor_collision_shape(
        &self,
        target: Target,
        value: &Value,
    ) -> Result<pumpkin_cluster::actor_occupancy::CollisionShape, ApplyFailure> {
        actor_collision_shape(target, value)
    }

    fn validate_world_border(
        &self,
        witness: pumpkin_cluster::world_time::WorldBorderWitness,
        source: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Result<(), ApplyFailure> {
        self.server
            .cluster_world_borders
            .load()
            .resolve(witness, source.tick)
            .map(|_| ())
            .ok_or(ApplyFailure::InvalidAction)
    }

    fn action_source_slot(
        &self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Option<u64> {
        self.server
            .cluster_original_source_slots
            .load()
            .get(&reference)
            .copied()
    }
    fn player_advancement_outcome(
        &self,
        event: &pumpkin_cluster::player_advancement::AdvancementEvent,
        state: &pumpkin_cluster::player_advancement::PlayerAdvancementState,
        achieved_millis: u64,
    ) -> Result<pumpkin_cluster::player_advancement::AdvancementOutcome, ApplyFailure> {
        super::cluster_advancements::outcome(state, event, achieved_millis)
            .ok_or(ApplyFailure::InvalidAction)
    }

    fn recipe_known(&self, id: &str) -> bool {
        super::cluster_advancements::recipe_displays(
            &self
                .server
                .recipe_manager
                .cluster_dynamic_recipes_snapshot(),
        )
        .contains_key(id)
    }
    fn entity_placement_matches(
        &self,
        intent: &pumpkin_cluster::entity_placement::EntityPlacementIntent,
    ) -> bool {
        let Ok(rules) = self.world_rules(intent.rules, intent.body.birth.action.tick) else {
            return false;
        };
        super::cluster_entity_placement::factory_matches(intent, &rules)
    }
    fn falling_damage_kind(
        &self,
        state: u16,
    ) -> Option<pumpkin_cluster::combat_action::DamageKind> {
        let block = pumpkin_data::BlockStateId::new(state)?.to_block();
        Some(
            if [
                &pumpkin_data::Block::ANVIL,
                &pumpkin_data::Block::CHIPPED_ANVIL,
                &pumpkin_data::Block::DAMAGED_ANVIL,
            ]
            .contains(&block)
            {
                pumpkin_cluster::combat_action::DamageKind::Anvil
            } else {
                pumpkin_cluster::combat_action::DamageKind::FallingBlock
            },
        )
    }
    fn primed_tnt_block(&self, state: u16) -> bool {
        pumpkin_data::BlockStateId::new(state)
            .is_some_and(|state| state.to_block() == &pumpkin_data::Block::TNT)
    }
    fn primed_tnt_identity(&self) -> Option<(u16, u16)> {
        Some((
            pumpkin_data::entity::EntityType::TNT.id,
            pumpkin_data::Block::TNT.default_state.id.as_u16(),
        ))
    }
    fn issuer_entity_admission_authority(
        &self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness,
    ) -> Result<(), ApplyFailure> {
        use pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority;
        if !witness.valid() || reference != witness.admission.body.birth.action {
            return Err(ApplyFailure::InvalidAction);
        }
        match &witness.authority {
            IssuerAdmissionAuthority::Command { .. } => super::cluster_command_issuance::validate(
                reference,
                witness,
                self.action_source_slot(reference),
            ),
            IssuerAdmissionAuthority::Primary { rules } => {
                match self.action_source_slot(reference) {
                    Some(slot) if slot == witness.source_slot => {}
                    Some(_) => return Err(ApplyFailure::InvalidAction),
                    None => {
                        return Err(ApplyFailure::MissingStakeholder(
                            pumpkin_cluster::canonical_action::Stakeholder::Chunk(
                                witness.actor_chunk,
                            ),
                        ));
                    }
                }
                self.server
                    .cluster_world_rules
                    .load()
                    .resolve(*rules, reference.tick)?;
                Ok(())
            }
        }
    }

    fn issuer_entity_initialization_matches(
        &self,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness,
    ) -> Result<(), ApplyFailure> {
        super::cluster_owned_mob_admission::validate_initialization(witness)
    }

    fn world_rules(
        &self,
        witness: pumpkin_cluster::world_rules::WorldRulesWitness,
        tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<pumpkin_cluster::world_rules::WorldRulesSnapshot, ApplyFailure> {
        self.server
            .cluster_world_rules
            .load()
            .resolve(witness, tick)
            .cloned()
    }

    fn resolve_world_respawn(
        &self,
        plan: pumpkin_cluster::world_respawn::WorldRespawnPlan,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Option<pumpkin_cluster::world_respawn::WorldRespawnResolution>, ApplyFailure> {
        super::cluster_world_respawn::resolve(plan, values)
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
    fn derive_world_light(
        &self,
        plan: pumpkin_cluster::world_action::WorldLightPlan,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        super::cluster_world_callback_light::derive_symbolic_light(self.server, plan, values)
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
    fn environmental_contact(
        &self,
        intent: &pumpkin_cluster::combat_action::EnvironmentalDamageIntent,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<pumpkin_cluster::combat_action::EnvironmentalContactFacts, ApplyFailure> {
        super::cluster_living_environment::contact(self, intent, values)
    }
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
    fn validate_live_player_lease(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> bool {
        self.inventory.historical_player_lease_matches(lease)
            || self.actors.remote_lease(lease.gid) == Some(lease)
    }

    fn entity_equipment_compatible(
        &self,
        kind: u16,
        slots: &[(
            pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot,
            pumpkin_cluster::inventory::InventoryCell,
        )],
    ) -> Result<(), ApplyFailure> {
        super::cluster_world_inventory_actions::entity_equipment_compatible(kind, slots)
    }

    fn validate_block_inventory_layout(
        &self,
        block: Target,
        state: u16,
        block_entity: &Cell,
        bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding],
    ) -> Result<(), ApplyFailure> {
        super::cluster_inventory_block_roots::validate_layout(block, state, block_entity, bindings)
    }
    fn entity_inventory_storage_capacity(
        &self,
        kind: u16,
        facets: &[(
            pumpkin_cluster::mob_species_property::MobSpeciesProperty,
            pumpkin_cluster::mob_species_property::MobSpeciesPropertyValue,
        )],
        storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        super::cluster_world_inventory_actions::entity_inventory_storage_capacity(
            kind,
            facets,
            storage_slots,
        )
    }

    fn entity_inventory_layout(
        &self,
        body: &pumpkin_cluster::entity_body::EntityBody,
    ) -> Result<Vec<InvLoc>, ApplyFailure> {
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
        let (address, loc) = match destination {
            Target::Player {
                lease: Some(lease),
                inventory: Some(InventoryId::Player { inv, slot }),
                ..
            } => (
                InventoryTarget::Player(lease.gid),
                pumpkin_cluster::inventory::InvLoc::new(inv, slot),
            ),
            Target::Player {
                lease: Some(lease),
                inventory:
                    Some(InventoryId::Virtual {
                        host,
                        kind,
                        session,
                        inv,
                        slot,
                    }),
                ..
            } if host == lease.gid.server && session == 0 => (
                InventoryTarget::LeasedVirtual {
                    host,
                    key: lease.key.0,
                    player: lease.gid,
                    incarnation: lease.incarnation,
                    session,
                    kind,
                },
                pumpkin_cluster::inventory::InvLoc::new(inv, slot),
            ),
            _ => self
                .inventory_target(None, destination)
                .ok_or(ApplyFailure::Inventory(destination))?,
        };
        super::cluster_world_inventory_actions::inventory_slot_limit(
            source,
            address,
            loc,
            destination,
        )
    }

    fn crossbow_items(
        &self,
        intent: &pumpkin_cluster::crossbow_items::CrossbowItems,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<BTreeMap<Target, pumpkin_cluster::inventory::InventoryCell>, ApplyFailure> {
        super::cluster_item_launch::crossbow_items(intent, id, tick, values)
    }
    fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
        super::cluster_vehicle_world::activator(state)
    }
    fn vehicle_drop_item(
        &self,
        body: &pumpkin_cluster::entity_body::EntityBody,
    ) -> Result<pumpkin_cluster::inventory::InventoryCell, ApplyFailure> {
        super::cluster_vehicle_birth::vehicle_drop_item(body)
    }
    fn dropped_item_body(
        &self,
        template: &pumpkin_cluster::entity_body::EntityBody,
        source: &pumpkin_cluster::inventory::InventoryCell,
    ) -> Result<pumpkin_cluster::entity_body::EntityBody, ApplyFailure> {
        super::cluster_item_birth::project_drop_template(template, source)
    }
    fn death_inventory_drop(
        &self,
        template: &pumpkin_cluster::entity_body::EntityBody,
        source: &pumpkin_cluster::inventory::InventoryCell,
    ) -> Result<Option<pumpkin_cluster::entity_body::EntityBody>, ApplyFailure> {
        super::cluster_item_birth::death_inventory_drop(template, source)
    }
    fn derive_cloud_potion(
        &self,
        intent: &pumpkin_cluster::area_cloud::AreaCloudIntent,
        victim: &pumpkin_cluster::item_contact::ContactObservation,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: pumpkin_cluster::time::TickStamp,
        values: &BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        pumpkin_cluster::area_cloud::CloudPotionOutcome,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        super::cluster_area_cloud::potion(self, intent, victim, id, tick, values)
    }
    fn derive_projectile_tick(
        &self,
        intent: &pumpkin_cluster::projectile_tick::ProjectileTickIntent,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: pumpkin_cluster::time::TickStamp,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    ) -> Result<Vec<pumpkin_cluster::canonical_action::Effect>, ApplyFailure> {
        super::cluster_projectile_tick::derive(self, intent, id, tick, values)
    }
    fn mending_repair(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
        experience: u32,
    ) -> Result<
        (
            pumpkin_cluster::canonical_inventory::ItemTransformOutput,
            u32,
        ),
        ApplyFailure,
    > {
        super::cluster_world_item_actions::derive_mending_repair(source, experience)
    }
    fn item_cooldown_group(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
    ) -> Result<String, ApplyFailure> {
        super::cluster_world_item_actions::item_cooldown_group(source)
    }
    fn derive_block_inventory_tick(
        &self,
        intent: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    ) -> Result<
        pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation,
        ApplyFailure,
    > {
        super::cluster_inventory_block_tick::derive(self, intent, values)
    }
    fn machine_tick_footprint(
        &self,
        identity: pumpkin_cluster::action_identity::MachineActionId,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    ) -> Result<pumpkin_cluster::action_command::ActionFootprint, ApplyFailure> {
        super::cluster_inventory_machine::footprint(self.server, self, identity, values)
    }
    fn derive_machine_tick(
        &self,
        identity: pumpkin_cluster::action_identity::MachineActionId,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    ) -> Result<Vec<pumpkin_cluster::canonical_action::AppliedWrite>, ApplyFailure> {
        super::cluster_inventory_machine::derive(self.server, self, identity, values)
    }
    fn menu_inventory_transform(
        &self,
        kind: pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind,
        selection: &pumpkin_cluster::canonical_menu::MenuSelection,
        inputs: &[pumpkin_cluster::inventory::InventoryCell],
    ) -> Result<pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure>
    {
        super::cluster_inventory_menu::derive_menu_inventory_transform(kind, selection, inputs)
    }
    fn item_transform(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
        operation: &pumpkin_cluster::canonical_inventory::ItemTransformOperation,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: TickStamp,
    ) -> Result<pumpkin_cluster::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        super::cluster_world_item_actions::derive_item_transform(source, operation, id, tick)
    }

    fn mob_breeding_advancements(
        &self,
        kind: u16,
        credit: &pumpkin_cluster::mob_breeding::MobBreedingCredit,
        state: &pumpkin_cluster::player_advancement::PlayerAdvancementState,
    ) -> Result<pumpkin_cluster::player_advancement::PlayerAdvancementState, ApplyFailure> {
        super::cluster_owned_mob_breeding::advancement_outcome(kind, credit, state)
    }
    fn derive_item_use(
        &self,
        intent: &pumpkin_cluster::item_use::ItemUseIntent,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        super::cluster_world_item_actions::derive_canonical_use(self, intent, id, tick, values)
    }

    fn available(&self, view: TruthView, target: Target) -> bool {
        if let Some(body) = pumpkin_cluster::derived_living::actor_body_target(target) {
            return self.available(view, body);
        }
        if target.inventory().is_some() {
            return self.inventory.admission_slot_known(target)
                || self
                    .inventory_target(Some(view), target)
                    .is_some_and(|(address, _)| {
                        self.inventory.phase_snapshot(view, address).is_some()
                    });
        }
        match target {
            Target::Chunk {
                key,
                part: ChunkPart::BlockSection(section_y),
                inventory: None,
            } => self.blocks.block_section(false, key, section_y).is_some(),
            Target::Chunk {
                key,
                part: ChunkPart::Whole,
                inventory: None,
            } => {
                self.blocks.contains(key)
                    && self
                        .blocks
                        .scheduled_queue(view == TruthView::Ground, key)
                        .is_some()
            }
            Target::Chunk {
                key,
                part: ChunkPart::Block(_),
                ..
            } => self.blocks.contains(key),
            Target::Chunk {
                key,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            } => self
                .blocks
                .occupancy_root(view == TruthView::Ground, key)
                .is_some(),
            Target::Chunk {
                key,
                part: ChunkPart::BlockEntity(_),
                inventory: None,
            } => self.blocks.contains(key),
            Target::Chunk {
                key,
                part: ChunkPart::LightSection(_),
                inventory: None,
            } => self.blocks.contains(key),
            Target::Chunk {
                key,
                part: ChunkPart::Entity(_),
                inventory: None,
            } => self.blocks.contains(key),
            Target::Chunk {
                key,
                part: ChunkPart::Player(actor),
                ..
            } => {
                self.blocks.contains(key)
                    && (self.inventory.player_lease_for_gid(actor).is_some()
                        || self.actors.remote_lease(actor).is_some()
                        || self.inventory.canonical_admission_body_known(actor, key))
            }
            _ => false,
        }
    }

    fn borrow(&self, view: TruthView, target: Target) -> Option<std::borrow::Cow<'_, Cell>> {
        match target {
            Target::Chunk {
                key,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            } => self
                .blocks
                .occupancy_root(view == TruthView::Ground, key)
                .map(std::borrow::Cow::Borrowed),
            Target::Chunk {
                key,
                part: ChunkPart::BlockSection(section_y),
                inventory: None,
            } => self
                .blocks
                .block_section(view == TruthView::Ground, key, section_y)
                .map(std::borrow::Cow::Borrowed),
            Target::Chunk {
                key,
                part: ChunkPart::LightSection(section_y),
                inventory: None,
            } => self
                .blocks
                .light_section(view == TruthView::Ground, key, section_y)
                .map(std::borrow::Cow::Borrowed),
            Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            } => self
                .blocks
                .entity_body(view == TruthView::Ground, key, id)
                .map(std::borrow::Cow::Borrowed),
            Target::Chunk {
                key,
                part: ChunkPart::BlockEntity(position),
                inventory: None,
            } => self
                .blocks
                .block_entity(view == TruthView::Ground, key, position)
                .map(std::borrow::Cow::Borrowed),
            Target::Chunk {
                key,
                part: ChunkPart::Whole,
                inventory: None,
            } => self
                .blocks
                .scheduled_queue(view == TruthView::Ground, key)
                .map(std::borrow::Cow::Borrowed),
            _ => self.read(view, target).map(std::borrow::Cow::Owned),
        }
    }

    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        if let Target::Chunk {
            key,
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        } = target
        {
            return self
                .blocks
                .occupancy_root(view == TruthView::Ground, key)
                .cloned();
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::BlockSection(section_y),
            inventory: None,
        } = target
        {
            return self
                .blocks
                .block_section(view == TruthView::Ground, key, section_y)
                .cloned();
        }
        if let Some(body_target) = pumpkin_cluster::derived_living::actor_body_target(target) {
            let body = self.read(view, body_target)?;
            return pumpkin_cluster::derived_living::actor_facet_cell(&body, target);
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::LightSection(section_y),
            inventory: None,
        } = target
        {
            return self
                .blocks
                .light_section(view == TruthView::Ground, key, section_y)
                .cloned();
        }
        if target.inventory().is_some() {
            let (address, loc) = self.inventory_target(Some(view), target)?;
            let snapshot = self.inventory.phase_snapshot(view, address)?;
            let cell = snapshot
                .cells
                .into_iter()
                .find(|cell| cell.loc == loc)
                .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc));
            return Some(Cell {
                revision: self
                    .versions
                    .get(&(view, target))
                    .copied()
                    .unwrap_or(snapshot.revision),
                value: Value::Inventory(cell),
            });
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::Whole,
            inventory: None,
        } = target
        {
            return self
                .blocks
                .scheduled_queue(view == TruthView::Ground, key)
                .cloned();
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::BlockEntity(position),
            inventory: None,
        } = target
        {
            return self
                .blocks
                .block_entity(view == TruthView::Ground, key, position)
                .cloned();
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::Entity(id),
            inventory: None,
        } = target
        {
            return self
                .blocks
                .entity_body(view == TruthView::Ground, key, id)
                .cloned();
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::Player(actor),
            inventory: None,
        } = target
        {
            let spatial = self
                .actors
                .phase_spatial(actor, view == TruthView::Ground)?;
            let chunk = spatial.chunk;
            if chunk != key {
                return None;
            }
            let living = self
                .living
                .phase_state(ActorId::Player(actor), view == TruthView::Ground)?;
            let avatar = living.avatar.as_ref()?;
            let position = avatar.position.map(f64::from_bits);
            if avatar.world != key.world
                || (position[0].floor() as i32).div_euclid(16) != key.x()
                || (position[2].floor() as i32).div_euclid(16) != key.z()
            {
                return None;
            }
            return Some(Cell {
                revision: self.versions.get(&(view, target)).copied().unwrap_or(0),
                value: Value::Living((living.clone()).into()),
            });
        }
        let Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            inventory: None,
        } = target
        else {
            return None;
        };
        let state = match view {
            TruthView::Local => self.blocks.block(key, position),
            TruthView::Ground => self.blocks.ground_block(key, position),
        }
        .ok()?;
        Some(Cell {
            revision: self.versions.get(&(view, target)).copied().unwrap_or(0),
            value: Value::Block(state.as_u16()),
        })
    }

    fn commit(
        &mut self,
        _context: ActionContext,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        let prepared = self.prepare_commit(view, writes, true)?;
        self.apply_prepared(prepared);
        Ok(())
    }
}

trait TargetInventory {
    fn inventory(self) -> Option<InventoryId>;
}

impl TargetInventory for Target {
    fn inventory(self) -> Option<InventoryId> {
        match self {
            Target::Chunk { inventory, .. } | Target::Player { inventory, .. } => inventory,
        }
    }
}
