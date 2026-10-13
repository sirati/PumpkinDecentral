use super::cluster_chunk_state::OwnedChunkStore;
use super::cluster_world_actor::{OwnedActors, OwnedLivingActors};
use super::cluster_world_inventory::InventoryOwner;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{LivePlayerLease, Target, TruthView};
use pumpkin_cluster::inventory::InventoryTarget;
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct SourceCategory<T> {
    pub chunks: BTreeSet<ChunkKey>,
    pub players: Vec<LivePlayerLease>,
    pub inventory_players: Vec<LivePlayerLease>,
    pub entities: Vec<GlobalEntityId>,
    pub roots: BTreeSet<InventoryTarget>,
    pub updates: Vec<T>,
}

pub(super) struct SourceBundle<T> {
    pub chunks: BTreeSet<ChunkKey>,
    pub blocks: OwnedChunkStore,
    pub actors: OwnedActors,
    pub living: OwnedLivingActors,
    pub inventory: InventoryOwner,
    pub versions: BTreeMap<(TruthView, Target), u64>,
    pub updates: Vec<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceExtractionError {
    Unavailable,
    InvalidActor,
    OverlappingCategory,
}

pub(super) fn close_category<T>(
    mut chunks: BTreeSet<ChunkKey>,
    mut roots: BTreeSet<InventoryTarget>,
    updates: Vec<T>,
    blocks: &OwnedChunkStore,
    actors: &OwnedActors,
    inventory: &InventoryOwner,
) -> Result<SourceCategory<T>, SourceExtractionError> {
    let mut players = BTreeSet::new();
    let mut entities = BTreeSet::new();
    loop {
        let before = (chunks.len(), roots.len(), players.len(), entities.len());
        let (needed, leases, ids, inventories) = inventory
            .unit_dependencies(&chunks, &roots)
            .map_err(|_| SourceExtractionError::Unavailable)?;
        chunks.extend(needed);
        roots.extend(inventories);
        entities.extend(ids);
        entities.extend(blocks.chunk_unit_entity_ids(&chunks));
        for &chunk in &chunks {
            for gid in actors.players_for_chunk(chunk) {
                players.insert(
                    actors
                        .lease_for_player(gid)
                        .ok_or(SourceExtractionError::InvalidActor)?,
                );
            }
        }
        for lease in leases {
            if actors.lease_for_player(lease.gid) == Some(lease) {
                players.insert(lease);
            }
        }
        for lease in &players {
            chunks.extend(actors.chunks_for_player(lease.gid));
        }
        for &id in &entities {
            for ground in [false, true] {
                if let Some(chunk) = blocks.entity_body_chunk(ground, id) {
                    chunks.insert(chunk);
                }
            }
        }
        if before == (chunks.len(), roots.len(), players.len(), entities.len()) {
            break;
        }
    }
    let (_, inventory_players, _, _) = inventory
        .unit_dependencies(&chunks, &roots)
        .map_err(|_| SourceExtractionError::Unavailable)?;
    Ok(SourceCategory {
        chunks,
        players: players.into_iter().collect(),
        inventory_players: inventory_players.into_iter().collect(),
        entities: entities.into_iter().collect(),
        roots,
        updates,
    })
}

pub(super) fn take<T>(
    categories: Vec<SourceCategory<T>>,
    blocks: &mut OwnedChunkStore,
    actors: &mut OwnedActors,
    living: &mut OwnedLivingActors,
    inventory: &mut InventoryOwner,
    versions: &mut BTreeMap<(TruthView, Target), u64>,
) -> Result<Vec<SourceBundle<T>>, (SourceExtractionError, Vec<SourceCategory<T>>)> {
    let mut seen = BTreeSet::new();
    let mut player_ids = BTreeSet::new();
    let mut player_keys = BTreeSet::new();
    let mut entity_ids = BTreeSet::new();
    let mut roots = BTreeSet::new();
    for category in &categories {
        if category.chunks.iter().any(|chunk| !seen.insert(*chunk))
            || category
                .inventory_players
                .iter()
                .map(|lease| lease.gid)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .any(|gid| !player_ids.insert(gid))
            || category
                .inventory_players
                .iter()
                .map(|lease| lease.key)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .any(|key| !player_keys.insert(key))
            || category.entities.iter().any(|id| !entity_ids.insert(*id))
            || category.roots.iter().any(|root| !roots.insert(*root))
        {
            return Err((SourceExtractionError::OverlappingCategory, categories));
        }
        let ids = category
            .players
            .iter()
            .map(|lease| ActorId::Player(lease.gid))
            .chain(category.entities.iter().copied().map(ActorId::Entity))
            .collect::<Vec<_>>();
        if blocks
            .preflight_chunk_unit(&category.chunks, &category.entities)
            .is_err()
            || actors.preflight_players(&category.players).is_err()
            || living.preflight_actors(&ids).is_err()
            || inventory
                .preflight_chunk_unit(
                    &category.chunks,
                    &category.inventory_players,
                    &category.entities,
                    &category.roots,
                )
                .is_err()
        {
            return Err((SourceExtractionError::Unavailable, categories));
        }
    }
    Ok(categories
        .into_iter()
        .map(|category| {
            let ids = category
                .players
                .iter()
                .map(|lease| ActorId::Player(lease.gid))
                .chain(category.entities.iter().copied().map(ActorId::Entity))
                .collect::<Vec<_>>();
            SourceBundle {
                blocks: blocks
                    .take_chunk_unit(&category.chunks, &category.entities)
                    .ok()
                    .expect("preflighted chunk source unit"),
                actors: actors
                    .take_players(&category.players)
                    .ok()
                    .expect("preflighted player source unit"),
                living: living
                    .take_actors(&ids)
                    .ok()
                    .expect("preflighted living source unit"),
                inventory: inventory
                    .take_chunk_unit(
                        &category.chunks,
                        &category.inventory_players,
                        &category.entities,
                        &category.roots,
                    )
                    .ok()
                    .expect("preflighted inventory source unit"),
                versions: super::cluster_world_canonical::take_chunk_versions(
                    versions,
                    &category.chunks,
                    &category.inventory_players,
                ),
                chunks: category.chunks,
                updates: category.updates,
            }
        })
        .collect())
}

pub(super) fn return_units<T>(
    units: Vec<SourceBundle<T>>,
    blocks: &mut OwnedChunkStore,
    actors: &mut OwnedActors,
    living: &mut OwnedLivingActors,
    inventory: &mut InventoryOwner,
    versions: &mut BTreeMap<(TruthView, Target), u64>,
) -> Result<Vec<Vec<T>>, Vec<SourceBundle<T>>> {
    let mut chunks = BTreeSet::new();
    let mut revision_keys = BTreeSet::new();
    let mut player_ids = BTreeSet::new();
    let mut player_keys = BTreeSet::new();
    let mut actor_ids = BTreeSet::new();
    let mut inventory_keys = BTreeSet::new();
    for unit in &units {
        if unit.chunks.iter().any(|key| !chunks.insert(*key))
            || unit
                .actors
                .returned_player_ids()
                .into_iter()
                .any(|id| !player_ids.insert(id))
            || unit
                .actors
                .returned_player_keys()
                .any(|key| !player_keys.insert(key))
            || unit
                .living
                .returned_actor_ids()
                .into_iter()
                .any(|id| !actor_ids.insert(id))
            || !blocks.can_return_chunk_unit(&unit.blocks)
            || !actors.can_return_players(&unit.actors)
            || !living.can_return_actors(&unit.living)
            || !unit.inventory.claim_unit_return_keys(&mut inventory_keys)
            || !inventory.can_return_chunk_unit(&unit.inventory)
            || unit
                .versions
                .keys()
                .any(|key| versions.contains_key(key) || !revision_keys.insert(*key))
        {
            return Err(units);
        }
    }
    Ok(units
        .into_iter()
        .map(|unit| {
            assert!(blocks.return_chunk_unit(unit.blocks).is_ok());
            assert!(actors.return_players(unit.actors).is_ok());
            assert!(living.return_actors(unit.living).is_ok());
            assert!(inventory.return_chunk_unit(unit.inventory).is_ok());
            assert!(
                super::cluster_world_canonical::return_chunk_versions(versions, unit.versions)
                    .is_ok()
            );
            unit.updates
        })
        .collect())
}

pub(super) fn categorize<T>(
    mut work: Vec<pumpkin_cluster::chunk_work::ChunkWork<T>>,
    blocks: &OwnedChunkStore,
    actors: &OwnedActors,
    inventory: &InventoryOwner,
) -> Result<Vec<SourceCategory<T>>, SourceExtractionError> {
    loop {
        let categories = work
            .into_iter()
            .map(|work| {
                close_category(
                    work.chunks.into_iter().collect(),
                    BTreeSet::new(),
                    work.updates,
                    blocks,
                    actors,
                    inventory,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        enum Ownership {
            Chunk(ChunkKey),
            Player(pumpkin_cluster::identity::GlobalPlayerId),
            PlayerKey(pumpkin_cluster::canonical_action::PlayerKey),
            Entity(GlobalEntityId),
            Inventory(InventoryTarget),
        }
        let mut owners = BTreeMap::new();
        let mut parents = (0..categories.len()).collect::<Vec<_>>();
        fn root(parents: &mut [usize], mut index: usize) -> usize {
            while parents[index] != index {
                parents[index] = parents[parents[index]];
                index = parents[index];
            }
            index
        }
        let mut overlap = false;
        for (index, category) in categories.iter().enumerate() {
            let domains = category
                .chunks
                .iter()
                .copied()
                .map(Ownership::Chunk)
                .chain(
                    category
                        .inventory_players
                        .iter()
                        .map(|lease| Ownership::Player(lease.gid)),
                )
                .chain(
                    category
                        .inventory_players
                        .iter()
                        .map(|lease| Ownership::PlayerKey(lease.key)),
                )
                .chain(category.entities.iter().copied().map(Ownership::Entity))
                .chain(category.roots.iter().copied().map(Ownership::Inventory));
            for domain in domains {
                if let Some(previous) = owners.insert(domain, index) {
                    let left = root(&mut parents, previous);
                    let right = root(&mut parents, index);
                    if left != right {
                        parents[right] = left;
                        overlap = true;
                    }
                }
            }
        }
        if !overlap {
            return Ok(categories);
        }
        let mut merged = BTreeMap::<usize, pumpkin_cluster::chunk_work::ChunkWork<T>>::new();
        for (index, category) in categories.into_iter().enumerate() {
            let joint = merged.entry(root(&mut parents, index)).or_insert_with(|| {
                pumpkin_cluster::chunk_work::ChunkWork {
                    chunks: Vec::new(),
                    updates: Vec::new(),
                }
            });
            joint.chunks.extend(category.chunks);
            joint.updates.extend(category.updates);
        }
        work = merged.into_values().collect();
    }
}

pub(super) fn run<T: Send, R: Send>(
    units: Vec<SourceBundle<T>>,
    worker: impl Fn(&mut SourceBundle<T>) -> R + Send + Sync,
) -> Vec<(SourceBundle<T>, R)> {
    use rayon::prelude::*;
    units
        .into_par_iter()
        .map(|mut unit| {
            let result = worker(&mut unit);
            (unit, result)
        })
        .collect()
}
