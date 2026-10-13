use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::action_graph::SemanticWrapper;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent;
use pumpkin_cluster::canonical_action::{
    ActionRef, ChunkPart, Effect, LivePlayerLease, Target, Value,
};
use pumpkin_cluster::inventory::{INV_SELECTED, InvLoc};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::{Server, arena_world, registered_players};
use crate::server::cluster_test_observation::GraphObservation;
use crate::server::cluster_test_scenarios::FixtureLoadoutTemplate;

#[derive(Default)]
pub(super) struct AvatarObserver {
    input: Option<mpsc::Receiver<GraphObservation>>,
    prepared: Option<LivePlayerLease>,
    retired: Option<LivePlayerLease>,
    admitted: Option<(
        LivePlayerLease,
        ActionRef,
        Vec<pumpkin_cluster::canonical_action::Cell>,
    )>,
    preheld: bool,
    settled: bool,
    failed: bool,
}

impl AvatarObserver {
    pub(super) fn selected() -> Option<Self> {
        std::env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
            .unwrap_or_default()
            .split(',')
            .any(|name| name == "initial-avatar-inventory")
            .then(Self::default)
    }

    pub(super) fn advance(&mut self, server: &Server, local: pumpkin_cluster::identity::ServerId) {
        if self.settled || self.failed {
            self.input = None;
            return;
        }
        if self.input.is_none() {
            let Some(players) = registered_players(server) else {
                return;
            };
            let Some((_, entry)) = crate::server::cluster_presence::all_presence_entries(server)
                .into_iter()
                .find(|(gid, entry)| *gid == players[0] && !entry.in_lobby)
            else {
                return;
            };
            let key =
                pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::from_bytes(entry.uuid));
            let Some(cells) = FixtureLoadoutTemplate::InitialAvatar.desired_cells() else {
                self.failed = true;
                return;
            };
            let mut targets = cells
                .into_iter()
                .map(|cell| Target::Player {
                    key,
                    lease: None,
                    inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player {
                        inv: cell.loc.inv,
                        slot: cell.loc.slot,
                    }),
                })
                .collect::<BTreeSet<_>>();
            if let Some(spatial) = server.cluster_actor_spatial.load().get(&players[0]) {
                targets.insert(Target::Chunk {
                    key: spatial.truth.local.chunk,
                    part: ChunkPart::Player(players[0]),
                    inventory: None,
                });
            } else if let Some(world) = arena_world(server) {
                targets.insert(Target::Chunk {
                    key: pumpkin_cluster::world_delta::chunk_of_block(
                        crate::server::cluster_world_delta::world_id(&world),
                        0,
                        0,
                    ),
                    part: ChunkPart::Player(players[0]),
                    inventory: None,
                });
            }
            match server
                .cluster_test
                .observations
                .watches
                .watch(BTreeSet::from([ActorId::Player(players[0])]), targets)
            {
                Ok(input) => self.input = Some(input),
                Err(()) => {
                    self.failed = true;
                    tracing::error!("cluster test avatar watch resource failure");
                    return;
                }
            }
        }
        let desired = FixtureLoadoutTemplate::InitialAvatar
            .desired_cells()
            .unwrap()
            .into_iter()
            .map(|cell| (cell.loc, cell))
            .collect::<BTreeMap<_, _>>();
        loop {
            let observation = match self.input.as_mut().unwrap().try_recv() {
                Ok(value) => value,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.input = None;
                    if self.prepared.is_some() {
                        self.failed = true;
                        tracing::error!("cluster test armed avatar watch expired");
                    }
                    break;
                }
            };
            if !observation.ground_applied
                || observation.local_generation != Some(observation.generation)
                || matches!(observation.wrapper, SemanticWrapper::Undone)
            {
                continue;
            }
            if self.prepared.is_none() {
                if let Some(pumpkin_cluster::canonical_action::SemanticIntent::CreativeInventory(
                    intent,
                )) = &observation.action.semantic_intent
                {
                    if let Some(lease) =
                        observation
                            .action
                            .player_leases
                            .iter()
                            .copied()
                            .find(|lease| {
                                crate::server::cluster_inventory_scenario_source::exact_intent(
                                    intent, *lease, &desired,
                                )
                            })
                    {
                        self.prepared = Some(lease);
                    }
                }
                continue;
            }
            for effect in &observation.action.effects {
                let Effect::PlayerAvatarLifecycle { target, intent } = effect else {
                    continue;
                };
                match intent {
                    AvatarLifecycleIntent::Retirement { lease, .. }
                        if Some(*lease) == self.prepared =>
                    {
                        self.retired = Some(*lease);
                        if let Target::Chunk { key, .. } = target {
                            self.preheld =
                                server.cluster_owned_chunk_blocks.load().contains_key(key)
                                    || server
                                        .cluster_chunk_directory
                                        .load()
                                        .holders_of(key)
                                        .is_some_and(|holders| holders.contains(&local.0));
                        }
                    }
                    AvatarLifecycleIntent::Admission {
                        lease, inventory, ..
                    } if self.retired.is_some_and(|old| {
                        old.key == lease.key
                            && old.gid == lease.gid
                            && old.incarnation != lease.incarnation
                    }) =>
                    {
                        let valid=inventory.valid()&&inventory.cells.len()==desired.len()&&inventory.cells.iter().all(|cell|matches!(&cell.value,Value::Inventory(item) if desired.get(&item.loc)==Some(item)));
                        if !valid || !self.preheld || self.admitted.is_some() {
                            self.failed = true;
                            tracing::error!("cluster test avatar lifecycle inventory mismatch");
                            return;
                        }
                        self.admitted =
                            Some((*lease, observation.reference, inventory.cells.clone()));
                    }
                    _ => {}
                }
            }
            let Some((lease, admission, cells)) = &self.admitted else {
                continue;
            };
            if observation.reference == *admission {
                continue;
            }
            let Some(result) = &observation.factual_result else {
                continue;
            };
            for write in &result.writes {
                if write.target
                    != (Target::Player {
                        key: lease.key,
                        lease: Some(*lease),
                        inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player {
                            inv: INV_SELECTED,
                            slot: 0,
                        }),
                    })
                {
                    continue;
                }
                let valid = matches!((&write.before,&write.after),
                    (Some(before),Some(after)) if matches!((&before.value,&after.value),
                        (Value::Inventory(old),Value::Inventory(new)) if old.loc==InvLoc::new(INV_SELECTED,0)&&new.loc==old.loc&&old.item==1&&new.item==2&&new.count==1));
                if !valid {
                    self.failed = true;
                    tracing::error!("cluster test avatar first selected source mismatch");
                    return;
                }
                let Ok(bytes) =
                    postcard::to_allocvec(&(lease, admission, cells, observation.reference))
                else {
                    self.failed = true;
                    return;
                };
                let hash = Sha256::digest(bytes)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                tracing::info!(ground_hash=%hash,inventory_in_lifecycle=true,observer_chunk_preheld=self.preheld,initial_main_count=2u8,initial_boots="iron_boots",initial_offhand="shield",first_selected=1u8,peer_action_reply_evidence="static_schema",peer_action_reply_protocol_variants=0u8,"cluster e2e initial avatar inventory settled");
                self.settled = true;
                self.input = None;
                return;
            }
        }
    }
}
