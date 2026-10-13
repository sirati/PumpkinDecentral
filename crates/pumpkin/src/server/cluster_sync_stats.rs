use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pumpkin_cluster::action_graph::{ActionGraphs, action_realm_held_by_chunk};
use pumpkin_cluster::canonical_action::Stakeholder;
use pumpkin_cluster::protocol::{ChunkKey, WorldId};
use pumpkin_cluster::time::TickStamp;

use super::Server;

#[derive(Clone, Default)]
pub struct ChunkSyncStats {
    pub pending_actions: usize,
    pub ground_tick: Option<TickStamp>,
    pub tracked: bool,
}

#[derive(Clone, Default)]
pub struct SyncStatsSnapshot {
    pub published_tick: TickStamp,
    pub chunks: BTreeMap<ChunkKey, ChunkSyncStats>,
    pub pending_actions: usize,
    pub pending_by_world: BTreeMap<WorldId, usize>,
}

pub fn publish(server: &Server, graphs: &ActionGraphs, now: TickStamp) {
    let mut snapshot = SyncStatsSnapshot {
        published_tick: now,
        ..Default::default()
    };
    for chunk in server.cluster_owned_chunk_blocks.load().keys() {
        snapshot.chunks.insert(
            *chunk,
            ChunkSyncStats {
                tracked: true,
                ground_tick: graphs
                    .chunk_frontiers
                    .get(chunk)
                    .map(|frontier| frontier.settled_through),
                pending_actions: 0,
            },
        );
    }
    for graph in graphs.ticks.values() {
        for node in graph.actions.values() {
            let pending_realms = node
                .realms
                .difference(&node.applied_realms)
                .copied()
                .collect::<Vec<_>>();
            if pending_realms.is_empty() {
                continue;
            }
            snapshot.pending_actions += 1;
            let action = node.semantic_action();
            let mut chunks = action
                .stakeholders
                .iter()
                .filter_map(|stakeholder| match stakeholder {
                    Stakeholder::Chunk(chunk) => Some(*chunk),
                    Stakeholder::Player(_) => None,
                })
                .collect::<BTreeSet<_>>();
            chunks.extend(action.player_chunks.iter().map(|(_, chunk)| *chunk));
            let mut worlds = BTreeSet::new();
            for chunk in chunks {
                if !pending_realms
                    .iter()
                    .any(|realm| action_realm_held_by_chunk(action, *realm, chunk))
                {
                    continue;
                }
                snapshot.chunks.entry(chunk).or_default().pending_actions += 1;
                worlds.insert(chunk.world);
            }
            for world in worlds {
                *snapshot.pending_by_world.entry(world).or_default() += 1;
            }
        }
    }
    server.cluster_sync_stats.store(Arc::new(snapshot));
}
