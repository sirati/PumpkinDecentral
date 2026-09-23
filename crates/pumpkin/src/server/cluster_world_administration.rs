use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use pumpkin_cluster::completion::{ActionKey, SourcedTickBatch, TickFinish};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, TickBatch};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::{
    AdministrativeDelta, BlockDelta, WORLD_DELTA_MAX_EDITS, chunk_of_block,
};
use pumpkin_util::math::position::BlockPos as WorldBlockPos;
use pumpkin_util::math::vector2::Vector2;
use tokio::sync::mpsc;

use super::Server;
use super::cluster_world_delta::LocalAdministrativeIntent;

pub struct AdministrativeOwner {
    pub receiver: mpsc::Receiver<LocalAdministrativeIntent>,
    pending: Vec<LocalAdministrativeIntent>,
    origins: HashMap<ActionKey, Option<u64>>,
    sequence: u16,
}

impl AdministrativeOwner {
    pub fn attach(server: &Server) -> Self {
        Self {
            receiver: super::cluster_world_delta::attach_administrative_owner(server),
            pending: Vec::new(),
            origins: HashMap::new(),
            sequence: 0,
        }
    }

    pub fn prepare(
        &mut self,
        server: &Server,
        intent: LocalAdministrativeIntent,
    ) -> Option<(TickBatch, TickFinish)> {
        let world = world_by_id(server, intent.world)?;
        let mut grouped = BTreeMap::<ChunkKey, Vec<_>>::new();
        for edit in &intent.edits {
            grouped
                .entry(chunk_of_block(intent.world, edit.pos.x, edit.pos.z))
                .or_default()
                .push(*edit);
        }
        for chunk in grouped.keys() {
            let position = Vector2::new(chunk.x(), chunk.z());
            world.level.pin_cluster_chunk(position);
            if !world.level.is_chunk_loaded(&position) {
                world.level.prefetch_cluster_chunk(position);
                self.pending.push(intent);
                return None;
            }
        }
        let local = ServerId(server.advanced_config.cluster.server_id);
        let tick = TickStamp::now();
        let mut batch = TickBatch::new(tick);
        for (chunk, edits) in grouped {
            for page in edits.chunks(WORLD_DELTA_MAX_EDITS) {
                let edits = page
                    .iter()
                    .map(|edit| {
                        let old_state = world
                            .level
                            .cluster_get_ground_block_state(&world_pos(edit.pos))
                            .as_u16();
                        BlockDelta {
                            pos: edit.pos,
                            old_state,
                            new_state: edit.new_state,
                        }
                    })
                    .collect();
                let update = AdministrativeDelta {
                    holder: local,
                    sequence: self.sequence,
                    chunk,
                    tick,
                    edits,
                };
                self.sequence = self.sequence.wrapping_add(1);
                self.origins
                    .insert(ActionKey::administrative(&update), intent.origin);
                batch.administrative.push(update);
            }
        }
        let finish = TickFinish::try_from_batch(&batch).ok()?;
        Some((batch, finish))
    }

    pub fn retry_pending(&mut self, server: &Server) -> Vec<(TickBatch, TickFinish)> {
        let pending = core::mem::take(&mut self.pending);
        pending
            .into_iter()
            .filter_map(|intent| self.prepare(server, intent))
            .collect()
    }

    pub fn apply_promoted(
        &mut self,
        server: &Server,
        tick: TickStamp,
        batches: &[SourcedTickBatch],
    ) {
        for update in batches.iter().flat_map(|batch| &batch.batch.administrative) {
            let action = ActionKey::administrative(update);
            let Some(world) = world_by_id(server, update.chunk.world) else {
                continue;
            };
            let accepted = update.edits.iter().all(|edit| {
                world
                    .level
                    .cluster_get_ground_block_state(&world_pos(edit.pos))
                    .as_u16()
                    == edit.old_state
            });
            if accepted {
                let winners: Vec<_> = update
                    .edits
                    .iter()
                    .map(|edit| (edit.pos, edit.new_state))
                    .collect();
                let promoted = winners
                    .iter()
                    .filter_map(|(position, state)| {
                        let state = pumpkin_data::BlockStateId::new(*state)?;
                        let (_, relative) =
                            world_pos(*position).chunk_and_chunk_relative_position();
                        Some(pumpkin_world::level::ClusterBlockEdit {
                            x: relative.x as usize,
                            y: relative.y,
                            z: relative.z as usize,
                            state,
                        })
                    })
                    .collect::<Vec<_>>();
                world.level.cluster_promote_tick(
                    &Vector2::new(update.chunk.x(), update.chunk.z()),
                    &promoted,
                    &[],
                );
                super::cluster_ground::publish_ground_blocks(
                    server,
                    update.chunk,
                    winners.iter().copied(),
                );
            }
            let ground = update
                .edits
                .iter()
                .map(|edit| BlockDelta {
                    pos: edit.pos,
                    old_state: world
                        .level
                        .cluster_get_ground_block_state(&world_pos(edit.pos))
                        .as_u16(),
                    new_state: world
                        .level
                        .cluster_get_ground_block_state(&world_pos(edit.pos))
                        .as_u16(),
                })
                .collect();
            super::cluster_world_delta::publish_administrative(
                server,
                super::cluster_world_delta::AdministrativePublication {
                    origin: self.origins.remove(&action).flatten(),
                    tick,
                    action,
                    chunk: update.chunk,
                    accepted,
                    ground,
                },
            );
        }
    }
}

fn world_by_id(
    server: &Server,
    id: pumpkin_cluster::protocol::WorldId,
) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == id)
        .cloned()
}

fn world_pos(position: BlockPos) -> WorldBlockPos {
    WorldBlockPos::new(position.x, position.y, position.z)
}
