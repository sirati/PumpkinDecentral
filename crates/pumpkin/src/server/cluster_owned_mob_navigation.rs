use std::collections::BTreeMap;
use std::sync::Arc;

use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
use pumpkin_data::BlockStateId;

use super::cluster_chunk_state::{BlockTransactionError, OwnedChunkBlocks, OwnedChunkStore};

#[derive(Clone)]
pub(crate) struct MobNavigationSnapshot {
    world: WorldId,
    chunks: Arc<BTreeMap<ChunkKey, Arc<OwnedChunkBlocks>>>,
    blocks: BTreeMap<BlockPos, BlockStateId>,
    pub(crate) min_y: i32,
    pub(crate) sea_level: i32,
}

impl MobNavigationSnapshot {
    pub(crate) fn capture(
        source: &OwnedChunkStore,
        world: WorldId,
        required: &std::collections::BTreeSet<ChunkKey>,
        min_y: i32,
        sea_level: i32,
    ) -> Self {
        let chunks = required.iter().copied()
            .filter(|key| key.world == world)
            .filter_map(|key| source.snapshot(key).map(|snapshot| (key, snapshot)))
            .collect();
        Self {
            world,
            chunks: Arc::new(chunks),
            blocks: BTreeMap::new(),
            min_y,
            sea_level,
        }
    }

    pub(crate) fn with_draft(&self, draft: &ActorActionDraft) -> Result<Self, Target> {
        let mut next = self.clone();
        for (target, (cell, _)) in draft.frozen_overlay().iter() {
            let Target::Chunk {
                key,
                part: ChunkPart::Block(position),
                inventory: None,
            } = target
            else {
                continue;
            };
            if key.world != self.world {
                continue;
            }
            match cell.as_ref().map(|cell| &cell.value) {
                Some(Value::Block(state)) => {
                    let Some(state) = BlockStateId::new(*state) else {
                        return Err(*target);
                    };
                    next.blocks.insert(*position, state);
                }
                _ => return Err(*target),
            }
        }
        Ok(next)
    }

    pub(crate) fn block(&self, position: BlockPos) -> Result<BlockStateId, ChunkKey> {
        if let Some(&state) = self.blocks.get(&position) {
            return Ok(state);
        }
        let key = ChunkKey::new(
            self.world,
            position.x.div_euclid(16),
            position.z.div_euclid(16),
        );
        match self.chunks.get(&key).ok_or(key)?.block(key, position) {
            Ok(state) => Ok(state),
            Err(BlockTransactionError::OutsideHeight) => {
                Ok(pumpkin_data::Block::AIR.default_state.id)
            }
            Err(_) => Err(key),
        }
    }

    fn require_radius(&self, position: BlockPos, radius: i32) -> Result<(), ChunkKey> {
        for x in position.x.saturating_sub(radius).div_euclid(16)
            ..=position.x.saturating_add(radius).div_euclid(16)
        {
            for z in position.z.saturating_sub(radius).div_euclid(16)
                ..=position.z.saturating_add(radius).div_euclid(16)
            {
                let key = ChunkKey::new(self.world, x, z);
                if !self.chunks.contains_key(&key) {
                    return Err(key);
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct MobPathResult {
    pub nodes: Vec<pumpkin_cluster::mob_continuation::MobPathNode>,
    pub target: BlockPos,
    pub reached: bool,
    pub distance: u32,
}

pub(crate) fn find_path(
    snapshot: MobNavigationSnapshot,
    registry: Arc<crate::block::registry::BlockRegistry>,
    draft: &ActorActionDraft,
    state: &pumpkin_cluster::mob_continuation::MobNavigation,
    data: crate::entity::ai::pathfinder::node_evaluator::MobData,
    destination: BlockPos,
    visit_budget: usize,
    reach: i32,
    maximum_distance: f32,
) -> Result<Option<MobPathResult>, super::cluster_owned_mob_world::MobReadFailure> {
    use super::cluster_owned_mob_world::MobReadFailure;
    use crate::entity::ai::pathfinder::{EvaluatorKind, PathFinder, node_evaluator::NodeEvaluator};
    use pumpkin_cluster::mob_continuation::NavigationKind;
    let mut data = data;
    for &(kind, value) in &state.path_type_malus {
        let Some(slot) = data.path_type_malus.get_mut(usize::from(kind)) else {
            return Err(MobReadFailure::Budget);
        };
        let value = f32::from_bits(value);
        if !value.is_finite() {
            return Err(MobReadFailure::Budget);
        }
        *slot = Some(value);
    }
    if visit_budget == 0
        || visit_budget > 4096
        || !maximum_distance.is_finite()
        || !(0.0..=256.0).contains(&maximum_distance)
        || !data.position.x.is_finite()
        || !data.position.y.is_finite()
        || !data.position.z.is_finite()
        || reach < 0
    {
        return Err(MobReadFailure::Budget);
    }
    let snapshot = snapshot.with_draft(draft).map_err(MobReadFailure::Type)?;
    snapshot
        .require_radius(
            BlockPos {
                x: data.position.x.floor() as i32,
                y: data.position.y.floor() as i32,
                z: data.position.z.floor() as i32,
            },
            maximum_distance.ceil() as i32 + 16,
        )
        .map_err(MobReadFailure::Unloaded)?;
    snapshot
        .require_radius(destination, 16)
        .map_err(MobReadFailure::Unloaded)?;
    let context =
        crate::entity::ai::pathfinder::pathfinding_context::PathfindingContext::from_owned(
            pumpkin_util::math::vector3::Vector3::new(
                data.position.x.floor() as i32,
                data.position.y.floor() as i32,
                data.position.z.floor() as i32,
            ),
            snapshot,
            registry,
        );
    let mut evaluator = match state.kind {
        NavigationKind::Walk => EvaluatorKind::Walk(
            crate::entity::ai::pathfinder::walk_node_evaluator::WalkNodeEvaluator::new(),
        ),
        NavigationKind::Fly => EvaluatorKind::Fly(
            crate::entity::ai::pathfinder::fly_node_evaluator::FlyNodeEvaluator::new(),
        ),
        NavigationKind::Swim => EvaluatorKind::Swim(
            crate::entity::ai::pathfinder::swim_node_evaluator::SwimNodeEvaluator::new(false),
        ),
        NavigationKind::Amphibious => EvaluatorKind::Amphibious(
            crate::entity::ai::pathfinder::amphibious_node_evaluator::AmphibiousNodeEvaluator::new(
                false,
            ),
        ),
    };
    let configure = |evaluator: &mut dyn NodeEvaluator| {
        evaluator.set_can_float(state.can_float);
        evaluator.set_can_open_doors(state.can_open_doors);
        evaluator.set_can_pass_doors(state.can_pass_doors);
        evaluator.set_can_walk_over_fences(state.can_walk_over_fences);
    };
    match &mut evaluator {
        EvaluatorKind::Walk(value) => configure(value),
        EvaluatorKind::Fly(value) => configure(value),
        EvaluatorKind::Swim(value) => configure(value),
        EvaluatorKind::Amphibious(value) => configure(value),
    }
    evaluator.prepare(context, data);
    let path = PathFinder::new(visit_budget).find_path_single(
        &mut evaluator,
        pumpkin_util::math::position::BlockPos::new(destination.x, destination.y, destination.z),
        maximum_distance,
        reach,
        1.0,
    );
    evaluator.done();
    Ok(path.map(|path| {
        let target = path.get_target();
        MobPathResult {
            target: BlockPos {
                x: target.0.x,
                y: target.0.y,
                z: target.0.z,
            },
            reached: path.can_reach(),
            distance: path.get_dist_to_target().to_bits(),
            nodes: (0..path.get_node_count())
                .filter_map(|index| path.get_node(index))
                .map(|node| pumpkin_cluster::mob_continuation::MobPathNode {
                    position: BlockPos {
                        x: node.pos.0.x,
                        y: node.pos.0.y,
                        z: node.pos.0.z,
                    },
                    hash: node.hash,
                    heap_index: node.heap_idx,
                    costs: [node.g, node.h, node.f, node.walked_dist, node.cost_malus]
                        .map(f32::to_bits),
                    predecessor: node.came_from.map(|position| BlockPos {
                        x: position.x,
                        y: position.y,
                        z: position.z,
                    }),
                    closed: node.closed,
                    path_type: node.path_type as u8,
                })
                .collect(),
        }
    }))
}
