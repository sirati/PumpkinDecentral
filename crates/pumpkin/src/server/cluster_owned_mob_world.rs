use std::borrow::Cow;
use std::collections::BTreeSet;

use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{
    ActionState, ApplyFailure, ChunkPart, Target, TruthView, Value,
};
use pumpkin_cluster::entity_body::EntityBody;
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, GlobalEntityId, WorldId};
use pumpkin_util::math::{
    boundingbox::BoundingBox, position::BlockPos as LocalBlockPos, vector3::Vector3,
};

pub(crate) struct MobWorldView<'a, S> {
    owners: &'a S,
    draft: &'a ActorActionDraft,
    world: WorldId,
    loaded: &'a BTreeSet<ChunkKey>,
    rules: Option<MobWorldRules>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MobWorldRules {
    pub mob_griefing: bool,
    pub difficulty: u8,
    pub minimum_y: i32,
    pub maximum_y: i32,
    pub sea_level: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MobReadFailure {
    Unloaded(ChunkKey),
    Missing(Target),
    Type(Target),
    InvalidBounds,
    Budget,
}

impl<'a, S: ActionState> MobWorldView<'a, S> {
    pub(crate) fn new(
        owners: &'a S,
        draft: &'a ActorActionDraft,
        world: WorldId,
        loaded: &'a BTreeSet<ChunkKey>,
    ) -> Self {
        Self {
            owners,
            draft,
            world,
            loaded,
            rules: None,
        }
    }

    pub(crate) fn with_rules(mut self, rules: MobWorldRules) -> Self {
        self.rules = Some(rules);
        self
    }

    pub(crate) fn block(
        &self,
        position: BlockPos,
    ) -> Result<pumpkin_data::BlockStateId, MobReadFailure> {
        if self
            .rules
            .is_some_and(|rules| position.y < rules.minimum_y || position.y > rules.maximum_y)
        {
            return Ok(pumpkin_data::Block::AIR.default_state.id);
        }
        let key = ChunkKey::new(
            self.world,
            position.x.div_euclid(16),
            position.z.div_euclid(16),
        );
        if !self.loaded.contains(&key) {
            return Err(MobReadFailure::Unloaded(key));
        }
        let target = Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            inventory: None,
        };
        match self
            .draft
            .view(self.owners)
            .read(TruthView::Local, target)
            .map(|cell| cell.value)
        {
            Some(Value::Block(state)) => {
                pumpkin_data::BlockStateId::new(state).ok_or(MobReadFailure::Type(target))
            }
            Some(_) => Err(MobReadFailure::Type(target)),
            None => Err(MobReadFailure::Missing(target)),
        }
    }

    pub(crate) fn entity(
        &self,
        chunk: ChunkKey,
        id: GlobalEntityId,
    ) -> Result<Option<Cow<'_, EntityBody>>, MobReadFailure> {
        if chunk.world != self.world || !self.loaded.contains(&chunk) {
            return Err(MobReadFailure::Unloaded(chunk));
        }
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(id),
            inventory: None,
        };
        match self.draft.borrow_local(self.owners, target) {
            Some(Cow::Borrowed(pumpkin_cluster::canonical_action::Cell {
                value: Value::Entity(body),
                ..
            })) => Ok(Some(Cow::Borrowed(body.as_ref()))),
            Some(Cow::Owned(pumpkin_cluster::canonical_action::Cell {
                value: Value::Entity(body),
                ..
            })) => Ok(Some(Cow::Owned(*body))),
            Some(_) => Err(MobReadFailure::Type(target)),
            None => Ok(None),
        }
    }

    pub(crate) fn move_bounds(
        &self,
        bounds: BoundingBox,
        velocity: Vector3<f64>,
        budget: usize,
    ) -> Result<pumpkin_util::math::collision::MovementCollision, MobReadFailure> {
        let collisions = pumpkin_world::collision::collect_block_collisions(
            bounds.stretch(velocity),
            budget,
            |at| {
                self.block(BlockPos {
                    x: at.0.x,
                    y: at.0.y,
                    z: at.0.z,
                })
                .map(|id| id.to_state())
            },
            |_, _| None,
        )
        .map_err(|error| match error {
            pumpkin_world::collision::CollisionReadFailure::Read { reason, .. } => reason,
            pumpkin_world::collision::CollisionReadFailure::InvalidBounds => {
                MobReadFailure::InvalidBounds
            }
            pumpkin_world::collision::CollisionReadFailure::Budget => MobReadFailure::Budget,
        })?;
        Ok(pumpkin_util::math::collision::resolve_block_movement(
            bounds,
            velocity,
            &collisions.shapes,
            &collisions.positions,
        ))
    }

    pub(crate) fn visible_between(
        &self,
        from: [f64; 3],
        to: [f64; 3],
        budget: usize,
    ) -> Result<bool, MobReadFailure> {
        if from
            .into_iter()
            .chain(to)
            .any(|coordinate| !coordinate.is_finite() || coordinate.abs() > 30_000_000.0)
        {
            return Err(MobReadFailure::InvalidBounds);
        }
        let positions = pumpkin_cluster::explosion_exposure::block_ray_positions(from, to)
            .map_err(|_| MobReadFailure::Budget)?;
        if positions.len() > budget { return Err(MobReadFailure::Budget); }
        for position in positions {
            let at = LocalBlockPos::new(position.x, position.y, position.z);
            let state = self.block(position)?.to_state();
            for shape in state.get_block_collision_shapes_at(&at) {
                let shape = shape.at_pos(at);
                let low = [shape.min.x, shape.min.y, shape.min.z];
                let high = [shape.max.x, shape.max.y, shape.max.z];
                if segment_intersects(from, to, low, high) { return Ok(false); }
            }
        }
        Ok(true)
    }

    pub(crate) fn fluid_contacts(
        &self,
        bounds: BoundingBox,
        budget: usize,
    ) -> Result<pumpkin_world::fluid_snapshot::EntityFluidContacts, MobReadFailure> {
        pumpkin_world::fluid_snapshot::sample_entity_fluid_contacts(
            bounds,
            budget,
            true,
            |at: LocalBlockPos| {
                self.block(BlockPos {
                    x: at.0.x,
                    y: at.0.y,
                    z: at.0.z,
                })
            },
        )
        .map_err(|error| match error {
            pumpkin_world::fluid_snapshot::FluidReadFailure::Read { reason, .. } => reason,
            pumpkin_world::fluid_snapshot::FluidReadFailure::InvalidPosition => {
                MobReadFailure::InvalidBounds
            }
            pumpkin_world::fluid_snapshot::FluidReadFailure::Budget => MobReadFailure::Budget,
        })
    }
}

fn segment_intersects(from: [f64; 3], to: [f64; 3], low: [f64; 3], high: [f64; 3]) -> bool {
    let mut first = 0.0_f64;
    let mut last = 1.0_f64;
    for axis in 0..3 {
        let direction = to[axis] - from[axis];
        if direction.abs() <= f64::EPSILON {
            if from[axis] <= low[axis] || from[axis] >= high[axis] {
                return false;
            }
        } else {
            let a = (low[axis] - from[axis]) / direction;
            let b = (high[axis] - from[axis]) / direction;
            first = first.max(a.min(b));
            last = last.min(a.max(b));
            if first >= last {
                return false;
            }
        }
    }
    last > 0.0 && first < 1.0
}

impl From<MobReadFailure> for ApplyFailure {
    fn from(value: MobReadFailure) -> Self {
        match value {
            MobReadFailure::Missing(target) => Self::Condition(target),
            MobReadFailure::Type(target) => Self::Type(target),
            MobReadFailure::Unloaded(_)
            | MobReadFailure::InvalidBounds
            | MobReadFailure::Budget => Self::Capacity,
        }
    }
}
