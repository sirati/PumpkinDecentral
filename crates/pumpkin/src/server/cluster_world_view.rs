use std::collections::HashMap;

use pumpkin_cluster::controller::ViewIntent;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::protocol::{ChunkKey, WorldId};
use pumpkin_util::math::vector2::Vector2;
use pumpkin_world::chunk_system::ChunkLoading;
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ViewState {
    pub intent: ViewIntent,
    pub cylinder: Cylindrical,
    pub view_ticket: Option<i8>,
    pub simulation_ticket: Option<i8>,
}

pub struct ViewDelta {
    pub old: Option<ViewState>,
    pub new: ViewState,
    pub enter: Vec<ChunkKey>,
    pub leave: Vec<ChunkKey>,
    pub center_changed: bool,
    pub delivery_reset: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewError {
    InvalidRadius,
    StaleTick,
}

#[derive(Default)]
pub struct OwnedViews {
    views: HashMap<GlobalPlayerId, ViewState>,
}

impl OwnedViews {
    pub fn apply(&mut self, intent: ViewIntent) -> Result<Option<ViewDelta>, ViewError> {
        if intent.radius.get() > pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE {
            return Err(ViewError::InvalidRadius);
        }
        let old = self.views.get(&intent.actor).copied();
        if let Some(old) = old {
            if intent.tick != old.intent.tick && !intent.tick.is_after(old.intent.tick) {
                return Err(ViewError::StaleTick);
            }
            if intent == old.intent {
                return Ok(None);
            }
        }
        let cylinder = Cylindrical::new(
            Vector2::new(intent.center.x, intent.center.z),
            intent.radius,
        );
        let (view_ticket, simulation_ticket) = if intent.spectator_generates {
            (
                Some(ChunkLoading::get_level_from_view_distance(
                    intent.radius.get() + 1,
                )),
                Some(ChunkLoading::get_level_from_simulation_distance(
                    intent.simulation_distance,
                )),
            )
        } else {
            (None, None)
        };
        let new = ViewState {
            intent,
            cylinder,
            view_ticket,
            simulation_ticket,
        };
        let (enter, leave) = match old {
            Some(old) if old.intent.world == intent.world => {
                let (enter, leave) = Cylindrical::changed_chunks(old.cylinder, cylinder);
                (
                    enter.map(|pos| chunk(intent.world, pos)).collect(),
                    leave.map(|pos| chunk(intent.world, pos)).collect(),
                )
            }
            Some(old) => (
                cylinder
                    .all_chunks_within()
                    .map(|pos| chunk(intent.world, pos))
                    .collect(),
                old.cylinder
                    .all_chunks_within()
                    .map(|pos| chunk(old.intent.world, pos))
                    .collect(),
            ),
            None => (
                cylinder
                    .all_chunks_within()
                    .map(|pos| chunk(intent.world, pos))
                    .collect(),
                Vec::new(),
            ),
        };
        self.views.insert(intent.actor, new);
        Ok(Some(ViewDelta {
            old,
            new,
            enter,
            leave,
            center_changed: old.is_none_or(|old| {
                old.intent.center != intent.center || old.intent.world != intent.world
            }),
            delivery_reset: old
                .is_some_and(|old| old.intent.chunk_send_epoch != intent.chunk_send_epoch),
        }))
    }

    pub fn get(&self, actor: GlobalPlayerId) -> Option<&ViewState> {
        self.views.get(&actor)
    }

    pub fn remove(&mut self, actor: GlobalPlayerId) -> Option<ViewState> {
        self.views.remove(&actor)
    }
}

fn chunk(world: WorldId, position: Vector2<i32>) -> ChunkKey {
    ChunkKey::new(world, position.x, position.y)
}

#[cfg(test)]
mod tests {
    use std::num::NonZero;

    use pumpkin_cluster::controller::ViewIntent;
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::{ChunkAddr, WorldId};
    use pumpkin_cluster::time::TickStamp;

    use super::OwnedViews;

    fn intent(x: i32, world: WorldId, tick: u16) -> ViewIntent {
        ViewIntent {
            actor: GlobalPlayerId::new(ServerId(1), PlayerSlot(2)),
            owner_install_generation: 1,
            world,
            center: ChunkAddr { x, z: 0 },
            radius: NonZero::new(2).unwrap(),
            spectator_generates: true,
            simulation_distance: 2,
            chunk_send_epoch: 3,
            tick: TickStamp(tick),
        }
    }

    #[test]
    fn adjacent_center_moves_only_edge_chunks() {
        let mut views = OwnedViews::default();
        let world = WorldId::new(0, 1);
        let initial = views.apply(intent(0, world, 10)).unwrap().unwrap();
        assert_eq!(initial.enter.len(), 49);
        let moved = views.apply(intent(1, world, 11)).unwrap().unwrap();
        assert_eq!(moved.enter.len(), 7);
        assert_eq!(moved.leave.len(), 7);
        assert!(moved.enter.iter().all(|chunk| chunk.x() == 4));
        assert!(moved.leave.iter().all(|chunk| chunk.x() == -3));
        assert!(!moved.delivery_reset);
    }

    #[test]
    fn world_change_releases_old_world_without_cross_world_overlap() {
        let mut views = OwnedViews::default();
        let old_world = WorldId::new(0, 1);
        let new_world = WorldId::new(1, 2);
        views.apply(intent(0, old_world, 10)).unwrap();
        let changed = views.apply(intent(0, new_world, 11)).unwrap().unwrap();
        assert_eq!(changed.enter.len(), 49);
        assert_eq!(changed.leave.len(), 49);
        assert!(changed.enter.iter().all(|chunk| chunk.world == new_world));
        assert!(changed.leave.iter().all(|chunk| chunk.world == old_world));
        assert!(changed.center_changed);
    }
}
