use std::collections::{HashMap, HashSet};

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::protocol::{ChunkKey, PlayerReplicaSnapshot, WorldId};

#[derive(Clone, Copy)]
pub struct ViewerVisibility {
    pub actor: GlobalPlayerId,
    pub world: WorldId,
    pub position: [f64; 3],
    pub radius_chunks: u8,
    pub chunk_send_epoch: u32,
}

pub enum VisibilityEvent {
    Spawn {
        viewer: GlobalPlayerId,
        actor: GlobalPlayerId,
        snapshot: PlayerReplicaSnapshot,
    },
    Update {
        viewer: GlobalPlayerId,
        actor: GlobalPlayerId,
        snapshot: PlayerReplicaSnapshot,
    },
    Remove {
        viewer: GlobalPlayerId,
        actor: GlobalPlayerId,
    },
}

struct ViewerState {
    view: ViewerVisibility,
    ready: HashSet<ChunkKey>,
}

#[derive(Default)]
pub struct OwnedVisibility {
    viewers: HashMap<GlobalPlayerId, ViewerState>,
    actors: HashMap<GlobalPlayerId, PlayerReplicaSnapshot>,
    visible: HashSet<(GlobalPlayerId, GlobalPlayerId)>,
}

impl OwnedVisibility {
    pub fn update_viewer(&mut self, view: ViewerVisibility) -> Vec<VisibilityEvent> {
        match self.viewers.get_mut(&view.actor) {
            Some(current) => {
                if current.view.actor == view.actor
                    && current.view.world == view.world
                    && current.view.position == view.position
                    && current.view.radius_chunks == view.radius_chunks
                    && current.view.chunk_send_epoch == view.chunk_send_epoch
                {
                    return Vec::new();
                }
                if current.view.chunk_send_epoch != view.chunk_send_epoch
                    || current.view.world != view.world
                {
                    current.ready.clear();
                }
                current.view = view;
            }
            None => {
                self.viewers.insert(
                    view.actor,
                    ViewerState {
                        view,
                        ready: HashSet::new(),
                    },
                );
            }
        }
        self.reconcile_viewer(view.actor)
    }

    pub fn chunks_ready(
        &mut self,
        viewer: GlobalPlayerId,
        epoch: u32,
        chunks: &[ChunkKey],
    ) -> Vec<VisibilityEvent> {
        let Some(state) = self.viewers.get_mut(&viewer) else {
            return Vec::new();
        };
        if state.view.chunk_send_epoch != epoch {
            return Vec::new();
        }
        let mut changed = false;
        for chunk in chunks {
            if chunk.world == state.view.world {
                changed |= state.ready.insert(*chunk);
            }
        }
        if !changed {
            return Vec::new();
        }
        self.reconcile_viewer(viewer)
    }

    pub fn chunk_unloaded(
        &mut self,
        viewer: GlobalPlayerId,
        chunk: ChunkKey,
    ) -> Vec<VisibilityEvent> {
        let Some(state) = self.viewers.get_mut(&viewer) else {
            return Vec::new();
        };
        if !state.ready.remove(&chunk) {
            return Vec::new();
        }
        self.reconcile_viewer(viewer)
    }

    pub fn update_actor(&mut self, snapshot: PlayerReplicaSnapshot) -> Vec<VisibilityEvent> {
        let actor = snapshot.movement.gid;
        self.actors.insert(actor, snapshot);
        let viewers: Vec<_> = self.viewers.keys().copied().collect();
        viewers
            .into_iter()
            .flat_map(|viewer| self.reconcile_actor_for(viewer, actor))
            .collect()
    }

    pub fn remove_actor(&mut self, actor: GlobalPlayerId) -> Vec<VisibilityEvent> {
        self.actors.remove(&actor);
        let viewers: Vec<_> = self.viewers.keys().copied().collect();
        viewers
            .into_iter()
            .filter_map(|viewer| {
                self.visible
                    .remove(&(viewer, actor))
                    .then_some(VisibilityEvent::Remove { viewer, actor })
            })
            .collect()
    }

    pub fn remove_viewer(&mut self, viewer: GlobalPlayerId) {
        self.viewers.remove(&viewer);
        self.visible
            .retain(|(held_viewer, _)| *held_viewer != viewer);
    }

    fn reconcile_viewer(&mut self, viewer: GlobalPlayerId) -> Vec<VisibilityEvent> {
        let actors: Vec<_> = self.actors.keys().copied().collect();
        actors
            .into_iter()
            .flat_map(|actor| self.reconcile_actor_for(viewer, actor))
            .collect()
    }

    fn reconcile_actor_for(
        &mut self,
        viewer: GlobalPlayerId,
        actor: GlobalPlayerId,
    ) -> Option<VisibilityEvent> {
        let snapshot = *self.actors.get(&actor)?;
        let should_see = self.viewers.get(&viewer).is_some_and(|state| {
            let position = snapshot.movement.pos;
            let chunk = ChunkKey::new(
                snapshot.movement.world,
                (position[0].floor() as i32).div_euclid(16),
                (position[2].floor() as i32).div_euclid(16),
            );
            let center_x = (state.view.position[0].floor() as i32).div_euclid(16);
            let center_z = (state.view.position[2].floor() as i32).div_euclid(16);
            let radius = i32::from(state.view.radius_chunks);
            viewer != actor
                && snapshot.movement.world == state.view.world
                && (chunk.x() - center_x).abs() <= radius
                && (chunk.z() - center_z).abs() <= radius
                && state.ready.contains(&chunk)
        });
        let was_visible = self.visible.contains(&(viewer, actor));
        match (was_visible, should_see) {
            (false, true) => {
                self.visible.insert((viewer, actor));
                Some(VisibilityEvent::Spawn {
                    viewer,
                    actor,
                    snapshot,
                })
            }
            (true, true) => Some(VisibilityEvent::Update {
                viewer,
                actor,
                snapshot,
            }),
            (true, false) => {
                self.visible.remove(&(viewer, actor));
                Some(VisibilityEvent::Remove { viewer, actor })
            }
            (false, false) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::{
        ChunkKey, PlayerReplicaSnapshot, PlayerVisualSnapshot, PosUpdate, WorldId,
    };
    use pumpkin_cluster::time::TickStamp;

    use super::{OwnedVisibility, ViewerVisibility, VisibilityEvent};

    fn player(server: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(1))
    }

    fn replica(actor: GlobalPlayerId, world: WorldId, x: f64) -> PlayerReplicaSnapshot {
        PlayerReplicaSnapshot {
            movement: PosUpdate {
                spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                    lease: pumpkin_cluster::canonical_action::LivePlayerLease {
                        key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::nil()),
                        gid: actor,
                        incarnation: 1,
                    },
                    admitted_at: TickStamp(1),
                },
                gid: actor,
                incarnation: 1,
                seq: PlayerSeq(1),
                tick: TickStamp(1),
                world,
                pos: [x, 80.0, 0.0],
                vel: [0.0; 3],
                yaw: 0.0,
                head_yaw: 0.0,
                pitch: 0.0,
                on_ground: true,
            },
            visual: PlayerVisualSnapshot::default(),
        }
    }

    fn view(actor: GlobalPlayerId, world: WorldId, epoch: u32) -> ViewerVisibility {
        ViewerVisibility {
            actor,
            world,
            position: [0.0, 80.0, 0.0],
            radius_chunks: 2,
            chunk_send_epoch: epoch,
        }
    }

    #[test]
    fn stationary_actor_spawns_once_when_chunk_ack_arrives_late() {
        let mut owner = OwnedVisibility::default();
        let world = WorldId::new(0, 1);
        let viewer = player(1);
        let actor = player(2);
        let chunk = ChunkKey::new(world, 1, 0);
        assert!(owner.update_viewer(view(viewer, world, 7)).is_empty());
        assert!(owner.update_actor(replica(actor, world, 17.0)).is_empty());
        let emitted = owner.chunks_ready(viewer, 7, &[chunk]);
        assert_eq!(emitted.len(), 1);
        assert!(
            matches!(emitted[0], VisibilityEvent::Spawn { viewer: v, actor: a, .. } if v == viewer && a == actor)
        );
        assert!(owner.chunks_ready(viewer, 7, &[chunk]).is_empty());
    }

    #[test]
    fn stale_ack_cannot_spawn_after_delivery_epoch_reset() {
        let mut owner = OwnedVisibility::default();
        let world = WorldId::new(0, 1);
        let viewer = player(1);
        let actor = player(2);
        let chunk = ChunkKey::new(world, 0, 0);
        owner.update_viewer(view(viewer, world, 7));
        owner.update_actor(replica(actor, world, 1.0));
        owner.update_viewer(view(viewer, world, 8));
        assert!(owner.chunks_ready(viewer, 7, &[chunk]).is_empty());
        assert!(matches!(
            owner.chunks_ready(viewer, 8, &[chunk]).as_slice(),
            [VisibilityEvent::Spawn { .. }]
        ));
        assert!(matches!(
            owner.update_viewer(view(viewer, world, 9)).as_slice(),
            [VisibilityEvent::Remove { .. }]
        ));
    }

    #[test]
    fn actor_crossing_into_unready_chunk_removes_until_delivery() {
        let mut owner = OwnedVisibility::default();
        let world = WorldId::new(0, 1);
        let viewer = player(1);
        let actor = player(2);
        owner.update_viewer(view(viewer, world, 7));
        owner.chunks_ready(viewer, 7, &[ChunkKey::new(world, 0, 0)]);
        assert!(matches!(
            owner.update_actor(replica(actor, world, 1.0)).as_slice(),
            [VisibilityEvent::Spawn { .. }]
        ));
        assert!(matches!(
            owner.update_actor(replica(actor, world, 17.0)).as_slice(),
            [VisibilityEvent::Remove { .. }]
        ));
        assert!(matches!(
            owner
                .chunks_ready(viewer, 7, &[ChunkKey::new(world, 1, 0)])
                .as_slice(),
            [VisibilityEvent::Spawn { .. }]
        ));
    }
}
