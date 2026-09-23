use serde::{Deserialize, Serialize};

use crate::identity::GlobalPlayerId;
use crate::identity::ServerId;
use crate::protocol::{EntityRef, GlobalEntityId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ActorId {
    Player(GlobalPlayerId),
    Entity(GlobalEntityId),
    Peer(ServerId),
}

impl ActorId {
    #[must_use]
    pub const fn origin(self) -> ServerId {
        match self {
            Self::Player(player) => player.server,
            Self::Entity(entity) => entity.origin,
            Self::Peer(peer) => peer,
        }
    }
}

impl From<GlobalPlayerId> for ActorId {
    fn from(player: GlobalPlayerId) -> Self {
        Self::Player(player)
    }
}

impl From<GlobalEntityId> for ActorId {
    fn from(entity: GlobalEntityId) -> Self {
        Self::Entity(entity)
    }
}

impl From<EntityRef> for ActorId {
    fn from(entity: EntityRef) -> Self {
        Self::Entity(entity.id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaState<State, Action> {
    pub ground: State,
    pub local: State,
    pub pending: Vec<Action>,
}

impl<State: Clone, Action: PartialEq> ReplicaState<State, Action> {
    #[must_use]
    pub fn new(ground: State) -> Self {
        Self {
            local: ground.clone(),
            ground,
            pending: Vec::new(),
        }
    }

    pub fn apply_local(&mut self, action: Action, apply: impl FnOnce(&mut State, &Action)) {
        apply(&mut self.local, &action);
        self.pending.push(action);
    }

    pub fn accept(
        &mut self,
        accepted: &[Action],
        apply_ground: impl Fn(&mut State, &Action),
        apply_remote_local: impl Fn(&mut State, &Action),
        undo_local: impl Fn(&mut State, &Action),
    ) {
        for pending in self.pending.iter().rev() {
            if !accepted.contains(pending) {
                undo_local(&mut self.local, pending);
            }
        }
        for action in accepted {
            apply_ground(&mut self.ground, action);
            if !self.pending.contains(action) {
                apply_remote_local(&mut self.local, action);
            }
        }
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Delta(i32);

    #[test]
    fn acceptance_undoes_only_losers_and_does_not_replay_local_winners() {
        let mut replica = ReplicaState::new(0_i32);
        replica.apply_local(Delta(5), |state, action| *state += action.0);
        replica.apply_local(Delta(3), |state, action| *state += action.0);

        replica.accept(
            &[Delta(5), Delta(7)],
            |state, action| *state += action.0,
            |state, action| *state += action.0,
            |state, action| *state -= action.0,
        );

        assert_eq!(replica.ground, 12);
        assert_eq!(replica.local, 12);
        assert!(replica.pending.is_empty());
    }
}
