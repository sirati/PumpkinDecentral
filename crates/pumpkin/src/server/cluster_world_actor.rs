use std::collections::HashMap;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::controller::{ControllerActorIntent, ControllerActorSource, ControllerFrame};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_action::{
    ActorCombatState, ActorFoodState, ItemAction, ItemActorEffect, ItemCooldownState, ItemUseState,
};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActorSpatialState {
    pub frame: ControllerFrame,
    pub source: ControllerActorSource,
    pub world: WorldId,
    pub chunk: ChunkKey,
    pub biome_id: Option<u8>,
    pub sneaking: bool,
    pub sprinting: bool,
    pub crawling: bool,
    pub flying: bool,
    pub game_mode: u8,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActorSpatialTruth {
    pub ground: ActorSpatialState,
    pub local: ActorSpatialState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorSpatialError {
    MismatchedActor,
    InvalidPosition,
    StaleInput,
    InvalidGameMode,
    MissingInitialState,
}

#[derive(Default)]
pub struct OwnedActors {
    actors: HashMap<GlobalPlayerId, ActorSpatialTruth>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorLivingState {
    pub food: ActorFoodState,
    pub combat: ActorCombatState,
    pub use_state: Option<ItemUseState>,
    pub cooldowns: HashMap<String, ItemCooldownState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorLivingTruth {
    pub ground: ActorLivingState,
    pub local: ActorLivingState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorLivingImport {
    pub actor: ActorId,
    pub state: ActorLivingState,
}

pub(crate) struct ActorAdmissionImport {
    pub actor: GlobalPlayerId,
    pub statistics: super::cluster_actor_statistics::ActorStatisticsImport,
    pub living: ActorLivingState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorEffectError {
    MissingActor,
    StaleFood,
    StaleCombat,
    StaleUse,
    StaleCooldown,
    InvalidActor,
}

#[derive(Default)]
pub struct OwnedLivingActors {
    actors: HashMap<ActorId, ActorLivingTruth>,
    dirty: bool,
}

impl OwnedLivingActors {
    pub fn install(
        &mut self,
        actor: ActorId,
        state: ActorLivingState,
    ) -> Result<(), ActorEffectError> {
        if state.food.actor != actor || state.combat.actor != actor {
            return Err(ActorEffectError::InvalidActor);
        }
        self.actors.insert(
            actor,
            ActorLivingTruth {
                ground: state.clone(),
                local: state,
            },
        );
        self.dirty = true;
        Ok(())
    }

    pub fn get(&self, actor: ActorId) -> Option<&ActorLivingTruth> {
        self.actors.get(&actor)
    }

    pub fn contains(&self, actor: ActorId) -> bool {
        self.actors.contains_key(&actor)
    }

    pub fn prepare_local(
        &self,
        action: &ItemAction,
    ) -> Result<Vec<(ActorId, ActorLivingState)>, ActorEffectError> {
        self.prepare(action, false)
    }

    pub fn prepare_ground(
        &self,
        action: &ItemAction,
    ) -> Result<Vec<(ActorId, ActorLivingState)>, ActorEffectError> {
        self.prepare(action, true)
    }

    fn prepare(
        &self,
        action: &ItemAction,
        ground: bool,
    ) -> Result<Vec<(ActorId, ActorLivingState)>, ActorEffectError> {
        let mut changed = HashMap::<ActorId, ActorLivingState>::new();
        for effect in &action.actor_effects {
            let ItemActorEffect::Food(effect) = effect;
            let state = self.state_for(effect.before.actor, ground, &mut changed)?;
            if state.food != effect.before {
                return Err(ActorEffectError::StaleFood);
            }
            state.food = effect.after;
        }
        if let Some(effect) = action.combat {
            let state = self.state_for(effect.before.actor, ground, &mut changed)?;
            if state.combat != effect.before {
                return Err(ActorEffectError::StaleCombat);
            }
            state.combat = effect.after;
        }
        if let Some(transition) = &action.use_transition {
            let state = self.state_for(action.actor, ground, &mut changed)?;
            if state.use_state != transition.before {
                return Err(ActorEffectError::StaleUse);
            }
            state.use_state = transition.after.clone();
        }
        if let Some(transition) = &action.cooldown_transition {
            let state = self.state_for(action.actor, ground, &mut changed)?;
            if state.cooldowns.get(&transition.after.group) != transition.before.as_ref() {
                return Err(ActorEffectError::StaleCooldown);
            }
            state
                .cooldowns
                .insert(transition.after.group.clone(), transition.after.clone());
        }
        Ok(changed.into_iter().collect())
    }

    fn state_for<'a>(
        &'a self,
        actor: ActorId,
        ground: bool,
        changed: &'a mut HashMap<ActorId, ActorLivingState>,
    ) -> Result<&'a mut ActorLivingState, ActorEffectError> {
        if !changed.contains_key(&actor) {
            let truth = self
                .actors
                .get(&actor)
                .ok_or(ActorEffectError::MissingActor)?;
            changed.insert(
                actor,
                if ground {
                    truth.ground.clone()
                } else {
                    truth.local.clone()
                },
            );
        }
        changed
            .get_mut(&actor)
            .ok_or(ActorEffectError::MissingActor)
    }

    pub fn commit_local(
        &mut self,
        prepared: Vec<(ActorId, ActorLivingState)>,
    ) -> Result<(), ActorEffectError> {
        for (actor, _) in &prepared {
            if !self.actors.contains_key(actor) {
                return Err(ActorEffectError::MissingActor);
            }
        }
        for (actor, state) in prepared {
            self.actors
                .get_mut(&actor)
                .ok_or(ActorEffectError::MissingActor)?
                .local = state;
        }
        self.dirty = true;
        Ok(())
    }

    pub fn promote_ground(
        &mut self,
        prepared: Vec<(ActorId, ActorLivingState)>,
    ) -> Result<(), ActorEffectError> {
        for (actor, _) in &prepared {
            if !self.actors.contains_key(actor) {
                return Err(ActorEffectError::MissingActor);
            }
        }
        for (actor, state) in prepared {
            self.actors
                .get_mut(&actor)
                .ok_or(ActorEffectError::MissingActor)?
                .ground = state;
        }
        self.dirty = true;
        Ok(())
    }

    pub fn remove(&mut self, actor: ActorId) {
        self.dirty |= self.actors.remove(&actor).is_some();
    }

    pub fn publish(&mut self, server: &super::Server) {
        if !self.dirty {
            return;
        }
        server.cluster_actor_living.store(std::sync::Arc::new(
            self.actors
                .iter()
                .map(|(actor, truth)| (*actor, truth.local.clone()))
                .collect(),
        ));
        self.dirty = false;
    }
}

impl OwnedActors {
    pub fn apply_controller(
        &mut self,
        intent: ControllerActorIntent,
        biome_at: impl FnOnce(ChunkKey, BlockPos) -> Option<u8>,
    ) -> Result<ActorSpatialState, ActorSpatialError> {
        if intent.actor != intent.frame.claimed.actor
            || intent.actor != intent.frame.effective.actor
        {
            return Err(ActorSpatialError::MismatchedActor);
        }
        let position = intent.frame.effective.position;
        if !position
            .iter()
            .all(|coordinate| coordinate.is_finite() && coordinate.abs() <= 30_000_000.0)
            || !intent
                .frame
                .effective
                .velocity
                .iter()
                .all(|value| value.is_finite())
            || !intent.frame.effective.yaw.is_finite()
            || !intent.frame.effective.pitch.is_finite()
        {
            return Err(ActorSpatialError::InvalidPosition);
        }
        let previous = self.actors.get(&intent.actor).map(|state| state.local);
        let game_mode = match (intent.source, intent.gamemode, previous) {
            (ControllerActorSource::Initial, Some(mode), _)
            | (ControllerActorSource::AcceptedAdministrative(_), Some(mode), _)
                if mode <= 3 =>
            {
                mode
            }
            (ControllerActorSource::Client, None, Some(previous)) => previous.game_mode,
            (ControllerActorSource::Client, None, None) => {
                return Err(ActorSpatialError::MissingInitialState);
            }
            _ => return Err(ActorSpatialError::InvalidGameMode),
        };
        if let Some(previous) = previous {
            let prior_seq = previous.frame.effective.source_seq;
            let incoming_seq = intent.frame.effective.source_seq;
            let accepted_administrative = matches!(intent.source, ControllerActorSource::AcceptedAdministrative(action)
                if previous.source != ControllerActorSource::AcceptedAdministrative(action));
            if !accepted_administrative
                && !incoming_seq.is_newer_than(prior_seq)
                && !(incoming_seq == prior_seq
                    && intent.tick.is_after(previous.frame.effective.tick))
            {
                return Err(ActorSpatialError::StaleInput);
            }
        }
        let block = BlockPos {
            x: position[0].floor() as i32,
            y: position[1].floor() as i32,
            z: position[2].floor() as i32,
        };
        let chunk = ChunkKey::new(intent.world, block.x.div_euclid(16), block.z.div_euclid(16));
        let state = ActorSpatialState {
            frame: intent.frame,
            source: intent.source,
            world: intent.world,
            chunk,
            biome_id: biome_at(chunk, block),
            sneaking: intent
                .sneaking
                .unwrap_or_else(|| previous.is_some_and(|state| state.sneaking)),
            sprinting: intent
                .sprinting
                .unwrap_or_else(|| previous.is_some_and(|state| state.sprinting)),
            crawling: intent
                .crawling
                .unwrap_or_else(|| previous.is_some_and(|state| state.crawling)),
            flying: intent.effective_flying,
            game_mode,
            revision: previous.map_or(0, |state| state.revision.wrapping_add(1)),
        };
        self.actors.insert(
            intent.actor,
            ActorSpatialTruth {
                ground: state,
                local: state,
            },
        );
        Ok(state)
    }

    pub fn get(&self, actor: GlobalPlayerId) -> Option<&ActorSpatialTruth> {
        self.actors.get(&actor)
    }

    pub fn remove(&mut self, actor: GlobalPlayerId) {
        self.actors.remove(&actor);
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::controller::{
        ClientClaimedState, ControllerActorIntent, ControllerActorSource, ControllerDecision,
        ControllerFrame, ControllerMovementStatistic, EffectiveControllerState,
    };
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_cluster::time::TickStamp;

    use super::{ActorSpatialError, OwnedActors};

    fn intent(seq: u16, tick: u16, claimed_x: f64, effective_x: f64) -> ControllerActorIntent {
        let actor = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let claimed = ClientClaimedState {
            actor,
            seq: PlayerSeq(seq),
            tick: TickStamp(tick),
            position: [claimed_x, 80.0, 0.0],
            velocity: [0.0; 3],
            yaw: 0.0,
            pitch: 0.0,
            on_ground: true,
            input_flags: 0,
        };
        ControllerActorIntent {
            actor,
            source: ControllerActorSource::Initial,
            world: WorldId::new(0, 3),
            gamemode: Some(0),
            tick: TickStamp(tick),
            frame: ControllerFrame {
                claimed,
                effective: EffectiveControllerState {
                    actor,
                    source_seq: PlayerSeq(seq),
                    tick: TickStamp(tick),
                    position: [effective_x, 80.0, 0.0],
                    velocity: [0.0; 3],
                    yaw: 0.0,
                    pitch: 0.0,
                    on_ground: true,
                    input_flags: 0,
                },
                decision: ControllerDecision::Modified(1),
            },
            position_changed: true,
            velocity_changed: false,
            rotation_changed: false,
            ground_changed: false,
            input_changed: false,
            sneaking: None,
            sprinting: None,
            crawling: None,
            flying: None,
            effective_flying: false,
            movement_statistic: ControllerMovementStatistic::Unchecked,
        }
    }

    #[test]
    fn effective_position_owns_chunk_while_claim_is_preserved_for_evidence() {
        let mut actors = OwnedActors::default();
        let applied = actors
            .apply_controller(intent(1, 10, 32.0, -0.25), |_, _| Some(4))
            .unwrap();
        assert_eq!(applied.chunk.x(), -1);
        assert_eq!(applied.biome_id, Some(4));
        assert_eq!(applied.frame.claimed.position[0], 32.0);
        assert_eq!(applied.frame.effective.position[0], -0.25);
        assert_eq!(
            actors.get(applied.frame.claimed.actor).unwrap().ground,
            applied
        );
    }

    #[test]
    fn delayed_policy_correction_replaces_same_sequence_only_on_later_tick() {
        let mut actors = OwnedActors::default();
        actors
            .apply_controller(intent(1, 10, 0.0, 0.0), |_, _| None)
            .unwrap();
        let mut same_tick = intent(1, 10, 0.0, 4.0);
        same_tick.source = ControllerActorSource::Client;
        same_tick.gamemode = None;
        assert_eq!(
            actors.apply_controller(same_tick, |_, _| None),
            Err(ActorSpatialError::StaleInput)
        );
        let mut later = intent(1, 11, 0.0, 4.0);
        later.source = ControllerActorSource::Client;
        later.gamemode = None;
        let corrected = actors.apply_controller(later, |_, _| None).unwrap();
        assert_eq!(corrected.frame.effective.position[0], 4.0);
        assert_eq!(corrected.revision, 1);
    }

    #[test]
    fn initial_frame_does_not_block_wrapped_first_movement() {
        let mut actors = OwnedActors::default();
        actors
            .apply_controller(intent(u16::MAX, 10, 0.0, 0.0), |_, _| None)
            .unwrap();
        let mut move_input = intent(0, 10, 0.0, 17.0);
        move_input.source = ControllerActorSource::Client;
        move_input.gamemode = None;
        let moved = actors.apply_controller(move_input, |_, _| None).unwrap();
        assert_eq!(moved.chunk.x(), 1);
        assert_eq!(moved.revision, 1);
    }

    #[test]
    fn accepted_admin_teleport_overrides_same_tick_client_pose_once() {
        let mut actors = OwnedActors::default();
        let first = intent(5, 10, 0.0, 1.0);
        actors.apply_controller(first, |_, _| None).unwrap();
        let mut teleport = intent(5, 10, 0.0, 48.0);
        teleport.source =
            ControllerActorSource::AcceptedAdministrative(pumpkin_cluster::completion::ActionKey {
                actor: pumpkin_cluster::actor::ActorId::Player(teleport.actor),
                sequence: 9,
                family: 7,
            });
        teleport.gamemode = Some(1);
        let applied = actors.apply_controller(teleport, |_, _| None).unwrap();
        assert_eq!(applied.chunk.x(), 3);
        assert_eq!(applied.game_mode, 1);
        assert_eq!(
            actors.apply_controller(teleport, |_, _| None),
            Err(ActorSpatialError::StaleInput)
        );
    }

    #[test]
    fn client_movement_cannot_change_accepted_game_mode() {
        let mut actors = OwnedActors::default();
        let first = intent(1, 10, 0.0, 0.0);
        let actor = first.actor;
        actors.apply_controller(first, |_, _| None).unwrap();
        let mut move_input = intent(2, 11, 0.0, 1.0);
        move_input.source = ControllerActorSource::Client;
        move_input.gamemode = None;
        assert_eq!(
            actors
                .apply_controller(move_input, |_, _| None)
                .unwrap()
                .game_mode,
            0
        );
        let mut teleport = intent(2, 11, 0.0, 48.0);
        teleport.source =
            ControllerActorSource::AcceptedAdministrative(pumpkin_cluster::completion::ActionKey {
                actor: pumpkin_cluster::actor::ActorId::Player(actor),
                sequence: 9,
                family: 7,
            });
        teleport.gamemode = Some(1);
        assert_eq!(
            actors
                .apply_controller(teleport, |_, _| None)
                .unwrap()
                .game_mode,
            1
        );
        let mut forged = intent(3, 12, 0.0, 49.0);
        forged.source = ControllerActorSource::Client;
        forged.gamemode = Some(0);
        assert_eq!(
            actors.apply_controller(forged, |_, _| None),
            Err(ActorSpatialError::InvalidGameMode)
        );
        forged.gamemode = None;
        assert_eq!(
            actors
                .apply_controller(forged, |_, _| None)
                .unwrap()
                .game_mode,
            1
        );
    }
}
