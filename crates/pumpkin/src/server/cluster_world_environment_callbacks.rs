use std::collections::BTreeMap;
use std::sync::Arc;

use pumpkin_cluster::canonical_action::{
    Action, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, Condition, Effect, Expected,
    ScheduledTickFact, ScheduledTickKind, ScheduledTickOrigin, Target, TruthView, Value,
};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_cluster::realm_completion::PeerGeneration;

use super::cluster_world_callbacks::{CallbackCapture, CallbackCaptureError, CallbackRoot};

pub(super) struct EnvironmentCallbackContext {
    pub reference: ActionRef,
    pub producer: PeerGeneration,
    pub world: Arc<crate::world::World>,
    pub chunk: ChunkKey,
    pub position: pumpkin_util::math::position::BlockPos,
    pub root: CallbackRoot,
    pub scheduled: Option<ScheduledTickFact>,
    pub fluid_rules_witness: Option<pumpkin_cluster::world_rules::WorldRulesWitness>,
}

impl EnvironmentCallbackContext {
    pub(super) fn random(
        reference: ActionRef,
        producer: PeerGeneration,
        world: Arc<crate::world::World>,
        chunk: ChunkKey,
        position: pumpkin_util::math::position::BlockPos,
        seed: u64,
        environment: &super::cluster_environment::EnvironmentView,
    ) -> Self {
        use pumpkin_data::environment_attribute::{DayTimeline, sample_float_track};
        let dimension = &world.dimension;
        let multiplier = if dimension.has_skylight && !dimension.has_fixed_time {
            sample_float_track(
                DayTimeline::SKY_LIGHT_LEVEL_KEYFRAMES,
                DayTimeline::PERIOD_TICKS,
                environment.time.time_of_day,
            )
        } else {
            1.0
        };
        let mut level = dimension.effective_sky_light_level() * multiplier;
        if dimension.has_skylight {
            let rain =
                (environment.weather.rain_level - environment.weather.thunder_level).max(0.0);
            level += rain * 0.3125 * (4.0 - level);
            level += environment.weather.thunder_level * 0.52734375 * (4.0 - level);
        }
        let ambient_darkness = (15.0 - level.clamp(0.0, 15.0)).clamp(0.0, 15.0) as u8;
        Self {
            reference,
            producer,
            world,
            chunk,
            position,
            root: CallbackRoot::Random {
                seed,
                ambient_darkness,
            },
            scheduled: None,
            fluid_rules_witness: None,
        }
    }

    pub(super) fn capture<S: ActionState>(
        &self,
        view: &S,
        snapshot: &mut crate::block::neighbor::NeighborSnapshot,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<CallbackCapture, CallbackCaptureError> {
        snapshot.processing_tick = self.scheduled;
        if let Some(fact) = self.scheduled {
            let queue = Target::Chunk {
                key: self.chunk,
                part: ChunkPart::Whole,
                inventory: None,
            };
            let cell = view
                .read(TruthView::Local, queue)
                .ok_or(CallbackCaptureError::InvalidWrite(queue))?;
            if !matches!(&cell.value, Value::ScheduledQueue(entries) if entries.contains(&fact)) {
                return Err(CallbackCaptureError::InvalidWrite(queue));
            }
            facts.insert(queue, Some(cell));
            let target = Target::Chunk {
                key: self.chunk,
                part: ChunkPart::Block(fact.position),
                inventory: None,
            };
            let cell = view
                .read(TruthView::Local, target)
                .ok_or(CallbackCaptureError::InvalidWrite(target))?;
            let Value::Block(id) = cell.value else {
                return Err(CallbackCaptureError::InvalidWrite(target));
            };
            let state = pumpkin_data::BlockStateId::new(id)
                .ok_or(CallbackCaptureError::InvalidWrite(target))?;
            facts.insert(target, Some(cell));
            let matches = match fact.kind {
                ScheduledTickKind::Block => {
                    state.to_block().id
                        == pumpkin_data::BlockId::new(fact.id)
                            .ok_or(CallbackCaptureError::InvalidWrite(target))?
                }
                ScheduledTickKind::Fluid => pumpkin_data::fluid::Fluid::from_id(fact.id)
                    .is_some_and(|fluid| {
                        pumpkin_data::fluid::Fluid::from_state_id(state)
                            .is_some_and(|current| current.matches_type(fluid))
                            || fluid.matches_type(&pumpkin_data::fluid::Fluid::WATER)
                                && state.to_block().is_waterlogged(state)
                    }),
            };
            if !matches {
                return Ok(CallbackCapture::Ready(
                    crate::block::neighbor::BlockCallbackResolution::default(),
                ));
            }
        }
        super::cluster_world_callbacks::capture_root(
            view,
            self.chunk.world,
            &self.world.block_registry,
            snapshot,
            facts,
            self.position,
            self.root,
            4096,
        )
        .map(|(_, capture)| capture)
    }

    pub(super) fn complete<S: ActionState>(
        &self,
        view: &S,
        facts: &BTreeMap<Target, Option<Cell>>,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
    ) -> Result<Action, ApplyFailure> {
        let mut action = super::cluster_world_callbacks::build_callback_action(
            view,
            self.reference,
            self.producer,
            Some(self.chunk),
            None,
            self.chunk.world,
            facts,
            resolution,
        )?;
        if let Some(fact) = self.scheduled {
            let target = Target::Chunk {
                key: self.chunk,
                part: ChunkPart::Whole,
                inventory: None,
            };
            action.conditions.push(Condition {
                target,
                expected: Expected::ScheduledTickFact {
                    fact,
                    present: true,
                },
            });
            action
                .effects
                .insert(0, Effect::CompleteScheduledTick { target, fact });
            if let ScheduledTickOrigin::Action { cause, .. } = fact.origin {
                action.dependencies.push(cause);
            }
        }
        super::cluster_world_callbacks::append_resolution_light(
            view,
            &self.world,
            self.chunk.world,
            &mut action,
            resolution,
        )?;
        action.validate()?;
        Ok(action)
    }

    pub(super) fn finalize(&self, action: &mut Action) -> Result<(), ApplyFailure> {
        let CallbackRoot::FluidScheduled { rules, .. } = self.root else {
            return Ok(());
        };
        let witness = self
            .fluid_rules_witness
            .ok_or(ApplyFailure::InvalidAction)?;
        let intent = pumpkin_cluster::world_action::WorldReplayIntent {
            steps: vec![
                pumpkin_cluster::world_action::WorldReplayStep::WorldFluidRules {
                    source: self.reference,
                    witness,
                    water_source_conversion: rules.water_source_conversion,
                    lava_source_conversion: rules.lava_source_conversion,
                },
                pumpkin_cluster::world_action::WorldReplayStep::RequiredEffects {
                    conditions: action.conditions.clone(),
                    effects: action.effects.clone(),
                },
            ],
        };
        action.class = pumpkin_cluster::canonical_action::ActionClass::Replayable;
        action.counterfactual_conditions = intent.read_targets().into_iter().collect();
        action.counterfactual_effects = intent.effect_targets().into_iter().collect();
        action.semantic_intent = Some(pumpkin_cluster::canonical_action::SemanticIntent::World(
            intent,
        ));
        action.validate()
    }

    pub(super) fn valid(&self) -> bool {
        let position = BlockPos {
            x: self.position.0.x,
            y: self.position.0.y,
            z: self.position.0.z,
        };
        self.reference.id.actor == pumpkin_cluster::actor::ActorId::Peer(self.producer.server)
            && self.reference.id.incarnation == self.producer.generation
            && self.producer.generation != 0
            && self.world.level.cluster_world_key == self.chunk.world.key
            && self.world.level.cluster_dimension == self.chunk.world.dimension
            && self.chunk.x() == position.x.div_euclid(16)
            && self.chunk.z() == position.z.div_euclid(16)
            && matches!(
                (self.root, self.scheduled),
                (CallbackRoot::Random { .. }, None)
                    | (CallbackRoot::Scheduled, Some(_))
                    | (CallbackRoot::FluidScheduled { .. }, Some(_))
            )
            && match (self.root, self.fluid_rules_witness) {
                (CallbackRoot::FluidScheduled { .. }, Some(witness)) => {
                    witness.rules.world == self.chunk.world
                        && witness.slot as u16 == self.reference.tick.0
                }
                (CallbackRoot::FluidScheduled { .. }, None) => false,
                (_, None) => true,
                _ => false,
            }
            && !matches!(self.root, CallbackRoot::Random { ambient_darkness, .. } if ambient_darkness > 15)
            && match (self.root, self.scheduled) {
                (CallbackRoot::Scheduled, Some(fact)) => fact.kind == ScheduledTickKind::Block,
                (CallbackRoot::FluidScheduled { fluid, .. }, Some(fact)) => {
                    fact.kind == ScheduledTickKind::Fluid && fact.id == fluid
                }
                _ => true,
            }
            && self.scheduled.is_none_or(|fact| {
                fact.position == position
                    && !fact.due.is_after(self.reference.tick)
                    && fact.executor == self.producer
            })
    }
}
