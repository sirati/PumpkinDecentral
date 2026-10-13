use std::collections::HashMap;
use std::sync::Arc;

use pumpkin_cluster::canonical_action::ActionRef;
use tokio::sync::mpsc;

use crate::block::neighbor::{CallbackQuery, CapturedNaturalBreak};
use crate::plugin::api::events::Cancellable;
use crate::plugin::api::events::block::{BlockBreakEvent, BlockPhysicsEvent};

use super::Server;

#[derive(Clone)]
pub(super) enum CallbackPolicyRequest {
    Fluid(crate::block::fluid::owned::FluidPolicy),
    Physics {
        position: pumpkin_util::math::position::BlockPos,
        source: pumpkin_util::math::position::BlockPos,
    },
    NaturalBreak {
        position: pumpkin_util::math::position::BlockPos,
        before: pumpkin_data::BlockStateId,
        loot: Option<crate::world::loot::BlockLootFacts>,
        drop_allowed: bool,
    },
}

impl CallbackPolicyRequest {
    pub(super) fn capture(
        reference: ActionRef,
        request: &crate::block::neighbor::CallbackRequest,
        snapshot: &crate::block::neighbor::NeighborSnapshot,
        loot_context: &crate::world::loot::LootContextParameters,
    ) -> Option<Self> {
        match request.query {
            CallbackQuery::FluidPolicy(policy) => Some(Self::Fluid(policy)),
            CallbackQuery::Physics(position, source) => Some(Self::Physics { position, source }),
            CallbackQuery::NaturalBreak(position, drop_allowed) => {
                let before = *request
                    .private_blocks
                    .get(&position)
                    .or_else(|| snapshot.blocks.get(&position))?;
                let position_seed = pumpkin_cluster::order::rank_tick_actor(
                    super::cluster_world_apply::CLUSTER_SEED
                        ^ reference.id.incarnation
                        ^ (u64::from(reference.id.sequence) << 32)
                        ^ (u64::from(reference.id.family) << 48)
                        ^ (position.0.x as u64).rotate_left(7)
                        ^ (position.0.y as u64).rotate_left(29)
                        ^ (position.0.z as u64).rotate_left(43),
                    reference.tick,
                    reference.id.actor,
                );
                let mut context = loot_context.clone();
                context.block_state = Some(before.to_state());
                context.position = Some(position.to_centered_f64());
                let loot = crate::world::loot::block_loot_facts(
                    before.to_block(),
                    position_seed as i64,
                    position_seed.rotate_left(17),
                    true,
                    &context,
                );
                Some(Self::NaturalBreak {
                    position,
                    before,
                    loot: drop_allowed.then_some(loot),
                    drop_allowed,
                })
            }
            _ => None,
        }
    }

    fn query(&self) -> CallbackQuery {
        match self {
            Self::Fluid(policy) => CallbackQuery::FluidPolicy(*policy),
            Self::Physics { position, source } => CallbackQuery::Physics(*position, *source),
            Self::NaturalBreak {
                position,
                drop_allowed,
                ..
            } => CallbackQuery::NaturalBreak(*position, *drop_allowed),
        }
    }
}

#[derive(Clone)]
pub(super) enum CallbackPolicyDecision {
    Fluid(bool),
    Physics(bool),
    NaturalBreak(CapturedNaturalBreak),
    Expired,
}

impl CallbackPolicyDecision {
    pub(super) fn install(
        self,
        request: &crate::block::neighbor::CallbackRequest,
        snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    ) -> bool {
        match (request.query.clone(), self) {
            (CallbackQuery::FluidPolicy(policy), Self::Fluid(allowed)) => {
                snapshot.fluid_policy.insert(policy, allowed);
                true
            }
            (CallbackQuery::Physics(position, source), Self::Physics(allowed)) => {
                snapshot.physics_allowed.insert((position, source), allowed);
                true
            }
            (CallbackQuery::NaturalBreak(position, _), Self::NaturalBreak(outcome)) => {
                let before = request
                    .private_blocks
                    .get(&position)
                    .or_else(|| snapshot.blocks.get(&position));
                if before != Some(&outcome.before) {
                    return false;
                }
                snapshot.natural_breaks.insert(position, outcome);
                true
            }
            _ => false,
        }
    }
}

struct PolicyOutcome {
    reference: ActionRef,
    query: CallbackQuery,
    generation: u64,
    decision: CallbackPolicyDecision,
}

pub(super) struct CallbackPolicyOwner {
    sender: mpsc::Sender<PolicyOutcome>,
    receiver: mpsc::Receiver<PolicyOutcome>,
    generation: u64,
    waiting: HashMap<(ActionRef, CallbackQuery), u64>,
    ready: HashMap<(ActionRef, CallbackQuery), CallbackPolicyDecision>,
}

impl CallbackPolicyOwner {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        Self {
            sender,
            receiver,
            generation: 0,
            waiting: HashMap::new(),
            ready: HashMap::new(),
        }
    }

    pub(super) fn stage(
        &mut self,
        server: &Arc<Server>,
        reference: ActionRef,
        request: CallbackPolicyRequest,
    ) -> bool {
        let query = request.query();
        let key = (reference, query.clone());
        if self.waiting.contains_key(&key) || self.ready.contains_key(&key) {
            return true;
        }
        if self.waiting.len() + self.ready.len() >= 1024 {
            return false;
        }
        let has_handlers = match &request {
            CallbackPolicyRequest::Fluid(crate::block::fluid::owned::FluidPolicy::Flow {
                ..
            }) => server
                .plugin_manager
                .has_handlers::<crate::plugin::api::events::block::block_from_to::BlockFromToEvent>(
            ),
            CallbackPolicyRequest::Fluid(crate::block::fluid::owned::FluidPolicy::Form {
                ..
            }) => server
                .plugin_manager
                .has_handlers::<crate::plugin::api::events::block::block_form::BlockFormEvent>(),
            CallbackPolicyRequest::Physics { .. } => {
                server.plugin_manager.has_handlers::<BlockPhysicsEvent>()
            }
            CallbackPolicyRequest::NaturalBreak { .. } => {
                server.plugin_manager.has_handlers::<BlockBreakEvent>()
            }
        };
        if !has_handlers {
            self.ready.insert(
                key,
                match request {
                    CallbackPolicyRequest::Fluid(_) => CallbackPolicyDecision::Fluid(true),
                    CallbackPolicyRequest::Physics { .. } => CallbackPolicyDecision::Physics(true),
                    CallbackPolicyRequest::NaturalBreak { before, loot, .. } => {
                        CallbackPolicyDecision::NaturalBreak(CapturedNaturalBreak {
                            before,
                            cancelled: false,
                            loot,
                        })
                    }
                },
            );
            return true;
        }
        let Ok(permit) = self.sender.clone().try_reserve_owned() else {
            return false;
        };
        self.generation = self.generation.wrapping_add(1).max(1);
        let generation = self.generation;
        self.waiting.insert(key, generation);
        let server = Arc::clone(server);
        tokio::spawn(async move {
            let decision = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                match request {
                    CallbackPolicyRequest::Fluid(
                        crate::block::fluid::owned::FluidPolicy::Flow { from, to, fluid },
                    ) => {
                        let Some(fluid) = pumpkin_data::fluid::Fluid::from_id(fluid) else {
                            return CallbackPolicyDecision::Expired;
                        };
                        let block = if fluid.matches_type(&pumpkin_data::fluid::Fluid::WATER) {
                            &pumpkin_data::Block::WATER
                        } else {
                            &pumpkin_data::Block::LAVA
                        };
                        let mut event =
                            crate::plugin::api::events::block::block_from_to::BlockFromToEvent::new(
                                from, to, block,
                            );
                        server.plugin_manager.fire(&server, &mut event).await;
                        CallbackPolicyDecision::Fluid(!event.cancelled())
                    }
                    CallbackPolicyRequest::Fluid(
                        crate::block::fluid::owned::FluidPolicy::Form { position, state },
                    ) => {
                        let mut event =
                            crate::plugin::api::events::block::block_form::BlockFormEvent::new(
                                position,
                                state.to_block(),
                            );
                        server.plugin_manager.fire(&server, &mut event).await;
                        CallbackPolicyDecision::Fluid(!event.cancelled())
                    }
                    CallbackPolicyRequest::Physics { position, source } => {
                        let mut event = BlockPhysicsEvent::new(position, source);
                        server.plugin_manager.fire(&server, &mut event).await;
                        CallbackPolicyDecision::Physics(!event.cancelled())
                    }
                    CallbackPolicyRequest::NaturalBreak {
                        position,
                        before,
                        mut loot,
                        ..
                    } => {
                        let mut event = BlockBreakEvent::new(
                            None,
                            before.to_block(),
                            position,
                            loot.as_ref().map_or(0, |loot| loot.experience),
                            loot.is_some(),
                        );
                        server.plugin_manager.fire(&server, &mut event).await;
                        if !event.drop {
                            loot = None;
                        }
                        if let Some(loot) = &mut loot {
                            loot.experience = event.exp;
                        }
                        CallbackPolicyDecision::NaturalBreak(CapturedNaturalBreak {
                            before,
                            cancelled: event.cancelled(),
                            loot,
                        })
                    }
                }
            })
            .await
            .unwrap_or(CallbackPolicyDecision::Expired);
            permit.send(PolicyOutcome {
                reference,
                query,
                generation,
                decision,
            });
        });
        true
    }

    pub(super) fn drain(&mut self) {
        while let Ok(outcome) = self.receiver.try_recv() {
            let key = (outcome.reference, outcome.query);
            if self.waiting.get(&key) != Some(&outcome.generation) {
                continue;
            }
            self.waiting.remove(&key);
            self.ready.insert(key, outcome.decision);
        }
    }

    pub(super) fn take(
        &mut self,
        reference: ActionRef,
        query: &CallbackQuery,
    ) -> Option<CallbackPolicyDecision> {
        self.ready.remove(&(reference, query.clone()))
    }

    pub(super) fn discard(&mut self, reference: ActionRef) {
        self.waiting
            .retain(|(candidate, _), _| *candidate != reference);
        self.ready
            .retain(|(candidate, _), _| *candidate != reference);
    }
}
