use std::collections::{BTreeMap, VecDeque};

use pumpkin_cluster::action_graph::{ActionGraphs, SemanticWrapper};
use pumpkin_cluster::canonical_action::{Action, ActionRef, AppliedAction};

use super::Server;

pub(crate) struct ObservationQueue {
    pub(crate) watches: super::cluster_test_watch::WatchQueue,
    requests: crossbeam::queue::ArrayQueue<ActionRef>,
    observations: crossbeam::queue::ArrayQueue<GraphObservation>,
}

impl ObservationQueue {
    pub(crate) fn new() -> Self {
        Self {
            watches: super::cluster_test_watch::WatchQueue::new(),
            requests: crossbeam::queue::ArrayQueue::new(128),
            observations: crossbeam::queue::ArrayQueue::new(128),
        }
    }

    pub(crate) fn request(&self, reference: ActionRef) -> Result<(), ActionRef> {
        self.requests.push(reference)
    }

    pub(crate) fn next(&self) -> Option<GraphObservation> {
        self.observations.pop()
    }
}

#[derive(Clone)]
pub(crate) struct GraphObservation {
    pub reference: ActionRef,
    pub action: Action,
    pub wrapper: SemanticWrapper,
    pub factual_result: Option<AppliedAction>,
    pub local_result: Option<AppliedAction>,
    pub generation: u64,
    pub local_generation: Option<u64>,
    pub ground_applied: bool,
    pub cumulative_ground_living: Option<GroundLivingWitness>,
}

#[derive(Clone)]
pub(crate) struct GroundLivingWitness {
    pub actor: pumpkin_cluster::actor::ActorId,
    pub simulation_tick: Option<pumpkin_cluster::time::TickStamp>,
    pub health_milli: u32,
    pub absorption_milli: u32,
    pub velocity_micro: [i64; 3],
    pub timing: pumpkin_cluster::item_action::CombatTiming,
    pub environment: pumpkin_cluster::item_action::LivingEnvironmentState,
    pub food: pumpkin_cluster::item_action::ActorFoodState,
}

impl GroundLivingWitness {
    pub(crate) fn capture(state: &super::cluster_world_actor::ActorLivingState) -> Self {
        Self {
            actor: state.combat.actor,
            simulation_tick: state.simulation_tick,
            health_milli: state.combat.health_milli,
            absorption_milli: state.combat.absorption_milli,
            velocity_micro: state.combat.velocity_micro,
            timing: state.combat.timing,
            environment: state.environment,
            food: state.food,
        }
    }
}

#[derive(Default)]
struct PendingObservation {
    previous: Option<(u64, Option<u64>, SemanticWrapper, bool)>,
    deferred: VecDeque<GraphObservation>,
    terminal: bool,
}

#[derive(Default)]
pub(crate) struct GraphObserver {
    watcher: super::cluster_test_watch::TargetWatcher,
    pending: BTreeMap<ActionRef, PendingObservation>,
}

impl GraphObserver {
    pub(crate) fn observe(
        &mut self,
        server: &Server,
        graphs: &ActionGraphs,
        living: &super::cluster_world_actor::OwnedLivingActors,
    ) {
        self.observe_queue(&server.cluster_test.observations, graphs);
        self.watcher.observe(
            &server.cluster_test.observations.watches,
            graphs,
            Some(living),
        );
    }

    fn observe_queue(&mut self, queue: &ObservationQueue, graphs: &ActionGraphs) {
        while self.pending.len() < 128 {
            let Some(reference) = queue.requests.pop() else {
                break;
            };
            self.pending.entry(reference).or_default();
        }
        self.pending.retain(|reference, pending| {
            if !pending.terminal
                && let Some(node) = graphs
                    .ticks
                    .get(&reference.tick)
                    .and_then(|graph| graph.actions.get(&reference.id))
            {
                let ground_applied = node.is_ground_applied();
                let current = (
                    node.generation,
                    node.local_generation,
                    node.wrapper,
                    ground_applied,
                );
                if pending.previous.as_ref() != Some(&current) {
                    if pending.deferred.len() == 8 {
                        tracing::error!(action = ?reference, "cluster test graph observation overflow");
                        pending.terminal = true;
                    } else {
                        pending.deferred.push_back(GraphObservation {
                            reference: *reference,
                            action: node.semantic_action().clone(),
                            wrapper: node.wrapper,
                            factual_result: node.factual.result.clone(),
                            local_result: node.local_result.clone(),
                            generation: node.generation,
                            local_generation: node.local_generation,
                            ground_applied,
                            cumulative_ground_living: None,
                        });
                        pending.previous = Some(current);
                        pending.terminal = ground_applied;
                    }
                }
            }
            while let Some(observation) = pending.deferred.pop_front() {
                if let Err(observation) = queue.observations.push(observation) {
                    pending.deferred.push_front(observation);
                    break;
                }
            }
            !pending.terminal || !pending.deferred.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::canonical_action::{
        ActionClass, ActionId, ActionKind, Cell, ChunkPart, ClientOrigin, Condition, Effect,
        Expected, OwnedStakeholderSet, Stakeholder, Target, TruthView, UndoProcedure, Value,
        apply_action,
    };
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
    use pumpkin_cluster::time::TickStamp;

    #[test]
    fn ground_observation_survives_queue_backpressure_and_graph_retirement() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 0, y: 80, z: 4 }),
            inventory: None,
        };
        let action = Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Peer(ServerId(0)),
                incarnation: 1,
                sequence: 7,
                family: 0,
            },
            tick: TickStamp(17),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            dependencies: Vec::new(),
            conditions: vec![Condition {
                target,
                expected: Expected::BlockState(0),
            }],
            effects: vec![Effect::Put {
                target,
                value: Value::Block(1),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Block,
            client_origin: ClientOrigin::None,
            presentation: Vec::new(),
        };
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        let before = Cell {
            revision: 0,
            value: Value::Block(0),
        };
        let mut owners = OwnedStakeholderSet {
            local: BTreeMap::from([(target, before.clone())]),
            ground: BTreeMap::from([(target, before)]),
        };
        let applied = apply_action(&mut owners, &action, TruthView::Local).unwrap();
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(action.tick, [action.clone()])
            .unwrap();
        graphs
            .ticks
            .get_mut(&action.tick)
            .unwrap()
            .mark_local_applied(action.id, 0, applied)
            .unwrap();
        let queue = ObservationQueue::new();
        let mut watched = queue
            .watches
            .watch(
                std::collections::BTreeSet::from([action.id.actor]),
                std::collections::BTreeSet::from([target]),
            )
            .unwrap();
        let mut watcher = super::super::cluster_test_watch::TargetWatcher::default();
        watcher.observe(&queue.watches, &graphs, None);
        assert!(!watched.try_recv().unwrap().ground_applied);
        queue.request(reference).unwrap();
        let mut observer = GraphObserver::default();
        observer.observe_queue(&queue, &graphs);
        let local = queue.next().unwrap();
        assert!(!local.ground_applied);
        assert_eq!(owners.ground[&target].value, Value::Block(0));
        for _ in 0..128 {
            assert!(queue.observations.push(local.clone()).is_ok());
        }
        apply_action(&mut owners, &action, TruthView::Ground).unwrap();
        let realms = {
            let node = graphs
                .ticks
                .get_mut(&action.tick)
                .unwrap()
                .actions
                .get_mut(&action.id)
                .unwrap();
            node.stable = true;
            node.realms.clone()
        };
        for realm in realms {
            graphs.mark_ground_realm_applied(reference, realm).unwrap();
        }
        observer.observe_queue(&queue, &graphs);
        watcher.observe(&queue.watches, &graphs, None);
        graphs.ticks.clear();
        let watched_ground = watched.try_recv().unwrap();
        assert!(watched_ground.ground_applied);
        assert_eq!(watched_ground.reference, reference);
        for _ in 0..128 {
            assert!(!queue.next().unwrap().ground_applied);
        }
        observer.observe_queue(&queue, &graphs);
        let ground = queue.next().unwrap();
        assert!(ground.ground_applied);
        assert_eq!(ground.reference, reference);
        assert_eq!(owners.ground[&target].value, Value::Block(1));
        assert!(observer.pending.is_empty());
        assert!(queue.next().is_none());
    }

    #[test]
    fn followed_item_retirement_is_observed_before_graph_drain_in_reverse_actor_order() {
        use pumpkin_cluster::entity_body::{DroppedItemChange, EntitySpatial};
        use pumpkin_cluster::entity_origin::EntityOriginAllocator;
        use pumpkin_cluster::inventory::{INV_MAIN, InvLoc};
        use pumpkin_cluster::realm_completion::PeerGeneration;

        let source = PeerGeneration {
            server: ServerId(0),
            generation: 1,
        };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let block = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 0, y: 80, z: 4 }),
            inventory: None,
        };
        let mut spawn = Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Peer(source.server),
                incarnation: source.generation,
                sequence: 11,
                family: 0,
            },
            tick: TickStamp(17),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            dependencies: Vec::new(),
            conditions: vec![Condition {
                target: block,
                expected: Expected::BlockState(0),
            }],
            effects: vec![Effect::Put {
                target: block,
                value: Value::Block(1),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Block,
            client_origin: ClientOrigin::None,
            presentation: Vec::new(),
        };
        let birth = ActionRef {
            tick: spawn.tick,
            id: spawn.id,
        };
        let identity = EntityOriginAllocator::new(source)
            .unwrap()
            .reserve(source, birth, 1)
            .unwrap()[0]
            .clone();
        let cell = super::super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, 0),
            &pumpkin_data::item_stack::ItemStack::new(
                7,
                &pumpkin_data::item::Item::NETHERITE_INGOT,
            ),
        )
        .unwrap();
        let body = super::super::cluster_item_birth::reserved_item_body(
            birth,
            &identity,
            source,
            EntitySpatial {
                world: chunk.world,
                position: [0.5_f64.to_bits(), 80.0_f64.to_bits(), 4.5_f64.to_bits()],
                velocity_micro: [0; 3],
                yaw: 0,
                pitch: 0,
                on_ground: true,
            },
            &cell,
            40,
        )
        .unwrap();
        let body_target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(body.id),
            inventory: None,
        };
        pumpkin_cluster::item_lifecycle::append_birth(&mut spawn, body.clone(), chunk).unwrap();
        let retire = pumpkin_cluster::item_lifecycle::lifecycle_action(
            &body,
            chunk,
            source,
            source,
            spawn.tick,
            12,
            DroppedItemChange::Damage {
                amount_milli: 5_000,
                fire: false,
            },
        )
        .unwrap();
        let retirement = ActionRef {
            tick: retire.tick,
            id: retire.id,
        };
        let before = Cell {
            revision: 0,
            value: Value::Block(0),
        };
        let mut owners = OwnedStakeholderSet {
            local: BTreeMap::from([(block, before.clone())]),
            ground: BTreeMap::from([(block, before)]),
        };
        let local_birth = apply_action(&mut owners, &spawn, TruthView::Local).unwrap();
        let local_retirement = apply_action(&mut owners, &retire, TruthView::Local).unwrap();
        assert!(!owners.local.contains_key(&body_target));
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(spawn.tick, [spawn.clone(), retire.clone()])
            .unwrap();
        {
            let graph = graphs.ticks.get_mut(&spawn.tick).unwrap();
            graph.mark_local_applied(spawn.id, 0, local_birth).unwrap();
            graph
                .mark_local_applied(retire.id, 0, local_retirement)
                .unwrap();
            assert_eq!(graph.actions.keys().next(), Some(&retire.id));
        }
        let queue = ObservationQueue::new();
        let mut observations = queue
            .watches
            .watch_follow_item_births(
                std::collections::BTreeSet::from([spawn.id.actor]),
                std::collections::BTreeSet::from([block]),
            )
            .unwrap();
        let mut watcher = super::super::cluster_test_watch::TargetWatcher::default();
        watcher.observe(&queue.watches, &graphs, None);
        let first = observations.try_recv().unwrap();
        let second = observations.try_recv().unwrap();
        assert_eq!(first.reference, birth);
        assert_eq!(second.reference, retirement);
        assert!(!first.ground_applied && !second.ground_applied);
        for action in [&spawn, &retire] {
            apply_action(&mut owners, action, TruthView::Ground).unwrap();
            let reference = ActionRef {
                tick: action.tick,
                id: action.id,
            };
            let realms = {
                let node = graphs
                    .ticks
                    .get_mut(&action.tick)
                    .unwrap()
                    .actions
                    .get_mut(&action.id)
                    .unwrap();
                node.stable = true;
                node.realms.clone()
            };
            for realm in realms {
                graphs.mark_ground_realm_applied(reference, realm).unwrap();
            }
        }
        watcher.observe(&queue.watches, &graphs, None);
        graphs.ticks.clear();
        let retired_ground = observations.try_recv().unwrap();
        let born_ground = observations.try_recv().unwrap();
        assert_eq!(retired_ground.reference, retirement);
        assert_eq!(born_ground.reference, birth);
        assert!(retired_ground.ground_applied && born_ground.ground_applied);
        let write = retired_ground
            .local_result
            .unwrap()
            .writes
            .into_iter()
            .find(|write| write.target == body_target)
            .unwrap();
        assert!(write.after.is_none());
        assert!(
            matches!(write.before.unwrap().value, Value::Entity(old) if old.birth == body.birth && old.id == body.id)
        );
        assert!(!owners.ground.contains_key(&body_target));
        assert!(observations.try_recv().is_err());
    }
}
