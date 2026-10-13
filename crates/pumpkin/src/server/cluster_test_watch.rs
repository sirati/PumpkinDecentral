use std::collections::{BTreeMap, BTreeSet, VecDeque};

use pumpkin_cluster::action_graph::{ActionGraphs, SemanticWrapper};
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{
    ActionRef, AppliedAction, ChunkPart, Target, TruthView, Value,
};
use pumpkin_cluster::entity_body::{EntityBirth, EntityPayload};
use pumpkin_cluster::protocol::GlobalEntityId;
use tokio::sync::mpsc;

use super::cluster_test_observation::GraphObservation;

pub(crate) struct WatchRequest {
    actors: BTreeSet<ActorId>,
    targets: BTreeSet<Target>,
    follow_item_births: bool,
    output: mpsc::Sender<GraphObservation>,
}

pub(crate) struct WatchQueue(crossbeam::queue::ArrayQueue<WatchRequest>);

impl WatchQueue {
    pub(crate) fn new() -> Self {
        Self(crossbeam::queue::ArrayQueue::new(8))
    }

    pub(crate) fn watch(
        &self,
        actors: BTreeSet<ActorId>,
        targets: BTreeSet<Target>,
    ) -> Result<mpsc::Receiver<GraphObservation>, ()> {
        self.register(actors, targets, false)
    }

    pub(crate) fn watch_follow_item_births(
        &self,
        actors: BTreeSet<ActorId>,
        targets: BTreeSet<Target>,
    ) -> Result<mpsc::Receiver<GraphObservation>, ()> {
        self.register(actors, targets, true)
    }

    fn register(
        &self,
        actors: BTreeSet<ActorId>,
        targets: BTreeSet<Target>,
        follow_item_births: bool,
    ) -> Result<mpsc::Receiver<GraphObservation>, ()> {
        if actors.is_empty() || actors.len() > 8 || targets.is_empty() || targets.len() > 64 {
            return Err(());
        }
        let (output, input) = mpsc::channel(128);
        self.0
            .push(WatchRequest {
                actors,
                targets,
                follow_item_births,
                output,
            })
            .map_err(|_| ())?;
        Ok(input)
    }
}

struct ActiveWatch {
    request: WatchRequest,
    parents: BTreeSet<ActorId>,
    followed_items: BTreeMap<GlobalEntityId, EntityBirth>,
    signatures: BTreeMap<ActionRef, (u64, Option<u64>, SemanticWrapper, bool)>,
    deferred: VecDeque<GraphObservation>,
    remaining: u16,
}

impl ActiveWatch {
    fn scan(
        &mut self,
        graphs: &ActionGraphs,
        living: Option<&super::cluster_world_actor::OwnedLivingActors>,
    ) -> Result<bool, ()> {
        let mut discovered = false;
        for (tick, graph) in &graphs.ticks {
            for (id, node) in &graph.actions {
                if !self.request.actors.contains(&id.actor) {
                    continue;
                }
                let action = node.semantic_action();
                let followed = match id.actor {
                    ActorId::Entity(entity) => self
                        .followed_items
                        .get(&entity)
                        .map(|birth| (entity, birth)),
                    _ => None,
                };
                let exact_child = followed.is_some_and(|(entity, birth)| {
                    node.local_result
                        .as_ref()
                        .is_some_and(|result| contains_birth(result, entity, birth))
                        || node.is_ground_applied()
                            && node
                                .factual
                                .result
                                .as_ref()
                                .is_some_and(|result| contains_birth(result, entity, birth))
                });
                if followed.is_some() && !exact_child {
                    continue;
                }
                if let Some((entity, birth)) = followed
                    && exact_child
                {
                    for result in node.local_result.iter().chain(
                        node.is_ground_applied()
                            .then_some(node.factual.result.as_ref())
                            .flatten(),
                    ) {
                        for write in &result.writes {
                            if [&write.before, &write.after].into_iter().flatten().any(|cell| matches!(&cell.value, Value::Entity(body) if body.id == entity && &body.birth == birth))
                                && !self.request.targets.contains(&write.target) {
                                if self.request.targets.len() == 64 {
                                    tracing::error!(entity=?entity, "cluster test followed item target overflow");
                                    return Err(());
                                }
                                self.request.targets.insert(write.target);
                            }
                        }
                    }
                }
                if !exact_child
                    && !action.targets().iter().any(|target| {
                        self.request
                            .targets
                            .iter()
                            .any(|scope| matches_target(scope, target))
                    })
                {
                    continue;
                }
                let reference = ActionRef {
                    tick: *tick,
                    id: *id,
                };
                if self.request.follow_item_births
                    && self.parents.contains(&id.actor)
                    && node.local_generation == Some(node.generation)
                    && let Some(applied) = node.local_result.as_ref()
                    && applied.id == *id
                    && applied.tick == *tick
                    && applied.view == TruthView::Local
                {
                    for write in &applied.writes {
                        let Some(after) = &write.after else { continue };
                        let Value::Entity(body) = &after.value else {
                            continue;
                        };
                        if write.before.is_some()
                            || body.birth.action != reference
                            || !matches!(&body.payload, EntityPayload::Item(_))
                        {
                            continue;
                        }
                        if !matches!(write.target, Target::Chunk {part: ChunkPart::Entity(entity), inventory: None, ..} if entity == body.id)
                        {
                            tracing::error!(action=?reference, entity=?body.id, "cluster test item birth target mismatch");
                            return Err(());
                        }
                        if let Some(birth) = self.followed_items.get(&body.id) {
                            if birth != &body.birth {
                                tracing::error!(action=?reference, entity=?body.id, "cluster test followed item birth changed");
                                return Err(());
                            }
                            continue;
                        }
                        let actor = ActorId::Entity(body.id);
                        if self.followed_items.len() == 7
                            || !self.request.actors.contains(&actor)
                                && self.request.actors.len() == 8
                            || !self.request.targets.contains(&write.target)
                                && self.request.targets.len() == 64
                        {
                            tracing::error!(action=?reference, entity=?body.id, "cluster test item follow observation overflow");
                            return Err(());
                        }
                        self.followed_items.insert(body.id, body.birth.clone());
                        self.request.actors.insert(actor);
                        self.request.targets.insert(write.target);
                        discovered = true;
                    }
                }
                let signature = (
                    node.generation,
                    node.local_generation,
                    node.wrapper,
                    node.is_ground_applied(),
                );
                if self.signatures.get(&reference) == Some(&signature) {
                    continue;
                }
                if self.deferred.len() == 256
                    || (!self.signatures.contains_key(&reference) && self.signatures.len() == 256)
                {
                    tracing::error!(action = ?reference, "cluster test target observation overflow");
                    return Err(());
                }
                self.deferred.push_back(GraphObservation {
                    reference,
                    action: action.clone(),
                    wrapper: node.wrapper,
                    factual_result: node.factual.result.clone(),
                    local_result: node.local_result.clone(),
                    generation: node.generation,
                    local_generation: node.local_generation,
                    ground_applied: node.is_ground_applied(),
                    cumulative_ground_living: node
                        .is_ground_applied()
                        .then(|| {
                            living.and_then(|living| {
                                living.phase_state(id.actor, true).map(
                                    super::cluster_test_observation::GroundLivingWitness::capture,
                                )
                            })
                        })
                        .flatten(),
                });
                self.signatures.insert(reference, signature);
            }
        }
        Ok(discovered)
    }
}

#[derive(Default)]
pub(crate) struct TargetWatcher(Vec<ActiveWatch>);

impl TargetWatcher {
    pub(crate) fn observe(
        &mut self,
        queue: &WatchQueue,
        graphs: &ActionGraphs,
        living: Option<&super::cluster_world_actor::OwnedLivingActors>,
    ) {
        while self.0.len() < 8 {
            let Some(request) = queue.0.pop() else { break };
            self.0.push(ActiveWatch {
                parents: request.actors.clone(),
                request,
                followed_items: BTreeMap::new(),
                signatures: BTreeMap::new(),
                deferred: VecDeque::new(),
                remaining: 1200,
            });
        }
        self.0.retain_mut(|watch| {
            if watch.request.output.is_closed() {
                return false;
            }
            match watch.scan(graphs, living) {
                Ok(true) => {
                    if watch.scan(graphs, living).is_err() {
                        return false;
                    }
                }
                Ok(false) => {}
                Err(()) => return false,
            }
            while let Some(observation) = watch.deferred.pop_front() {
                match watch.request.output.try_send(observation) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(observation)) => {
                        watch.deferred.push_front(observation);
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return false,
                }
            }
            watch.remaining -= 1;
            if watch.remaining == 0 {
                tracing::error!(actors=?watch.parents, targets=?watch.request.targets, followed_items=watch.followed_items.len(), "cluster test target observation expired");
                return false;
            }
            true
        });
    }
}

fn contains_birth(applied: &AppliedAction, entity: GlobalEntityId, birth: &EntityBirth) -> bool {
    applied.writes.iter().any(|write| {
        [&write.before, &write.after].into_iter().flatten().any(|cell| {
            matches!(&cell.value, Value::Entity(body) if body.id == entity && &body.birth == birth)
        })
    })
}

fn matches_target(scope: &Target, target: &Target) -> bool {
    scope == target
        || matches!((scope,target),
        (Target::Player {key: expected,lease:None,inventory:expected_inventory},
         Target::Player {key:actual,inventory:actual_inventory,..})
        if expected==actual && expected_inventory==actual_inventory)
}
