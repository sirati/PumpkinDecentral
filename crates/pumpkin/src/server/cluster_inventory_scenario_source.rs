use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{
    ActionProposal, ActionRef, ClaimedAction, InventoryId, LivePlayerLease, SemanticIntent, Target,
};
use pumpkin_cluster::creative_inventory::{CreativeInventoryIntent, CreativeInventoryOperation};
use pumpkin_cluster::identity::{PlayerSeq, ServerId};
use pumpkin_cluster::inventory::{
    INV_SELECTED, InvLoc, InventoryCell, InventoryGesture, InventoryTarget,
};
use pumpkin_cluster::time::TickStamp;
use tokio::sync::{mpsc, oneshot};

use super::Server;
use super::cluster_test_observation::GraphObservation;
use super::cluster_test_scenarios::{
    FixtureLoadoutTemplate, InventoryScenarioCommand, InventoryScenarioOutcome,
    InventoryScenarioPhase, InventoryScenarioRequest,
};

type SourceKey = (LivePlayerLease, TickStamp, PlayerSeq);

enum LoadoutStep {
    EnterCreative(Option<[u8; 16]>),
    Stage,
    Raw(oneshot::Receiver<Option<(TickStamp, PlayerSeq)>>),
    Grant,
    Restore(Option<[u8; 16]>),
    Finished,
}

struct LoadoutCapture {
    step: LoadoutStep,
    mode_target: Target,
    mode_grounded: BTreeMap<ActionRef, u8>,
    creative_requested: bool,
    cleanup: Option<[u8; 16]>,
    request: InventoryScenarioRequest,
    template: FixtureLoadoutTemplate,
    desired: BTreeMap<InvLoc, InventoryCell>,
    source: Option<SourceKey>,
    issued: BTreeSet<ActionRef>,
    observed: BTreeSet<ActionRef>,
    grounded: BTreeSet<ActionRef>,
    pending_observations: BTreeMap<ActionRef, GraphObservation>,
    input: mpsc::Receiver<GraphObservation>,
    captured_published: bool,
    accepted_published: bool,
    failed: bool,
    created: Instant,
}

#[derive(Default)]
pub(crate) struct InventoryScenarioSource {
    active: BTreeMap<[u8; 16], LoadoutCapture>,
    sources: BTreeMap<SourceKey, [u8; 16]>,
    pending_sources: BTreeMap<SourceKey, Vec<ActionRef>>,
}

impl InventoryScenarioSource {
    pub(crate) fn watch_loadout(
        &mut self,
        server: &Server,
        local: ServerId,
        request: InventoryScenarioRequest,
    ) -> Result<(), ()> {
        let InventoryScenarioCommand::Loadout(template) = request.command else {
            return Err(());
        };
        if request.target.gid.server != local
            || self.active.len() == 8
            || self.active.contains_key(&request.token)
            || self
                .active
                .values()
                .any(|capture| capture.request.target == request.target)
        {
            return Err(());
        }
        let cells = template.desired_cells().ok_or(())?;
        let chunk = server
            .cluster_actor_spatial
            .load()
            .get(&request.target.gid)
            .ok_or(())?
            .truth
            .local
            .chunk;
        let mode_target = Target::Chunk {
            key: chunk,
            part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                actor: ActorId::Player(request.target.gid),
                property: pumpkin_cluster::living_property::LivingProperty::GameMode,
            },
            inventory: None,
        };
        let targets = cells
            .iter()
            .map(|cell| Target::Player {
                key: request.target.key,
                lease: Some(request.target),
                inventory: Some(InventoryId::Player {
                    inv: cell.loc.inv,
                    slot: cell.loc.slot,
                }),
            })
            .chain([mode_target])
            .collect();
        let input = server.cluster_test.observations.watches.watch(
            BTreeSet::from([ActorId::Player(request.target.gid)]),
            targets,
        )?;
        self.active.insert(
            request.token,
            LoadoutCapture {
                step: LoadoutStep::EnterCreative(None),
                mode_target,
                mode_grounded: BTreeMap::new(),
                creative_requested: false,
                cleanup: None,
                request,
                template,
                desired: cells.into_iter().map(|cell| (cell.loc, cell)).collect(),
                source: None,
                issued: BTreeSet::new(),
                observed: BTreeSet::new(),
                grounded: BTreeSet::new(),
                pending_observations: BTreeMap::new(),
                input,
                captured_published: false,
                accepted_published: false,
                failed: false,
                created: Instant::now(),
            },
        );
        Ok(())
    }

    pub(crate) fn tracks_inventory_source(&self, proposal: &ActionProposal) -> bool {
        self.active
            .values()
            .any(|capture| source_matches(capture, proposal))
    }

    pub(crate) fn issued_inventory_source(
        &mut self,
        proposal: &ActionProposal,
        references: &[ActionRef],
    ) {
        let Some(lease) = proposal.lease else { return };
        let key = (lease, proposal.tick, PlayerSeq(proposal.id.sequence));
        let Some(capture) = self
            .active
            .values_mut()
            .find(|capture| source_matches(capture, proposal))
        else {
            return;
        };
        let valid = !references.is_empty()
            && references.len() <= 64
            && references.iter().all(|reference| {
                reference.tick == proposal.tick
                    && reference.id.actor == proposal.id.actor
                    && reference.id.incarnation == proposal.id.incarnation
            })
            && references.iter().copied().collect::<BTreeSet<_>>().len() == references.len();
        if !valid || self.pending_sources.contains_key(&key) {
            capture.failed = true;
            tracing::error!(
                ?key,
                "cluster test loadout original source association failed"
            );
            return;
        }
        if capture.source == Some(key) {
            if !capture.issued.is_empty() {
                capture.failed = true;
            } else {
                capture.issued.extend(references.iter().copied());
            }
        } else if capture.source.is_none() {
            if self.pending_sources.len() == 64 {
                capture.failed = true;
                tracing::error!(?key, "cluster test loadout pending source resource failure");
            } else {
                self.pending_sources.insert(key, references.to_vec());
            }
        }
    }

    pub(crate) fn advance(&mut self, server: &Arc<Server>) {
        let mut retired = Vec::new();
        for (token, capture) in &mut self.active {
            if !capture.failed && capture.created.elapsed() >= Duration::from_secs(30) {
                capture.failed = true;
                tracing::error!(?token,target=?capture.request.target,"cluster test loadout source observation expired");
            }
            loop {
                let retained = capture
                    .pending_observations
                    .keys()
                    .find(|reference| capture.issued.contains(reference))
                    .copied();
                let observation = if let Some(reference) = retained {
                    capture.pending_observations.remove(&reference).unwrap()
                } else {
                    match capture.input.try_recv() {
                        Ok(observation) => observation,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            if !capture.failed {
                                tracing::error!(?token, "cluster test loadout observation closed");
                            }
                            capture.failed = true;
                            break;
                        }
                    }
                };
                if observation.ground_applied
                    && observation.local_generation == Some(observation.generation)
                    && observation.action.kind
                        == pumpkin_cluster::canonical_action::ActionKind::PlayerAdministration
                    && observation
                        .action
                        .player_leases
                        .contains(&capture.request.target)
                    && !matches!(
                        observation.wrapper,
                        pumpkin_cluster::action_graph::SemanticWrapper::Undone
                    )
                    && let Some(result) = &observation.factual_result
                {
                    for write in &result.writes {
                        if write.target==capture.mode_target
                            && let Some(pumpkin_cluster::canonical_action::Cell {value:pumpkin_cluster::canonical_action::Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(mode)),..})=&write.after {
                            if *mode>3 || capture.mode_grounded.len()==8 && !capture.mode_grounded.contains_key(&observation.reference) {
                                capture.failed=true;
                                tracing::error!(?token,"cluster test loadout mode observation resource failure");
                            } else {capture.mode_grounded.insert(observation.reference,*mode as u8);}
                        }
                    }
                }
                if capture.failed {
                    continue;
                }
                if !capture.issued.contains(&observation.reference) {
                    if matches!(&observation.action.semantic_intent,Some(SemanticIntent::CreativeInventory(intent)) if exact_intent(intent,capture.request.target,&capture.desired))
                    {
                        if capture.pending_observations.len() == 64
                            && !capture
                                .pending_observations
                                .contains_key(&observation.reference)
                        {
                            capture.failed = true;
                            tracing::error!(
                                ?token,
                                "cluster test loadout pending graph resource failure"
                            );
                        } else {
                            capture
                                .pending_observations
                                .insert(observation.reference, observation);
                        }
                    }
                    continue;
                }
                let valid = observation
                    .action
                    .player_leases
                    .contains(&capture.request.target)
                    && matches!(&observation.action.semantic_intent,Some(SemanticIntent::CreativeInventory(intent)) if exact_intent(intent,capture.request.target,&capture.desired));
                if !valid {
                    capture.failed = true;
                    tracing::error!(?token,action=?observation.reference,"cluster test loadout graph semantic source mismatch");
                    continue;
                }
                capture.observed.insert(observation.reference);
                if observation.ground_applied {
                    if observation.factual_result.is_none()
                        || observation.local_generation != Some(observation.generation)
                        || matches!(
                            observation.wrapper,
                            pumpkin_cluster::action_graph::SemanticWrapper::Undone
                        )
                    {
                        capture.failed = true;
                        tracing::error!(?token,action=?observation.reference,"cluster test loadout source settled without grant");
                    } else {
                        capture.grounded.insert(observation.reference);
                    }
                }
            }
            if capture.failed {
                if !capture.creative_requested {
                    retired.push(*token);
                    continue;
                }
                if capture.cleanup.is_none() {
                    match submit_mode(server, capture, capture.template.final_gamemode()) {
                        Ok(correlation) => capture.cleanup = Some(correlation),
                        Err(()) => {
                            tracing::error!(
                                ?token,
                                "cluster test failed loadout mode cleanup submission failed"
                            );
                            retired.push(*token);
                        }
                    }
                } else if let Some(terminal) = capture
                    .cleanup
                    .and_then(|correlation| server.cluster_player_commands.terminal(correlation))
                {
                    if !terminal.applied || terminal.action.is_none() {
                        tracing::error!(
                            ?token,
                            "cluster test failed loadout mode cleanup rejected"
                        );
                    }
                    retired.push(*token);
                }
                continue;
            }
            if !capture.issued.is_empty() {
                if !capture.captured_published && capture.observed == capture.issued {
                    capture.captured_published =
                        publish(server, capture, InventoryScenarioPhase::Captured);
                }
                if capture.captured_published
                    && !capture.accepted_published
                    && capture.grounded == capture.issued
                {
                    capture.accepted_published =
                        publish(server, capture, InventoryScenarioPhase::Accepted);
                }
            }
            let mut next = None;
            let result: Result<(), ()> = match &mut capture.step {
                LoadoutStep::EnterCreative(correlation) => {
                    if let Some(correlation) = *correlation {
                        match mode_ready(server, capture, correlation, 1) {
                            Ok(Some(_)) => {
                                capture.creative_requested = true;
                                next = Some(LoadoutStep::Stage);
                                Ok(())
                            }
                            Ok(None) => Ok(()),
                            Err(()) => Err(()),
                        }
                    } else {
                        match submit_mode(server, capture, 1) {
                            Ok(correlation) => {
                                capture.creative_requested = true;
                                next = Some(LoadoutStep::EnterCreative(Some(correlation)));
                                Ok(())
                            }
                            Err(()) => Err(()),
                        }
                    }
                }
                LoadoutStep::Stage => {
                    match exact_player(server, capture.request.target).and_then(|player| {
                        super::cluster_inventory_protocol::stage_feature_loadout(
                            &player,
                            &capture.desired.values().cloned().collect::<Vec<_>>(),
                            *token,
                        )
                        .map_err(|_| ())
                    }) {
                        Ok(receipt) => {
                            next = Some(LoadoutStep::Raw(receipt));
                            Ok(())
                        }
                        Err(()) => Err(()),
                    }
                }
                LoadoutStep::Raw(receipt) => match receipt.try_recv() {
                    Ok(Some((tick, sequence))) => {
                        let key = (capture.request.target, tick, sequence);
                        if capture.source.is_some() || self.sources.contains_key(&key) {
                            Err(())
                        } else {
                            capture.source = Some(key);
                            self.sources.insert(key, *token);
                            if let Some(references) = self.pending_sources.remove(&key) {
                                capture.issued.extend(references);
                            }
                            next = Some(LoadoutStep::Grant);
                            Ok(())
                        }
                    }
                    Ok(None) | Err(oneshot::error::TryRecvError::Closed) => Err(()),
                    Err(oneshot::error::TryRecvError::Empty) => Ok(()),
                },
                LoadoutStep::Grant => {
                    if capture.accepted_published {
                        next = Some(LoadoutStep::Restore(None));
                    }
                    Ok(())
                }
                LoadoutStep::Restore(correlation) => {
                    if let Some(correlation) = *correlation {
                        match mode_ready(
                            server,
                            capture,
                            correlation,
                            capture.template.final_gamemode(),
                        ) {
                            Ok(Some(reference)) => {
                                let outcome = InventoryScenarioOutcome {
                                    token: *token,
                                    target: capture.request.target,
                                    template: capture.template,
                                    phase: InventoryScenarioPhase::Restored,
                                    actions: vec![reference],
                                };
                                if server
                                    .cluster_test
                                    .scenarios
                                    .publish_inventory_outcome(outcome)
                                    .is_ok()
                                {
                                    next = Some(LoadoutStep::Finished);
                                }
                                Ok(())
                            }
                            Ok(None) => Ok(()),
                            Err(()) => Err(()),
                        }
                    } else {
                        match submit_mode(server, capture, capture.template.final_gamemode()) {
                            Ok(correlation) => {
                                next = Some(LoadoutStep::Restore(Some(correlation)));
                                Ok(())
                            }
                            Err(()) => Err(()),
                        }
                    }
                }
                LoadoutStep::Finished => {
                    retired.push(*token);
                    Ok(())
                }
            };
            if result.is_err() {
                capture.failed = true;
                tracing::error!(?token,target=?capture.request.target,"cluster test loadout canonical preparation failed");
            }
            if let Some(next) = next {
                capture.step = next;
            }
        }
        for token in retired {
            self.retire(token);
        }
    }

    pub(crate) fn accepted(&self, token: [u8; 16]) -> bool {
        self.active
            .get(&token)
            .is_some_and(|capture| capture.accepted_published && !capture.failed)
    }

    pub(crate) fn retire(&mut self, token: [u8; 16]) {
        if let Some(capture) = self.active.remove(&token) {
            if let Some(source) = capture.source {
                self.sources.remove(&source);
            }
            self.pending_sources
                .retain(|(lease, _, _), _| *lease != capture.request.target);
        }
    }
}

fn source_matches(capture: &LoadoutCapture, proposal: &ActionProposal) -> bool {
    proposal.lease == Some(capture.request.target)
        && proposal.id.actor == ActorId::Player(capture.request.target.gid)
        && proposal.id.incarnation == capture.request.target.incarnation
        && matches!(&proposal.claim, ClaimedAction::Inventory(input) if exact_gestures(&input.gestures,capture.request.target,&capture.desired))
}

fn publish(server: &Server, capture: &LoadoutCapture, phase: InventoryScenarioPhase) -> bool {
    server
        .cluster_test
        .scenarios
        .publish_inventory_outcome(InventoryScenarioOutcome {
            token: capture.request.token,
            target: capture.request.target,
            template: capture.template,
            phase,
            actions: capture.issued.iter().copied().collect(),
        })
        .is_ok()
}

fn exact_gestures(
    gestures: &[InventoryGesture],
    lease: LivePlayerLease,
    desired: &BTreeMap<InvLoc, InventoryCell>,
) -> bool {
    if gestures.len() != desired.len() {
        return false;
    }
    let mut seen = BTreeSet::new();
    gestures.iter().all(|gesture| match gesture {
        InventoryGesture::CreativeSet {
            destination, stack, ..
        } => {
            let exact = match destination.target {
                InventoryTarget::Player(gid) => gid == lease.gid,
                InventoryTarget::LeasedPlayer {
                    key,
                    player,
                    incarnation,
                } => key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation,
                _ => false,
            };
            exact
                && destination.loc.inv != INV_SELECTED
                && desired.get(&destination.loc) == Some(stack)
                && seen.insert(destination.loc)
        }
        InventoryGesture::SelectHotbar { slot, selected } => {
            let exact = match slot.target {
                InventoryTarget::Player(gid) => gid == lease.gid,
                InventoryTarget::LeasedPlayer {
                    key,
                    player,
                    incarnation,
                } => key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation,
                _ => false,
            };
            exact
                && slot.loc == InvLoc::new(INV_SELECTED, 0)
                && desired.get(&slot.loc).is_some_and(|cell| {
                    cell.item == u16::from(*selected) + 1 && cell.count == 1 && cell.nbt.is_empty()
                })
                && seen.insert(slot.loc)
        }
        _ => false,
    }) && seen.len() == desired.len()
}

pub(crate) fn exact_intent(
    intent: &CreativeInventoryIntent,
    lease: LivePlayerLease,
    desired: &BTreeMap<InvLoc, InventoryCell>,
) -> bool {
    if intent.operations.len() != desired.len() {
        return false;
    }
    let mut seen = BTreeSet::new();
    intent.operations.iter().all(|operation| match operation {
        CreativeInventoryOperation::Set { destination, stack } => {
            let Target::Player { key, lease: Some(actual), inventory: Some(InventoryId::Player { inv, slot }) } = destination else { return false; };
            let loc = InvLoc::new(*inv, *slot);
            *key == lease.key && *actual == lease && loc.inv != INV_SELECTED
                && desired.get(&loc) == Some(stack) && seen.insert(loc)
        }
        CreativeInventoryOperation::Conserve(InventoryGesture::SelectHotbar { slot, selected }) => {
            let owner = matches!(slot.target, InventoryTarget::Player(gid) if gid == lease.gid)
                || matches!(slot.target, InventoryTarget::LeasedPlayer { key, player, incarnation }
                    if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation);
            let bound = intent.bindings.iter().any(|binding| &binding.slot == slot
                && binding.target == Target::Player {
                    key: lease.key, lease: Some(lease),
                    inventory: Some(InventoryId::Player { inv: INV_SELECTED, slot: 0 }),
                });
            owner && bound && slot.loc == InvLoc::new(INV_SELECTED, 0)
                && desired.get(&slot.loc).is_some_and(|cell| cell.item == u16::from(*selected) + 1 && cell.count == 1 && cell.nbt.is_empty())
                && seen.insert(slot.loc)
        }
        _ => false,
    }) && seen.len() == desired.len()
}

fn exact_player(
    server: &Server,
    lease: LivePlayerLease,
) -> Result<Arc<crate::entity::player::Player>, ()> {
    server
        .get_all_players()
        .into_iter()
        .find(|player| {
            player.gameprofile.id == lease.key.0
                && super::cluster_world_owner::player_action_lease(server, player) == Some(lease)
        })
        .ok_or(())
}

fn submit_mode(server: &Arc<Server>, capture: &LoadoutCapture, mode: u8) -> Result<[u8; 16], ()> {
    use crate::command::{CommandSender, CommandSource};
    let target = exact_player(server, capture.request.target)?;
    let (sender, entity) = match capture.request.issuer {
        super::cluster_test_scenarios::ScenarioIssuer::Console => (CommandSender::Console, None),
        super::cluster_test_scenarios::ScenarioIssuer::Player(lease) => {
            let issuer = exact_player(server, lease)?;
            (
                CommandSender::Player(Arc::clone(&issuer)),
                Some(issuer as Arc<dyn crate::entity::EntityBase>),
            )
        }
    };
    let source = CommandSource::new(
        sender,
        target.world(),
        entity,
        Default::default(),
        Default::default(),
        "cluster-e2e-loadout".to_owned(),
        pumpkin_util::text::TextComponent::text("cluster-e2e-loadout"),
        Arc::clone(server),
    );
    super::cluster_gamemode_commands::submit(server, &source, capture.request.target, mode, None)
        .map_err(|_| ())
}

fn mode_ready(
    server: &Server,
    capture: &LoadoutCapture,
    correlation: [u8; 16],
    mode: u8,
) -> Result<Option<ActionRef>, ()> {
    let Some(terminal) = server.cluster_player_commands.terminal(correlation) else {
        return Ok(None);
    };
    if !terminal.applied {
        return Err(());
    }
    let reference = terminal.action.ok_or(())?;
    match capture.mode_grounded.get(&reference) {
        Some(actual) if *actual == mode => Ok(Some(reference)),
        Some(_) => Err(()),
        None => Ok(None),
    }
}
