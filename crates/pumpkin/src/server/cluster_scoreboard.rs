use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::streams::InboundParcel;
use tokio::sync::mpsc;

use super::cluster_control_registers::{self as control, Frame, Registers};
use crate::world::scoreboard::Scoreboard;

pub const PREFIX: &[u8; 8] = b"PScore01";

pub struct ScoreboardUpdate {
    pub world: String,
    pub changes: Vec<(Vec<String>, Option<serde_json::Value>)>,
    pub semantic: Option<ScoreMutation>,
    pub feedback: Option<Arc<crate::command::CommandSource>>,
    pub completion:
        Option<pumpkin_command::deferred::DeferredCommandCompletion<crate::command::CommandSource>>,
}

impl ScoreboardUpdate {
    fn try_finish(
        mut self,
        result: Result<i32, &'static str>,
    ) -> Result<(), (Self, Result<i32, &'static str>)> {
        if let Some(source) = self.feedback.take() {
            match result {
                Ok(value) => source.send_feedback(
                    pumpkin_util::text::TextComponent::text(format!(
                        "Scoreboard operation completed: {value}"
                    )),
                    true,
                ),
                Err(reason) => source.send_error(pumpkin_util::text::TextComponent::text(reason)),
            }
        }
        if let Some(completion) = self.completion.take() {
            let value = match result {
                Ok(value) => pumpkin_command::source::ReturnValue::Success(value),
                Err(_) => pumpkin_command::source::ReturnValue::Failure,
            };
            if let Err((completion, value)) = completion.try_finish(value) {
                self.completion = Some(completion);
                let result = match value {
                    pumpkin_command::source::ReturnValue::Success(value) => Ok(value),
                    pumpkin_command::source::ReturnValue::Failure => {
                        Err(result.err().unwrap_or("Scoreboard operation failed"))
                    }
                };
                return Err((self, result));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum ScoreOperation {
    Assign,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Minimum,
    Maximum,
    Swap,
}

#[derive(Clone, Debug)]
pub enum ScoreAction {
    Delta(i32),
    Set(i32),
    Operation {
        operation: ScoreOperation,
        sources: Vec<String>,
        objective: String,
    },
    Trigger {
        value: i32,
        add: bool,
    },
}

#[derive(Clone, Debug)]
pub struct ScoreMutation {
    pub targets: Vec<String>,
    pub objective: String,
    pub action: ScoreAction,
}

impl ScoreMutation {
    fn valid(&self) -> bool {
        let mut bytes = self.objective.len();
        if self.objective.is_empty()
            || self.objective.len() > 4096
            || self.targets.is_empty()
            || self.targets.len() > 4096
        {
            return false;
        }
        for name in &self.targets {
            if name.is_empty() || name.len() > 4096 {
                return false;
            }
            bytes += name.len() + 256;
        }
        if let ScoreAction::Operation {
            sources, objective, ..
        } = &self.action
        {
            if sources.is_empty()
                || self.targets.len().saturating_mul(sources.len()) > 4096
                || objective.is_empty()
                || objective.len() > 4096
            {
                return false;
            }
            for source in sources {
                if source.is_empty() || source.len() > 4096 {
                    return false;
                }
                bytes += source.len() + 256;
            }
            bytes += objective.len();
        }
        bytes <= 700_000
    }

    fn execute(&self, board: &mut Scoreboard) -> Result<i32, &'static str> {
        if !self.valid() {
            return Err("Invalid scoreboard operation");
        }
        let objective = board
            .get_objective(&self.objective)
            .ok_or("Scoreboard objective no longer exists")?;
        if matches!(self.action, ScoreAction::Trigger { .. }) && objective.criterion != "trigger" {
            return Err("Invalid trigger objective");
        }
        let mut next = board.clone();
        let mut result = 0i32;
        for target in &self.targets {
            let sources = match &self.action {
                ScoreAction::Operation { sources, .. } => {
                    sources.iter().map(Some).collect::<Vec<_>>()
                }
                _ => vec![None],
            };
            for source in sources {
                let current = next.get_score_value(target, &self.objective).unwrap_or(0);
                let value = match &self.action {
                    ScoreAction::Delta(delta) => current.wrapping_add(*delta),
                    ScoreAction::Set(value) => *value,
                    ScoreAction::Trigger { value, add } => {
                        if next
                            .get_score(target, &self.objective)
                            .is_none_or(|score| score.locked)
                        {
                            return Err("Trigger objective is not enabled");
                        }
                        if *add {
                            current.wrapping_add(*value)
                        } else {
                            *value
                        }
                    }
                    ScoreAction::Operation {
                        operation,
                        objective,
                        ..
                    } => {
                        let source = source.unwrap();
                        if next.get_objective(objective).is_none() {
                            return Err("Source scoreboard objective no longer exists");
                        }
                        let rhs = next.get_score_value(source, objective).unwrap_or(0);
                        if next.get_score(source, objective).is_none() {
                            next.set_score_value(
                                &crate::world::scoreboard::NoTarget,
                                source,
                                objective,
                                0,
                            );
                        }
                        match operation {
                            ScoreOperation::Assign => rhs,
                            ScoreOperation::Add => current.wrapping_add(rhs),
                            ScoreOperation::Subtract => current.wrapping_sub(rhs),
                            ScoreOperation::Multiply => current.wrapping_mul(rhs),
                            ScoreOperation::Divide => {
                                if rhs == 0 {
                                    return Err("Cannot divide a scoreboard score by zero");
                                }
                                let quotient = current.wrapping_div(rhs);
                                if current.wrapping_rem(rhs) != 0 && (current < 0) != (rhs < 0) {
                                    quotient.wrapping_sub(1)
                                } else {
                                    quotient
                                }
                            }
                            ScoreOperation::Modulo => {
                                if rhs == 0 {
                                    return Err("Cannot divide a scoreboard score by zero");
                                }
                                let remainder = current.wrapping_rem(rhs);
                                if remainder != 0 && (remainder < 0) != (rhs < 0) {
                                    remainder.wrapping_add(rhs)
                                } else {
                                    remainder
                                }
                            }
                            ScoreOperation::Minimum => current.min(rhs),
                            ScoreOperation::Maximum => current.max(rhs),
                            ScoreOperation::Swap => {
                                next.set_score_value(
                                    &crate::world::scoreboard::NoTarget,
                                    source,
                                    objective,
                                    current,
                                );
                                rhs
                            }
                        }
                    }
                };
                next.set_score_value(
                    &crate::world::scoreboard::NoTarget,
                    target,
                    &self.objective,
                    value,
                );
                if matches!(self.action, ScoreAction::Trigger { .. }) {
                    let mut score = next.get_score(target, &self.objective).unwrap().clone();
                    score.locked = true;
                    next.update_score(&crate::world::scoreboard::NoTarget, score);
                }
            }
            result =
                result.wrapping_add(next.get_score_value(target, &self.objective).unwrap_or(0));
        }
        *board = next;
        Ok(result)
    }
}

fn prepare_update(
    registers: &Registers,
    update: &ScoreboardUpdate,
) -> Result<(Vec<(Vec<String>, Option<serde_json::Value>)>, i32), &'static str> {
    if !valid_update(update) {
        return Err("Invalid scoreboard mutation");
    }
    if let Some(semantic) = &update.semantic {
        let worlds = materialize(registers);
        let before =
            Scoreboard::from_records(worlds.get(&update.world).unwrap_or(&BTreeMap::new()));
        let mut after = before.clone();
        let result = semantic.execute(&mut after)?;
        let changes = after.changes_from(&before);
        let checked = ScoreboardUpdate {
            world: update.world.clone(),
            changes,
            semantic: None,
            feedback: None,
            completion: None,
        };
        if !valid_update(&checked) {
            return Err("Scoreboard mutation exceeds admitted result capacity");
        }
        Ok((checked.changes, result))
    } else {
        Ok((update.changes.clone(), 0))
    }
}

fn apply_local(
    registers: &mut Registers,
    update: &ScoreboardUpdate,
    origin: u16,
    incarnation: u64,
) -> Result<(Vec<control::Register>, i32), &'static str> {
    let (changes, result) = prepare_update(registers, update)?;
    let entries = changes
        .into_iter()
        .map(|(mut key, value)| {
            key.insert(0, update.world.clone());
            let entry = registers.issue(
                origin,
                incarnation,
                serde_json::to_string(&key).unwrap(),
                value,
            );
            registers.merge(entry.clone());
            entry
        })
        .collect();
    Ok((entries, result))
}

pub(crate) fn valid_update(update: &ScoreboardUpdate) -> bool {
    if update
        .semantic
        .as_ref()
        .is_some_and(|semantic| !semantic.valid())
        || update.semantic.is_some() && !update.changes.is_empty()
        || update.changes.len() > 4096
    {
        return false;
    }
    let mut bytes = 0;
    for (key, value) in &update.changes {
        let mut key = key.clone();
        key.insert(0, update.world.clone());
        let entry = control::Register {
            key: serde_json::to_string(&key).unwrap(),
            stamp: control::Stamp {
                millis: i64::MAX as u64,
                origin: u16::MAX,
                incarnation: u64::MAX,
                ordinal: u64::MAX,
            },
            value: value.clone(),
        };
        if !valid_register(&entry) {
            return false;
        }
        bytes += serde_json::to_vec(&entry).map_or(900_000, |bytes| bytes.len());
        if bytes > 800_000 {
            return false;
        }
    }
    true
}

pub fn receive(server: &super::Server, parcel: InboundParcel) -> Result<(), InboundParcel> {
    let Some(tx) = server.cluster_scoreboard_inbound.load_full() else {
        return Err(parcel);
    };
    tx.try_send(parcel).map_err(|error| error.into_inner())
}

fn valid_register(entry: &control::Register) -> bool {
    let Ok(key) = serde_json::from_str::<Vec<String>>(&entry.key) else {
        return false;
    };
    if entry.stamp.ordinal == 0
        || entry.stamp.millis > i64::MAX as u64
        || key.len() < 3
        || key.len() > 4
        || key.iter().any(|part| part.len() > 4096)
    {
        return false;
    }
    if serde_json::to_vec(&entry.value).map_or(true, |value| value.len() > 65536) {
        return false;
    }
    serde_json::to_string(&key).is_ok_and(|canonical| canonical == entry.key)
        && Scoreboard::valid_record(&key[1..], entry.value.as_ref())
}

fn materialize(
    registers: &Registers,
) -> BTreeMap<String, BTreeMap<Vec<String>, serde_json::Value>> {
    let mut worlds = BTreeMap::<String, BTreeMap<Vec<String>, serde_json::Value>>::new();
    for entry in registers.entries() {
        let Ok(mut key) = serde_json::from_str::<Vec<String>>(&entry.key) else {
            continue;
        };
        let world = key.remove(0);
        let records = worlds.entry(world).or_default();
        if let Some(value) = &entry.value {
            records.insert(key, value.clone());
        }
    }
    worlds
}

pub fn spawn(
    server: &Arc<super::Server>,
    mut updates: mpsc::Receiver<ScoreboardUpdate>,
    mut custom: mpsc::Receiver<crate::world::scoreboard::CustomScoreboardUpdate>,
) {
    let (tx, mut rx) = mpsc::channel::<InboundParcel>(256);
    server.cluster_scoreboard_inbound.store(Some(Arc::new(tx)));
    let persist = !server.advanced_config.cluster.enabled
        || matches!(
            server.advanced_config.cluster.role,
            pumpkin_config::ClusterRole::Primary
        );
    let local = server.advanced_config.cluster.server_id;
    let path = std::path::PathBuf::from("data/cluster-scoreboard-registers.json");
    let mut registers = Registers::default();
    if persist {
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<Registers>(&bytes) {
                Ok(saved) if saved.entries().all(valid_register) => registers = saved,
                _ => {
                    tracing::error!("invalid persisted scoreboard controls");
                    return;
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::error!(%error, "scoreboard controls could not be loaded");
                return;
            }
        }
    }
    let (save_tx, mut save_rx) = mpsc::channel::<Registers>(1);
    if persist {
        server.spawn_task(async move {
            while let Some(registers) = save_rx.recv().await {
                let path = path.clone();
                match tokio::task::spawn_blocking(move || control::save(&path, &registers)).await {
                    Ok(Ok(())) => {}
                    result => tracing::error!(?result, "scoreboard control persistence failed"),
                }
            }
        });
    }
    let weak = Arc::downgrade(server);
    server.spawn_task(async move {
        let mut outgoing =
            VecDeque::<(Option<pumpkin_cluster::identity::ServerId>, Vec<u8>)>::new();
        let mut peers = HashMap::new();
        let mut seeds = VecDeque::<pumpkin_cluster::identity::ServerId>::new();
        let mut assemblies = control::SeedAssemblies::default();
        let mut seed_backlog =
            VecDeque::<([u8; 16], pumpkin_cluster::streams::InboundParcel)>::new();
        let mut pending_save = None;
        let mut pending_completion: Option<(ScoreboardUpdate, Result<i32, &'static str>)> = None;
        let mut published = BTreeMap::new();
        let mut materialized = BTreeMap::new();
        let mut diagnostics = control::Diagnostics::default();
        let mut pending_inbound: Option<pumpkin_cluster::streams::InboundParcel> = None;
        let mut deferred_inbound = VecDeque::<(pumpkin_cluster::streams::InboundParcel, std::time::Instant)>::new();
        let mut shutdown_started = false;
        let mut dirty = true;
        let mut drained = false;
        let mut custom_drained = false;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            let Some(server) = weak.upgrade() else {
                break;
            };
            let shutting_down = server
                .cluster_global_shutdown
                .load(std::sync::atomic::Ordering::Acquire);
            let local_boot_ready = !server.advanced_config.cluster.enabled
                || server
                    .cluster_peer_incarnations
                    .load()
                    .get(&pumpkin_cluster::identity::ServerId(local))
                    .is_some_and(|boot| *boot != 0);
            let inbound_ready = pending_inbound.as_ref().is_none_or(|parcel| control::control_publication_ready(parcel.generation, server.cluster_peer_generations.load().get(&parcel.peer).copied(), server.cluster_peer_incarnations.load().get(&parcel.peer).copied()));
            if !inbound_ready { deferred_inbound.push_back((pending_inbound.take().unwrap(), std::time::Instant::now())); }
            if shutting_down && !shutdown_started {
                shutdown_started = true;
                updates.close();
                custom.close();
                rx.close();
                seeds.clear();
                outgoing.retain(|(_, bytes)| {
                    !matches!(control::decode(PREFIX, bytes), Some(Frame::Seed { .. }))
                });
            }
            tokio::select! {
                update = custom.recv(), if !custom_drained => {
                    if let Some(update) = update { crate::world::scoreboard::apply_custom_scoreboard(&server, update); } else { custom_drained = true; }
                }
                update = updates.recv(), if !drained && pending_completion.is_none() && (shutting_down || local_boot_ready && outgoing.len() < 4096 && !dirty) => {
                    if let Some(update) = update {
                        let incarnation = server.cluster_peer_incarnations.load().get(&ServerId(local)).copied().unwrap_or(0);
                        let (entries, result) = match apply_local(&mut registers, &update, local, incarnation) {
                            Ok(prepared) => prepared,
                            Err(reason) => {
                                if update.feedback.is_none() { diagnostics.rejected("scoreboard", local, reason); }
                                pending_completion = update.try_finish(Err(reason)).err();
                                continue;
                            }
                        };
                        if !entries.is_empty() {
                            let bytes = control::encode(PREFIX, &Frame::Mutation(entries)).expect("admitted scoreboard control fits a frame");
                            outgoing.push_back((None, bytes));
                            dirty = true;
                        }
                        pending_completion = update.try_finish(Ok(result)).err();
                    } else { drained = true; }
                }
                parcel = async {
                    if let Some(parcel) = pending_inbound.take() { Some(parcel) } else { rx.recv().await }
                }, if !shutting_down && (pending_inbound.is_some() || seed_backlog.len() < 256 && deferred_inbound.len() < 256) && outgoing.len() < 4096 && !dirty => {
                    let Some(parcel) = parcel else { continue; };
                    let generation = server.cluster_peer_generations.load().get(&parcel.peer).copied();
                    if generation.is_none_or(|generation| generation < parcel.generation) { deferred_inbound.push_back((parcel, std::time::Instant::now())); continue; }
                    if server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation) { diagnostics.rejected("scoreboard", parcel.peer.0, "stale transport"); continue; }
                    let Some(incarnation) = server.cluster_peer_incarnations.load().get(&parcel.peer).copied().filter(|boot| *boot != 0) else { deferred_inbound.push_back((parcel, std::time::Instant::now())); continue; };
                    match control::decode(PREFIX, &parcel.bytes) {
                        Some(Frame::Mutation(entries)) => {
                            if let Some(changed) = registers.merge_authenticated(parcel.peer.0, incarnation, entries, valid_register) { dirty |= changed; } else { diagnostics.rejected("scoreboard", parcel.peer.0, "invalid mutation"); }
                        }
                        Some(Frame::Seed { transfer, index, total, entries }) if total > 0 && total <= 64 && index < total && entries.iter().all(valid_register) => {
                            match assemblies.push(parcel.peer.0, parcel.generation, transfer, index, total, entries) {
                                control::SeedResult::Pending => continue,
                                control::SeedResult::Full => { seed_backlog.push_back((transfer, parcel)); continue; }
                                control::SeedResult::Rejected(reason) => { diagnostics.rejected("scoreboard", parcel.peer.0, reason); continue; }
                                control::SeedResult::Complete(entries) => {
                                let Some(changed) = registers.merge_reconciled(entries, valid_register) else { diagnostics.rejected("scoreboard", parcel.peer.0, "contradictory seed"); continue; };
                                dirty |= changed;
                                if changed {
                                    for peer in server.cluster_connected_peers.load().iter() {
                                        if *peer != parcel.peer && seeds.len() < 256 && !seeds.contains(peer) { seeds.push_back(*peer); }
                                    }
                                }
                                }
                            }
                        }
                        _ => { diagnostics.rejected("scoreboard", parcel.peer.0, "malformed frame"); },
                    }
                }
                _ = interval.tick() => {
                    if dirty { materialized = materialize(&registers); }
                    for world in server.worlds.load().iter() {
                        let key = format!("{}:{}", world.dimension.id, world.get_world_name());
                        let desired = materialized.get(&key);
                        if published.get(&world.uuid) != desired {
                            let desired = desired.cloned().unwrap_or_default();
                            world.scoreboard.install(world, Scoreboard::from_records(&desired));
                            published.insert(world.uuid, desired);
                        }
                    }
                    if dirty { if persist { pending_save = Some(registers.clone()); } dirty = false; }
                    if let Some(save) = pending_save.take() { if let Err(error) = save_tx.try_send(save) { pending_save = Some(error.into_inner()); } }
                    if let Some((update, result)) = pending_completion.take() { pending_completion = update.try_finish(result).err(); }
                    if shutting_down && drained && custom_drained && pending_completion.is_none() && pending_save.is_none() && outgoing.is_empty() { break; }
                    let connected = server.cluster_connected_peers.load();
                    let generations = server.cluster_peer_generations.load();
                    for peer in connected.iter() {
                        let generation = generations.get(peer).copied();
                        if peers.get(peer) != Some(&generation) && seeds.len() < 256 {
                            if !seeds.contains(peer) { seeds.push_back(*peer); }
                            peers.insert(*peer, generation);
                        }
                    }
                    peers.retain(|peer, _| connected.contains(peer));
                    assemblies.retain(|peer, generation| generations.get(&pumpkin_cluster::identity::ServerId(peer)) == Some(&generation));
                    for (parcel, since) in &deferred_inbound {
                        if since.elapsed().as_secs() >= 10 { diagnostics.deferred("scoreboard", parcel.peer.0, "authenticated source publication unavailable"); }
                    }
                    if pending_inbound.is_none() {
                        let boots = server.cluster_peer_incarnations.load();
                        pending_inbound = control::take_ready_control(&mut deferred_inbound, |(parcel, _)| control::control_publication_ready(parcel.generation, generations.get(&parcel.peer).copied(), boots.get(&parcel.peer).copied())).map(|(parcel, _)| parcel);
                    }
                    if pending_inbound.is_none() && deferred_inbound.len() < 256 {
                        if let Some(index) = seed_backlog.iter().position(|(transfer, parcel)|
                            generations.get(&parcel.peer).is_some_and(|generation| *generation > parcel.generation)
                                || assemblies.can_accept(parcel.peer.0, parcel.generation, *transfer)) {
                            pending_inbound = Some(seed_backlog.remove(index).unwrap().1);
                        }
                    }
                    if !shutting_down && outgoing.len() < 4032 {
                        if let Some(peer) = seeds.pop_front() {
                            if connected.contains(&peer) {
                                if let Some(frames) = control::seed_frames(PREFIX, &registers) { outgoing.extend(frames.into_iter().map(|bytes| (Some(peer), bytes))); }
                            }
                        }
                    }
                    for _ in 0..16 {
                        let Some((peer, bytes)) = outgoing.front() else { break; };
                        let targets = match peer { Some(peer) if connected.contains(peer) => vec![*peer], Some(_) => { outgoing.pop_front(); continue; }, None => connected.iter().copied().collect() };
                        if targets.is_empty() || server.cluster_global_outbound.try_event(&server, targets, bytes.clone()) { outgoing.pop_front(); } else { break; }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::scoreboard::ScoreboardBuilder;
    use pumpkin_util::text::TextComponent;

    fn frame(
        issuer: &mut Registers,
        world: &str,
        before: &Scoreboard,
        after: &Scoreboard,
    ) -> Vec<u8> {
        let update = ScoreboardUpdate {
            world: world.into(),
            changes: after.changes_from(before),
            semantic: None,
            feedback: None,
            completion: None,
        };
        assert!(valid_update(&update));
        let entries = update
            .changes
            .into_iter()
            .map(|(mut key, value)| {
                key.insert(0, update.world.clone());
                issuer.issue(2, 11, serde_json::to_string(&key).unwrap(), value)
            })
            .collect();
        control::encode(PREFIX, &Frame::Mutation(entries)).unwrap()
    }

    fn receive_frame(replica: &mut Registers, bytes: &[u8]) -> bool {
        let Some(Frame::Mutation(entries)) = control::decode(PREFIX, bytes) else {
            panic!("invalid scoreboard frame")
        };
        replica
            .merge_authenticated(2, 11, entries, valid_register)
            .unwrap()
    }

    #[test]
    fn admitted_diff_crosses_wire_to_world_scoped_snapshot_and_retains_deletion() {
        let empty = Scoreboard::new();
        let first = ScoreboardBuilder::new()
            .objective("kills", TextComponent::text("Kills"))
            .score("Alice", "kills", 7)
            .build();
        let other = ScoreboardBuilder::new()
            .objective("kills", TextComponent::text("Kills"))
            .score("Alice", "kills", 99)
            .build();
        let mut issuer = Registers::default();
        let original = frame(&mut issuer, "0:overworld", &empty, &first);
        let second_world = frame(&mut issuer, "2:end", &empty, &other);
        let deletion = frame(&mut issuer, "0:overworld", &first, &empty);
        let mut replica = Registers::default();
        assert!(receive_frame(&mut replica, &second_world));
        assert!(receive_frame(&mut replica, &original));
        let snapshots = materialize(&replica);
        assert_eq!(
            Scoreboard::from_records(&snapshots["0:overworld"]).get_score_value("Alice", "kills"),
            Some(7)
        );
        assert_eq!(
            Scoreboard::from_records(&snapshots["2:end"]).get_score_value("Alice", "kills"),
            Some(99)
        );
        assert!(receive_frame(&mut replica, &deletion));
        assert!(!receive_frame(&mut replica, &original));
        let snapshots = materialize(&replica);
        assert!(
            Scoreboard::from_records(&snapshots["0:overworld"])
                .get_objective("kills")
                .is_none()
        );
        assert_eq!(
            Scoreboard::from_records(&snapshots["2:end"]).get_score_value("Alice", "kills"),
            Some(99)
        );
    }
    fn semantic(action: ScoreAction) -> ScoreboardUpdate {
        ScoreboardUpdate {
            world: "0:overworld".into(),
            changes: Vec::new(),
            semantic: Some(ScoreMutation {
                targets: vec!["Alice".into()],
                objective: "kills".into(),
                action,
            }),
            feedback: None,
            completion: None,
        }
    }

    fn initial_scores() -> Registers {
        let board = ScoreboardBuilder::new()
            .objective("kills", TextComponent::text("Kills"))
            .score("Alice", "kills", 7)
            .score("Bob", "kills", 2)
            .build();
        let mut issuer = Registers::default();
        let bytes = frame(&mut issuer, "0:overworld", &Scoreboard::new(), &board);
        let mut registers = Registers::default();
        receive_frame(&mut registers, &bytes);
        registers
    }

    fn board(registers: &Registers) -> Scoreboard {
        Scoreboard::from_records(&materialize(registers)["0:overworld"])
    }

    #[test]
    fn admitted_relative_additions_execute_against_owned_state_and_survive_wire_restart() {
        let mut issuer = initial_scores();
        let mut replica = issuer.clone();
        let first = semantic(ScoreAction::Delta(1));
        let second = semantic(ScoreAction::Delta(1));
        let (first_entries, first_value) = apply_local(&mut issuer, &first, 2, 11).unwrap();
        let (second_entries, second_value) = apply_local(&mut issuer, &second, 2, 11).unwrap();
        assert_eq!((first_value, second_value), (8, 9));
        let first_bytes = control::encode(PREFIX, &Frame::Mutation(first_entries)).unwrap();
        let second_bytes = control::encode(PREFIX, &Frame::Mutation(second_entries)).unwrap();
        receive_frame(&mut replica, &second_bytes);
        assert!(!receive_frame(&mut replica, &first_bytes));
        let restored: Registers =
            serde_json::from_slice(&serde_json::to_vec(&replica).unwrap()).unwrap();
        assert_eq!(board(&restored).get_score_value("Alice", "kills"), Some(9));
        assert_eq!(board(&restored).get_score_value("Bob", "kills"), Some(2));
    }

    #[test]
    fn operation_uses_latest_owned_rhs_and_preserves_swap_and_floor_arithmetic() {
        let mut registers = initial_scores();
        let mut change_source = semantic(ScoreAction::Delta(3));
        change_source.semantic.as_mut().unwrap().targets = vec!["Bob".into()];
        let operation = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Add,
            sources: vec!["Bob".into()],
            objective: "kills".into(),
        });
        apply_local(&mut registers, &change_source, 2, 11).unwrap();
        assert_eq!(
            apply_local(&mut registers, &operation, 2, 11).unwrap().1,
            12
        );
        let swap = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Swap,
            sources: vec!["Bob".into()],
            objective: "kills".into(),
        });
        apply_local(&mut registers, &swap, 2, 11).unwrap();
        assert_eq!(board(&registers).get_score_value("Alice", "kills"), Some(5));
        assert_eq!(board(&registers).get_score_value("Bob", "kills"), Some(12));
        apply_local(&mut registers, &semantic(ScoreAction::Set(-7)), 2, 11).unwrap();
        let division = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Divide,
            sources: vec!["Bob".into()],
            objective: "kills".into(),
        });
        assert_eq!(apply_local(&mut registers, &division, 2, 11).unwrap().1, -1);
        let modulo = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Modulo,
            sources: vec!["Bob".into()],
            objective: "kills".into(),
        });
        assert_eq!(apply_local(&mut registers, &modulo, 2, 11).unwrap().1, 11);
    }

    #[test]
    fn changed_rhs_zero_and_deleted_objective_fail_without_partial_result_registers() {
        let mut registers = initial_scores();
        let division = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Divide,
            sources: vec!["Bob".into()],
            objective: "kills".into(),
        });
        let mut zero = semantic(ScoreAction::Set(0));
        zero.semantic.as_mut().unwrap().targets = vec!["Bob".into()];
        apply_local(&mut registers, &zero, 2, 11).unwrap();
        let before = serde_json::to_vec(&registers).unwrap();
        assert!(apply_local(&mut registers, &division, 2, 11).is_err());
        assert_eq!(serde_json::to_vec(&registers).unwrap(), before);
        let deletion = ScoreboardUpdate {
            world: "0:overworld".into(),
            changes: Scoreboard::new().changes_from(&board(&registers)),
            semantic: None,
            feedback: None,
            completion: None,
        };
        apply_local(&mut registers, &deletion, 2, 11).unwrap();
        assert!(apply_local(&mut registers, &semantic(ScoreAction::Delta(1)), 2, 11).is_err());
        assert!(board(&registers).get_objective("kills").is_none());
    }

    #[test]
    fn trigger_lock_is_revalidated_between_already_admitted_commands() {
        let mut initial = Scoreboard::new();
        initial.add_objective(
            &crate::world::scoreboard::NoTarget,
            crate::world::scoreboard::ScoreboardObjective::new(
                "kills",
                TextComponent::text("Trigger"),
                pumpkin_protocol::java::client::play::RenderType::Integer,
                None,
                "trigger",
            ),
        );
        let mut score = crate::world::scoreboard::ScoreboardScore::new(
            "Alice",
            "kills",
            pumpkin_protocol::codec::var_int::VarInt(7),
            None,
            None,
        );
        score.locked = false;
        initial.update_score(&crate::world::scoreboard::NoTarget, score);
        let mut registers = Registers::default();
        let update = ScoreboardUpdate {
            world: "0:overworld".into(),
            changes: initial.changes_from(&Scoreboard::new()),
            semantic: None,
            feedback: None,
            completion: None,
        };
        apply_local(&mut registers, &update, 2, 11).unwrap();
        let trigger = semantic(ScoreAction::Trigger {
            value: 1,
            add: true,
        });
        assert_eq!(apply_local(&mut registers, &trigger, 2, 11).unwrap().1, 8);
        assert!(apply_local(&mut registers, &trigger, 2, 11).is_err());
        assert_eq!(board(&registers).get_score_value("Alice", "kills"), Some(8));
        assert!(
            board(&registers)
                .get_score("Alice", "kills")
                .unwrap()
                .locked
        );
    }
    #[test]
    fn every_operation_uses_integer_score_semantics_and_multi_source_failure_is_atomic() {
        for (operation, expected) in [
            (ScoreOperation::Assign, 2),
            (ScoreOperation::Add, 9),
            (ScoreOperation::Subtract, 5),
            (ScoreOperation::Multiply, 14),
            (ScoreOperation::Divide, 3),
            (ScoreOperation::Modulo, 1),
            (ScoreOperation::Minimum, 2),
            (ScoreOperation::Maximum, 7),
            (ScoreOperation::Swap, 2),
        ] {
            let mut registers = initial_scores();
            let update = semantic(ScoreAction::Operation {
                operation,
                sources: vec!["Bob".into()],
                objective: "kills".into(),
            });
            assert_eq!(
                apply_local(&mut registers, &update, 2, 11).unwrap().1,
                expected
            );
        }
        let mut registers = initial_scores();
        let mut zero = semantic(ScoreAction::Set(0));
        zero.semantic.as_mut().unwrap().targets = vec!["Carol".into()];
        apply_local(&mut registers, &zero, 2, 11).unwrap();
        let before = serde_json::to_vec(&registers).unwrap();
        let update = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Divide,
            sources: vec!["Bob".into(), "Carol".into()],
            objective: "kills".into(),
        });
        assert!(apply_local(&mut registers, &update, 2, 11).is_err());
        assert_eq!(serde_json::to_vec(&registers).unwrap(), before);
        let update = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Add,
            sources: vec!["Bob".into(), "Bob".into()],
            objective: "kills".into(),
        });
        assert_eq!(apply_local(&mut registers, &update, 2, 11).unwrap().1, 11);
        assert_eq!(
            apply_local(
                &mut registers,
                &semantic(ScoreAction::Delta(i32::MAX)),
                2,
                11
            )
            .unwrap()
            .1,
            i32::MIN + 10
        );
    }
    #[test]
    fn owned_command_completion_stores_actual_sequential_results_and_failure_once() {
        struct Capture(tokio::sync::mpsc::Sender<pumpkin_command::source::ReturnValue>);
        impl pumpkin_command::source::ReturnValueCallable for Capture {
            fn call(&self, value: pumpkin_command::source::ReturnValue) {
                self.0.try_send(value).unwrap();
            }
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel(3);
        let mut source = crate::command::CommandSource::dummy();
        source.command_result_taker =
            pumpkin_command::source::ResultValueTaker(vec![Arc::new(Capture(tx))]);
        let source = Arc::new(source);
        let mut registers = initial_scores();
        let mut queued = VecDeque::new();
        for ordinal in 1..=2 {
            let mut update = semantic(ScoreAction::Delta(1));
            update.feedback = Some(Arc::clone(&source));
            update.completion = Some(
                pumpkin_command::deferred::DeferredCommandCompletion::reserve(
                    Arc::clone(&source),
                    pumpkin_command::deferred::DeferredCommandToken([ordinal; 16]),
                )
                .unwrap(),
            );
            queued.push_back(update);
        }
        assert!(rx.try_recv().is_err());
        while let Some(update) = queued.pop_front() {
            let outcome = apply_local(&mut registers, &update, 2, 11).map(|(_, value)| value);
            assert!(update.try_finish(outcome).is_ok());
        }
        assert_eq!(
            rx.try_recv().unwrap(),
            pumpkin_command::source::ReturnValue::Success(8)
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            pumpkin_command::source::ReturnValue::Success(9)
        );
        let mut failure = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Divide,
            sources: vec!["missing".into()],
            objective: "kills".into(),
        });
        failure.feedback = Some(Arc::clone(&source));
        failure.completion = Some(
            pumpkin_command::deferred::DeferredCommandCompletion::reserve(
                source,
                pumpkin_command::deferred::DeferredCommandToken([3; 16]),
            )
            .unwrap(),
        );
        let outcome = apply_local(&mut registers, &failure, 2, 11).map(|(_, value)| value);
        assert!(outcome.is_err());
        assert!(failure.try_finish(outcome).is_ok());
        assert_eq!(
            rx.try_recv().unwrap(),
            pumpkin_command::source::ReturnValue::Failure
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(board(&registers).get_score_value("Alice", "kills"), Some(9));
    }
    #[test]
    fn successful_operation_creates_zero_rhs_but_zero_division_and_modulo_create_nothing() {
        let mut registers = initial_scores();
        let update = semantic(ScoreAction::Operation {
            operation: ScoreOperation::Add,
            sources: vec!["Untracked".into()],
            objective: "kills".into(),
        });
        assert_eq!(apply_local(&mut registers, &update, 2, 11).unwrap().1, 7);
        assert_eq!(
            board(&registers).get_score_value("Untracked", "kills"),
            Some(0)
        );
        for operation in [ScoreOperation::Divide, ScoreOperation::Modulo] {
            let before = serde_json::to_vec(&registers).unwrap();
            let update = semantic(ScoreAction::Operation {
                operation,
                sources: vec!["AbsentDivision".into()],
                objective: "kills".into(),
            });
            assert!(apply_local(&mut registers, &update, 2, 11).is_err());
            assert_eq!(serde_json::to_vec(&registers).unwrap(), before);
            assert_eq!(
                board(&registers).get_score_value("AbsentDivision", "kills"),
                None
            );
        }
    }
    #[test]
    fn pending_completion_retains_original_actual_result_without_reapplying_mutation() {
        struct Capture {
            values: tokio::sync::mpsc::Sender<pumpkin_command::source::ReturnValue>,
            readiness: tokio::sync::mpsc::Sender<()>,
        }
        impl pumpkin_command::source::ReturnValueCallable for Capture {
            fn call(&self, value: pumpkin_command::source::ReturnValue) {
                self.values.try_send(value).unwrap();
            }
            fn deferred_completion_readiness(
                &self,
                _: pumpkin_command::deferred::DeferredCommandToken,
            ) -> pumpkin_command::deferred::CompletionReadiness {
                if self.readiness.is_closed() {
                    pumpkin_command::deferred::CompletionReadiness::Ready
                } else {
                    pumpkin_command::deferred::CompletionReadiness::Pending
                }
            }
        }
        let (values, mut received) = tokio::sync::mpsc::channel(1);
        let (readiness, gate) = tokio::sync::mpsc::channel(1);
        let mut source = crate::command::CommandSource::dummy();
        source.command_result_taker =
            pumpkin_command::source::ResultValueTaker(vec![Arc::new(Capture {
                values,
                readiness,
            })]);
        let source = Arc::new(source);
        let mut update = semantic(ScoreAction::Delta(1));
        update.feedback = Some(Arc::clone(&source));
        update.completion = Some(
            pumpkin_command::deferred::DeferredCommandCompletion::reserve(
                source,
                pumpkin_command::deferred::DeferredCommandToken([4; 16]),
            )
            .unwrap(),
        );
        let mut registers = initial_scores();
        let result = apply_local(&mut registers, &update, 2, 11).map(|(_, value)| value);
        let Err((update, actual)) = update.try_finish(result) else {
            panic!("completion must retain pending provenance");
        };
        assert_eq!(actual, Ok(8));
        assert!(update.feedback.is_none());
        assert!(received.try_recv().is_err());
        assert_eq!(
            apply_local(&mut registers, &semantic(ScoreAction::Delta(1)), 2, 11)
                .unwrap()
                .1,
            9
        );
        let Err((update, actual)) = update.try_finish(actual) else {
            panic!("completion must remain pending");
        };
        assert_eq!(actual, Ok(8));
        assert_eq!(board(&registers).get_score_value("Alice", "kills"), Some(9));
        drop(gate);
        assert!(update.try_finish(actual).is_ok());
        assert_eq!(
            received.try_recv().unwrap(),
            pumpkin_command::source::ReturnValue::Success(8)
        );
        assert!(received.try_recv().is_err());
        assert_eq!(board(&registers).get_score_value("Alice", "kills"), Some(9));
    }

    #[test]
    fn denied_completion_closes_original_token_without_semantic_result_callback() {
        struct Capture(tokio::sync::mpsc::Sender<pumpkin_command::deferred::DeferredCommandToken>);
        impl pumpkin_command::source::ReturnValueCallable for Capture {
            fn call(&self, _: pumpkin_command::source::ReturnValue) {
                panic!("retired authority must not store a semantic result");
            }
            fn deferred_completion_readiness(
                &self,
                _: pumpkin_command::deferred::DeferredCommandToken,
            ) -> pumpkin_command::deferred::CompletionReadiness {
                pumpkin_command::deferred::CompletionReadiness::Denied
            }
            fn complete_deferred_branch(
                &self,
                token: pumpkin_command::deferred::DeferredCommandToken,
            ) {
                self.0.try_send(token).unwrap();
            }
        }
        let (tx, mut received) = tokio::sync::mpsc::channel(1);
        let mut source = crate::command::CommandSource::dummy();
        source.command_result_taker =
            pumpkin_command::source::ResultValueTaker(vec![Arc::new(Capture(tx))]);
        let token = pumpkin_command::deferred::DeferredCommandToken([5; 16]);
        let mut update = semantic(ScoreAction::Delta(1));
        update.completion = Some(
            pumpkin_command::deferred::DeferredCommandCompletion::reserve(Arc::new(source), token)
                .unwrap(),
        );
        let mut registers = initial_scores();
        let result = apply_local(&mut registers, &update, 2, 11).map(|(_, value)| value);
        assert!(update.try_finish(result).is_ok());
        assert_eq!(received.try_recv().unwrap(), token);
        assert!(received.try_recv().is_err());
        assert_eq!(board(&registers).get_score_value("Alice", "kills"), Some(8));
    }
}
