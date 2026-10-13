use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_data::game_rules::{GameRule, GameRuleRegistry, GameRuleValue};
use pumpkin_util::Difficulty;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::Server;

pub const PREFIX: &[u8; 8] = b"GmRule01";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleValue {
    Bool(bool),
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigMutation {
    Rule { key: String, value: RuleValue },
    Difficulty { value: Difficulty, force: bool },
    DifficultyLock(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameruleSnapshot {
    pub issuer: PeerGeneration,
    pub revision: u64,
    pub slot: u64,
    pub rules: GameRuleRegistry,
    pub difficulty: Difficulty,
    pub difficulty_locked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Control {
    Request(ConfigMutation),
    Snapshot(GameruleSnapshot),
}

#[derive(Clone)]
pub enum GameruleInput {
    Local(ConfigMutation),
    Remote(InboundParcel),
}

fn rule(key: &str) -> Option<&'static GameRule> {
    GameRule::all().iter().find(|rule| rule.to_string() == key)
}

fn valid_mutation(mutation: &ConfigMutation) -> bool {
    match mutation {
        ConfigMutation::Rule { key, value } => rule(key).is_some_and(|rule| {
            matches!(
                (GameRuleRegistry::default().get(rule), value),
                (GameRuleValue::Bool(_), RuleValue::Bool(_))
                    | (GameRuleValue::Int(_), RuleValue::Int(_))
            )
        }),
        _ => true,
    }
}

fn encode(control: &Control) -> Result<Vec<u8>, postcard::Error> {
    let mut bytes = PREFIX.to_vec();
    bytes.extend(postcard::to_allocvec(control)?);
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Option<Control> {
    let bytes = bytes
        .strip_prefix(PREFIX)
        .filter(|bytes| bytes.len() <= 32 * 1024)?;
    let control: Control = postcard::from_bytes(bytes).ok()?;
    match &control {
        Control::Request(mutation) if !valid_mutation(mutation) => None,
        Control::Snapshot(value) if value.issuer.generation == 0 || value.revision == 0 => None,
        _ => Some(control),
    }
}

fn accepts(
    source: PeerGeneration,
    elected: PeerGeneration,
    previous: Option<&GameruleSnapshot>,
    next: &GameruleSnapshot,
) -> bool {
    source == elected
        && next.issuer == elected
        && next.revision != 0
        && previous.is_none_or(|old| {
            old.issuer != next.issuer || next.revision > old.revision && next.slot >= old.slot
        })
}

fn mutate(snapshot: &mut GameruleSnapshot, mutation: ConfigMutation, hardcore: bool) -> bool {
    match mutation {
        ConfigMutation::Rule { key, value } => {
            let Some(rule) = rule(&key) else {
                return false;
            };
            match (snapshot.rules.get_mut(rule), value) {
                (GameRuleValue::Bool(target), RuleValue::Bool(value)) => *target = value,
                (GameRuleValue::Int(target), RuleValue::Int(value)) => *target = value,
                _ => return false,
            }
        }
        ConfigMutation::Difficulty { value, force } => {
            if snapshot.difficulty_locked && !force {
                return false;
            }
            snapshot.difficulty = if hardcore { Difficulty::Hard } else { value };
        }
        ConfigMutation::DifficultyLock(value) => snapshot.difficulty_locked = value,
    }
    true
}

pub fn query(server: &Server, rule: &GameRule) -> Option<RuleValue> {
    let elected = super::cluster_world_rules::primary(server)?;
    let snapshot = server.cluster_gamerules_view.load();
    let snapshot = snapshot
        .as_ref()
        .filter(|snapshot| snapshot.issuer == elected)?;
    Some(match snapshot.rules.get(rule) {
        GameRuleValue::Bool(value) => RuleValue::Bool(*value),
        GameRuleValue::Int(value) => RuleValue::Int(*value),
    })
}

pub fn difficulty(server: &Server) -> Option<Difficulty> {
    let elected = super::cluster_world_rules::primary(server)?;
    server
        .cluster_gamerules_view
        .load()
        .as_ref()
        .filter(|snapshot| snapshot.issuer == elected)
        .map(|snapshot| snapshot.difficulty)
}

pub fn submit(server: &Server, mutation: ConfigMutation) -> bool {
    !server
        .cluster_global_shutdown
        .load(std::sync::atomic::Ordering::Acquire)
        && view(server).is_some()
        && super::cluster_world_rules::disciplined_slot().is_some()
        && valid_mutation(&mutation)
        && server
            .cluster_gamerules
            .try_send(GameruleInput::Local(mutation))
            .is_ok()
}

pub fn set_rule(server: &Server, rule: &GameRule, value: GameRuleValue<i64, bool>) -> bool {
    submit(
        server,
        ConfigMutation::Rule {
            key: rule.to_string(),
            value: match value {
                GameRuleValue::Bool(value) => RuleValue::Bool(value),
                GameRuleValue::Int(value) => RuleValue::Int(value),
            },
        },
    )
}

pub fn receive(server: &Server, parcel: InboundParcel) -> Result<(), InboundParcel> {
    if !parcel.bytes.starts_with(PREFIX)
        || parcel.header.kind != pumpkin_cluster::protocol::StreamKind::Control
    {
        return Err(parcel);
    }
    server
        .cluster_gamerules
        .try_send(GameruleInput::Remote(parcel))
        .map_err(|error| match error.into_inner() {
            GameruleInput::Remote(parcel) => parcel,
            GameruleInput::Local(_) => unreachable!(),
        })
}

fn install(server: &Server, snapshot: Arc<GameruleSnapshot>) {
    let before = server.level_info.load();
    let difficulty_changed = before.difficulty != snapshot.difficulty
        || before.difficulty_locked != snapshot.difficulty_locked;
    let mut info = (**before).clone();
    if let Some(world) = server
        .worlds
        .load()
        .iter()
        .find(|world| world.dimension.minecraft_name == "minecraft:overworld")
    {
        let environment = world.cluster_environment_view.load();
        if environment
            .snapshot
            .as_ref()
            .is_some_and(|value| value.issuer == snapshot.issuer)
        {
            let border = &environment.border;
            let slot =
                super::cluster_world_rules::disciplined_slot().unwrap_or(border.lerp_start_slot);
            info.border_center_x = border.center_x;
            info.border_center_z = border.center_z;
            info.border_size = border.current_diameter(slot);
            info.border_size_lerp_target = border.new_diameter;
            info.border_size_lerp_time = border.remaining_millis(slot);
            info.border_warning_blocks = f64::from(border.warning_blocks);
            info.border_warning_time = f64::from(border.warning_time);
            info.border_damage_per_block = f64::from(border.damage_per_block);
            info.border_safe_zone = f64::from(border.buffer);
            info.spawn_x = environment.spawn.x;
            info.spawn_y = environment.spawn.y;
            info.spawn_z = environment.spawn.z;
            info.spawn_yaw = environment.spawn.yaw;
            info.spawn_pitch = environment.spawn.pitch;
        }
    }
    info.game_rules = snapshot.rules.clone();
    info.difficulty = snapshot.difficulty;
    info.difficulty_locked = snapshot.difficulty_locked;
    server.level_info.store(Arc::new(info));
    for world in server.worlds.load().iter() {
        world.level_info.store(server.level_info.load_full());
        if difficulty_changed {
            let excluded = world
                .players
                .load()
                .iter()
                .filter(|player| player.is_in_cluster_lobby())
                .map(|player| player.gameprofile.id)
                .collect::<Vec<_>>();
            world.broadcast_packet_except_editioned(
                &excluded,
                &pumpkin_protocol::java::client::play::CChangeDifficulty::new(
                    snapshot.difficulty as u8,
                    snapshot.difficulty_locked,
                ),
                &pumpkin_protocol::bedrock::client::CSetDifficulty {
                    difficulty: (snapshot.difficulty as u32).into(),
                },
            );
        }
    }
    server.cluster_gamerules_view.store(Some(snapshot));
}

fn disk_path(server: &Server) -> Option<std::path::PathBuf> {
    let worlds = server.worlds.load();
    worlds
        .iter()
        .find(|world| world.dimension.minecraft_name == "minecraft:overworld")
        .or_else(|| worlds.first())
        .map(|world| {
            world
                .level
                .level_folder
                .root_folder
                .join("data/minecraft/cluster_game_rules.dat")
        })
}

pub fn view(server: &Server) -> Option<Arc<GameruleSnapshot>> {
    let elected = super::cluster_world_rules::primary(server)?;
    server
        .cluster_gamerules_view
        .load_full()
        .filter(|view| view.issuer == elected)
}

pub fn spawn(server: &Arc<Server>, mut rx: mpsc::Receiver<GameruleInput>) {
    if !server.advanced_config.cluster.enabled {
        return;
    }
    let (disk_tx, disk_rx) = mpsc::channel::<(std::path::PathBuf, Arc<GameruleSnapshot>)>(32);
    server.spawn_task(super::cluster_environment::save_configurations(
        disk_rx,
        |snapshot: &Arc<GameruleSnapshot>| encode(&Control::Snapshot((**snapshot).clone())).ok(),
    ));
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut diagnostics=super::cluster_control_registers::Diagnostics::default();
        let mut revision = 0_u64;
        let mut latest = None::<Arc<GameruleSnapshot>>;
        let mut pending = None;
        let mut unknown = VecDeque::new();
        let mut requests = VecDeque::<(ServerId,ConfigMutation)>::new();
        let mut snapshots = BTreeMap::<ServerId,Arc<GameruleSnapshot>>::new();
        let mut peers = HashMap::new();
        let mut save = None::<Arc<GameruleSnapshot>>;
        let mut restored = false;
        let mut adopted = None;
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            if let Some(elected)=super::cluster_world_rules::primary(&task_server)
                && elected.server.0 != task_server.advanced_config.cluster.server_id
                && let Some((_,value))=requests.front() {
                if let Ok(bytes)=encode(&Control::Request(value.clone()))
                    && task_server.cluster_global_outbound.try_event(&task_server,vec![elected.server],bytes) {requests.pop_front();}
            }

            if let Some((_,value))=snapshots.first_key_value() {
                let targets=snapshots.iter().filter(|(_,other)|other.issuer==value.issuer && other.revision==value.revision).map(|(peer,_)|*peer).collect::<Vec<_>>();
                if let Ok(bytes)=encode(&Control::Snapshot((**value).clone()))
                    && task_server.cluster_global_outbound.try_event(&task_server,targets.clone(),bytes) {for peer in targets {snapshots.remove(&peer);}}
            }

            if !restored && let Some(path)=disk_path(&task_server) {
                if matches!(task_server.advanced_config.cluster.role,pumpkin_config::ClusterRole::Primary)
                    && let Ok(bytes)=tokio::fs::read(path).await
                    && let Some(Control::Snapshot(value))=decode(&bytes) {
                    let mut info=(**task_server.level_info.load()).clone();
                    info.game_rules=value.rules;info.difficulty=value.difficulty;info.difficulty_locked=value.difficulty_locked;
                    task_server.level_info.store(Arc::new(info));
                }
                restored=true;
            }
            if pending.is_none()
                && super::cluster_world_rules::primary(&task_server).is_some_and(|issuer|issuer.server==ServerId(task_server.advanced_config.cluster.server_id))
                && let Some((_,value))=requests.pop_front() {pending=Some(GameruleInput::Local(value));}
            let stopping=task_server.cluster_global_shutdown.load(std::sync::atomic::Ordering::Acquire);
            if stopping {
                rx.close();
                if matches!(task_server.advanced_config.cluster.role,pumpkin_config::ClusterRole::Primary)
                    && let Some(value)=save.as_ref() && let Some(path)=disk_path(&task_server)
                    && disk_tx.try_send((path,value.clone())).is_ok() {save=None;}
                if rx.is_empty() && pending.is_none() && unknown.is_empty() && requests.is_empty() && snapshots.is_empty() && save.is_none() {break;}
            }
            let input=tokio::select! {
                input=rx.recv(), if pending.is_none() && unknown.len()<128 && requests.len()<1024 && (!rx.is_closed() || !rx.is_empty()) => input,
                _=flush.tick() => None,
            };
            let input=pending.take().or(input).or_else(||unknown.pop_front());
            if !restored {pending=input;continue;}
            let Some(elected)=super::cluster_world_rules::primary(&task_server) else {pending=input;continue;};
            let Some(slot)=super::cluster_world_rules::disciplined_slot() else {pending=input;continue;};
            let primary=elected.server==ServerId(task_server.advanced_config.cluster.server_id);
            if primary {
                let Some(cut)=super::cluster_environment::authority(&task_server,elected,slot) else {pending=input;continue;};
                if adopted!=Some((elected,cut.from_slot)) {
                    let mut info=(**task_server.level_info.load()).clone();
                    info.game_rules=cut.gamerules.rules.clone();info.difficulty=cut.gamerules.difficulty;info.difficulty_locked=cut.gamerules.difficulty_locked;
                    task_server.level_info.store(Arc::new(info));latest=None;adopted=Some((elected,cut.from_slot));
                }
            }
            if primary && latest.as_ref().is_some_and(|value|value.issuer==elected && slot<value.slot) {pending=input;continue;}
            let mut mutation=None;
            let mut mutation_peer=task_server.advanced_config.cluster.server_id;
            match input {
                Some(GameruleInput::Local(value)) => mutation=Some(value),
                Some(GameruleInput::Remote(parcel)) => {
                    let known_generation=task_server.cluster_peer_generations.load().get(&parcel.peer).copied();
                    if known_generation.is_some_and(|known|known>parcel.generation) {diagnostics.rejected("gamerules",parcel.peer.0,"stale transport");continue;}
                    if known_generation!=Some(parcel.generation) || !task_server.cluster_connected_peers.load().contains(&parcel.peer) {unknown.push_back(GameruleInput::Remote(parcel));continue;}
                    let Some(boot)=task_server.cluster_peer_incarnations.load().get(&parcel.peer).copied().filter(|boot| *boot!=0) else {unknown.push_back(GameruleInput::Remote(parcel));continue;};
                    let source=PeerGeneration{server:parcel.peer,generation:boot};
                    match decode(&parcel.bytes) {
                        Some(Control::Request(value)) => {mutation=Some(value);mutation_peer=parcel.peer.0;},
                        Some(Control::Snapshot(value)) => {
                            if accepts(source,elected,latest.as_deref(),&value) {
                                let value=Arc::new(value);install(&task_server,value.clone());latest=Some(value);
                            } else if source!=elected || latest.as_deref()!=Some(&value) {
                                diagnostics.rejected("gamerules",parcel.peer.0,"invalid source or snapshot version");
                            }
                        }
                        None => diagnostics.rejected("gamerules",parcel.peer.0,"malformed control"),
                    }
                }
                None => {},
            }
            if let Some(value)=mutation {
                if !primary {requests.push_back((elected.server,value));}
                else {
                    let info=task_server.level_info.load();
                    let mut next=GameruleSnapshot {issuer:elected,revision:revision.saturating_add(1),slot,rules:info.game_rules.clone(),difficulty:info.difficulty,difficulty_locked:info.difficulty_locked};
                    if mutate(&mut next,value,task_server.basic_config.hardcore) && next.revision>revision {
                        revision=next.revision;let next=Arc::new(next);install(&task_server,next.clone());latest=Some(next.clone());save=Some(next.clone());
                        for peer in task_server.cluster_connected_peers.load().iter() {snapshots.insert(*peer,next.clone());}
                    } else {diagnostics.rejected("gamerules",mutation_peer,"locked difficulty or invalid mutation");}
                }
            }
            if primary && !stopping && restored {
                let info=task_server.level_info.load();
                let changed=latest.as_ref().is_none_or(|old| old.issuer!=elected || old.rules!=info.game_rules || old.difficulty!=info.difficulty || old.difficulty_locked!=info.difficulty_locked);
                if changed && let Some(next_revision)=revision.checked_add(1) {
                    revision=next_revision;
                    let next=Arc::new(GameruleSnapshot{issuer:elected,revision,slot,rules:info.game_rules.clone(),difficulty:info.difficulty,difficulty_locked:info.difficulty_locked});
                    drop(info);install(&task_server,next.clone());latest=Some(next.clone());save=Some(next.clone());
                    for peer in task_server.cluster_connected_peers.load().iter() {snapshots.insert(*peer,next.clone());}
                }
                let connected=task_server.cluster_connected_peers.load();let generations=task_server.cluster_peer_generations.load();
                for peer in connected.iter() {
                    let generation=generations.get(peer).copied();
                    if peers.get(peer)!=Some(&generation) && let Some(value)=latest.as_ref().filter(|value|value.issuer==elected) {snapshots.insert(*peer,value.clone());peers.insert(*peer,generation);}
                }
                peers.retain(|peer,_|connected.contains(peer));snapshots.retain(|peer,_|connected.contains(peer));
            }
            if let Some(value)=latest.as_ref().filter(|value|value.issuer==elected) {install(&task_server,value.clone());}
            if !primary {peers.clear();}
            if primary && let Some(value)=save.as_ref() && let Some(path)=disk_path(&task_server)
                && disk_tx.try_send((path,value.clone())).is_ok() {save=None;}
            if stopping && rx.is_empty() && pending.is_none() && unknown.is_empty() && requests.is_empty() && snapshots.is_empty() && save.is_none() {break;}
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(issuer: PeerGeneration) -> GameruleSnapshot {
        GameruleSnapshot {
            issuer,
            revision: 1,
            slot: 1000,
            rules: GameRuleRegistry::default(),
            difficulty: Difficulty::Normal,
            difficulty_locked: false,
        }
    }

    #[test]
    fn full_registry_transport_rejects_forged_sources_and_replayed_versions() {
        let issuer = PeerGeneration {
            server: ServerId(1),
            generation: 9,
        };
        let mut state = snapshot(issuer);
        for rule in GameRule::all() {
            let value = match state.rules.get(rule) {
                GameRuleValue::Bool(value) => RuleValue::Bool(!value),
                GameRuleValue::Int(_) => RuleValue::Int(i64::MAX),
            };
            assert!(mutate(
                &mut state,
                ConfigMutation::Rule {
                    key: rule.to_string(),
                    value
                },
                false
            ));
        }
        let bytes = encode(&Control::Snapshot(state.clone())).unwrap();
        assert_eq!(decode(&bytes), Some(Control::Snapshot(state.clone())));
        assert!(!accepts(
            PeerGeneration {
                generation: 8,
                ..issuer
            },
            issuer,
            None,
            &state
        ));
        assert!(!accepts(issuer, issuer, Some(&state), &state));
        let mut next = state.clone();
        next.revision += 1;
        next.slot -= 1;
        assert!(!accepts(issuer, issuer, Some(&state), &next));
        next.slot = state.slot;
        assert!(accepts(issuer, issuer, Some(&state), &next));
    }

    #[test]
    fn typed_rules_preserve_signed_values_and_difficulty_lock_force_rules() {
        let issuer = PeerGeneration {
            server: ServerId(1),
            generation: 9,
        };
        let mut state = snapshot(issuer);
        assert!(mutate(
            &mut state,
            ConfigMutation::Rule {
                key: GameRule::PlayersSleepingPercentage.to_string(),
                value: RuleValue::Int(-500)
            },
            false
        ));
        assert_eq!(state.rules.players_sleeping_percentage, -500);
        assert!(!mutate(
            &mut state,
            ConfigMutation::Rule {
                key: GameRule::KeepInventory.to_string(),
                value: RuleValue::Int(1)
            },
            false
        ));
        assert!(mutate(
            &mut state,
            ConfigMutation::DifficultyLock(true),
            false
        ));
        assert!(!mutate(
            &mut state,
            ConfigMutation::Difficulty {
                value: Difficulty::Hard,
                force: false
            },
            false
        ));
        assert_eq!(state.difficulty, Difficulty::Normal);
        assert!(mutate(
            &mut state,
            ConfigMutation::Difficulty {
                value: Difficulty::Easy,
                force: true
            },
            true
        ));
        assert_eq!(state.difficulty, Difficulty::Hard);
        assert!(
            decode(
                &encode(&Control::Request(ConfigMutation::Rule {
                    key: "invalid".to_owned(),
                    value: RuleValue::Bool(true)
                }))
                .unwrap()
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn durable_rules_restore_full_registry_and_lock_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cluster_game_rules.dat");
        let issuer = PeerGeneration {
            server: ServerId(1),
            generation: 9,
        };
        let mut state = snapshot(issuer);
        state.rules.keep_inventory = true;
        state.difficulty = Difficulty::Hard;
        state.difficulty_locked = true;
        let bytes = encode(&Control::Snapshot(state.clone())).unwrap();
        super::super::cluster_environment::save_environment(&path, &bytes)
            .await
            .unwrap();
        assert_eq!(
            decode(&tokio::fs::read(&path).await.unwrap()),
            Some(Control::Snapshot(state))
        );
    }
}
