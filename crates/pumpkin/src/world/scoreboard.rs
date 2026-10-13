use arc_swap::ArcSwap;
use rustc_hash::FxHashMap;
use std::sync::Arc;

use pumpkin_data::scoreboard::ScoreboardDisplaySlot;
use pumpkin_protocol::{
    BClientPacket, ClientPacket, NumberFormat,
    bedrock::client::{
        remove_objective::CRemoveObjective as BRemoveObjective,
        set_display_objective::CSetDisplayObjective as BSetDisplayObjective,
        set_score::{CSetScore as BSetScore, ScoreEntry as BScoreEntry},
    },
    codec::var_int::VarInt,
    java::client::play::{
        CDisplayObjective, CResetScore, CSetPlayerTeam, CUpdateObjectives, CUpdateScore, Mode,
        RenderType, TeamMethod, TeamParameters,
    },
};
use pumpkin_util::text::{TextComponent, color::NamedColor};
use tracing::warn;

use super::World;
use crate::entity::player::Player;

pub trait ScoreboardTarget: Send + Sync {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        je_packet: &J,
        be_packet: &B,
    );
    fn send_je<J: ClientPacket + Sync>(&self, je_packet: &J);
}

fn world_scoreboard_visible(
    custom: &Option<crate::entity::player::CustomScoreboard>,
    bedrock: bool,
) -> bool {
    !matches!(
        (custom, bedrock),
        (
            Some(crate::entity::player::CustomScoreboard::Java(_)),
            false
        ) | (
            Some(crate::entity::player::CustomScoreboard::Bedrock(_)),
            true
        )
    )
}

fn uses_world_scoreboard(player: &Player) -> bool {
    world_scoreboard_visible(
        player.custom_scoreboard.snapshot().as_ref(),
        matches!(
            player.client.as_ref(),
            crate::net::ClientPlatform::Bedrock(_)
        ),
    )
}

impl ScoreboardTarget for World {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        je_packet: &J,
        be_packet: &B,
    ) {
        let players = self.players.load();
        let recipients: Vec<_> = players
            .iter()
            .filter(|player| uses_world_scoreboard(player))
            .collect();
        Self::broadcast_java_grouped(
            je_packet,
            Self::collect_java_recipients_by_version(recipients.iter().copied()),
        );
        Self::broadcast_bedrock_grouped(
            be_packet,
            recipients
                .iter()
                .filter_map(|player| match player.client.as_ref() {
                    crate::net::ClientPlatform::Bedrock(client) => Some(client),
                    crate::net::ClientPlatform::Java(_) => None,
                }),
        );
    }

    fn send_je<J: ClientPacket + Sync>(&self, je_packet: &J) {
        let players = self.players.load();
        Self::broadcast_java_grouped(
            je_packet,
            Self::collect_java_recipients_by_version(
                players
                    .iter()
                    .filter(|player| uses_world_scoreboard(player)),
            ),
        );
    }
}

impl<T: ScoreboardTarget + ?Sized> ScoreboardTarget for &T {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        je_packet: &J,
        be_packet: &B,
    ) {
        (*self).send_editioned(je_packet, be_packet);
    }

    fn send_je<J: ClientPacket + Sync>(&self, je_packet: &J) {
        (*self).send_je(je_packet);
    }
}

impl ScoreboardTarget for std::sync::Arc<World> {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        je_packet: &J,
        be_packet: &B,
    ) {
        <World as ScoreboardTarget>::send_editioned(self.as_ref(), je_packet, be_packet);
    }

    fn send_je<J: ClientPacket + Sync>(&self, je_packet: &J) {
        <World as ScoreboardTarget>::send_je(self.as_ref(), je_packet);
    }
}

impl ScoreboardTarget for Player {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        je_packet: &J,
        be_packet: &B,
    ) {
        self.try_enqueue_packet_editioned(je_packet, be_packet);
    }

    fn send_je<J: ClientPacket + Sync>(&self, je_packet: &J) {
        self.try_send_client_packet(je_packet);
    }
}

impl ScoreboardTarget for std::sync::Arc<Player> {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        je_packet: &J,
        be_packet: &B,
    ) {
        self.try_enqueue_packet_editioned(je_packet, be_packet);
    }

    fn send_je<J: ClientPacket + Sync>(&self, je_packet: &J) {
        self.try_send_client_packet(je_packet);
    }
}

pub struct NoTarget;

impl ScoreboardTarget for NoTarget {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        _je_packet: &J,
        _be_packet: &B,
    ) {
    }

    fn send_je<J: ClientPacket + Sync>(&self, _je_packet: &J) {}
}

#[derive(Clone, Debug, Default)]
pub struct Scoreboard {
    objectives: FxHashMap<String, ScoreboardObjective>,
    display_slots: FxHashMap<ScoreboardDisplaySlot, String>,
    scores: FxHashMap<String, FxHashMap<String, ScoreboardScore>>,
    teams: FxHashMap<String, Team>,
    deferred: bool,
    team_snapshot: Option<Arc<ArcSwap<FxHashMap<String, Team>>>>,
    score_snapshot: Option<Arc<ArcSwap<FxHashMap<String, FxHashMap<String, ScoreboardScore>>>>>,
}

impl Scoreboard {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_team_snapshot(snapshot: Arc<ArcSwap<FxHashMap<String, Team>>>) -> Self {
        Self {
            team_snapshot: Some(snapshot),
            ..Self::default()
        }
    }

    pub fn with_score_snapshot(
        mut self,
        snapshot: Arc<ArcSwap<FxHashMap<String, FxHashMap<String, ScoreboardScore>>>>,
    ) -> Self {
        self.score_snapshot = Some(snapshot);
        self
    }

    fn publish_scores(&self) {
        if self.deferred {
            return;
        }
        if let Some(snapshot) = self.score_snapshot.as_ref() {
            snapshot.store(Arc::new(self.scores.clone()));
        }
    }

    fn publish_teams(&self) {
        if self.deferred {
            return;
        }
        if let Some(snapshot) = self.team_snapshot.as_ref() {
            snapshot.store(Arc::new(self.teams.clone()));
        }
    }

    #[must_use]
    pub const fn get_objectives(&self) -> &FxHashMap<String, ScoreboardObjective> {
        &self.objectives
    }

    #[must_use]
    pub fn get_objective(&self, name: &str) -> Option<&ScoreboardObjective> {
        self.objectives.get(name)
    }

    pub fn get_objective_mut(&mut self, name: &str) -> Option<&mut ScoreboardObjective> {
        self.objectives.get_mut(name)
    }

    #[must_use]
    pub const fn get_display_slots(&self) -> &FxHashMap<ScoreboardDisplaySlot, String> {
        &self.display_slots
    }

    #[must_use]
    pub fn get_display_objective(&self, slot: ScoreboardDisplaySlot) -> Option<&str> {
        self.display_slots.get(&slot).map(String::as_str)
    }

    #[must_use]
    pub const fn get_scores(&self) -> &FxHashMap<String, FxHashMap<String, ScoreboardScore>> {
        &self.scores
    }

    #[must_use]
    pub fn get_score(&self, entity_name: &str, objective_name: &str) -> Option<&ScoreboardScore> {
        self.scores.get(objective_name)?.get(entity_name)
    }

    #[must_use]
    pub fn get_score_value(&self, entity_name: &str, objective_name: &str) -> Option<i32> {
        self.get_score(entity_name, objective_name)
            .map(|s| s.value.0)
    }

    #[must_use]
    pub fn get_scores_for_objective(
        &self,
        objective_name: &str,
    ) -> Option<&FxHashMap<String, ScoreboardScore>> {
        self.scores.get(objective_name)
    }

    #[must_use]
    pub fn get_scores_for_entity(&self, entity_name: &str) -> FxHashMap<String, &ScoreboardScore> {
        let mut entity_scores = FxHashMap::default();
        for (obj_name, obj_scores) in &self.scores {
            if let Some(score) = obj_scores.get(entity_name) {
                entity_scores.insert(obj_name.clone(), score);
            }
        }
        entity_scores
    }

    #[must_use]
    pub const fn get_teams(&self) -> &FxHashMap<String, Team> {
        &self.teams
    }

    #[must_use]
    pub fn get_team(&self, name: &str) -> Option<&Team> {
        self.teams.get(name)
    }

    pub fn get_team_mut(&mut self, name: &str) -> Option<&mut Team> {
        self.teams.get_mut(name)
    }

    #[must_use]
    pub fn get_entity_team(&self, entity_name: &str) -> Option<&Team> {
        self.teams
            .values()
            .find(|team| team.players.iter().any(|p| p == entity_name))
    }

    pub fn add_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        objective: ScoreboardObjective,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if self.objectives.contains_key(&objective.name) {
            warn!(
                "Tried to create an objective which already exists: {}",
                &objective.name
            );
            return;
        }

        let je_update = CUpdateObjectives::new(
            objective.name.clone(),
            Mode::Add,
            objective.display_name.clone(),
            objective.render_type,
            objective.number_format.clone(),
        );

        let be_update = BSetDisplayObjective {
            display_slot_name: "sidebar".to_string(), // Default to sidebar
            objective_name: objective.name.clone(),
            objective_display_name: objective.display_name.clone().get_text(),
            criteria_name: "dummy".to_string(),
            sort_order: VarInt(0),
        };

        target.send_editioned(&je_update, &be_update);

        self.objectives.insert(objective.name.clone(), objective);
    }

    pub fn update_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        objective: ScoreboardObjective,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if !self.objectives.contains_key(&objective.name) {
            warn!(
                "Tried to update an objective which does not exist: {}",
                &objective.name
            );
            return;
        }

        let je_update = CUpdateObjectives::new(
            objective.name.clone(),
            Mode::Update,
            objective.display_name.clone(),
            objective.render_type,
            objective.number_format.clone(),
        );

        let be_update = BSetDisplayObjective {
            display_slot_name: "sidebar".to_string(),
            objective_name: objective.name.clone(),
            objective_display_name: objective.display_name.clone().get_text(),
            criteria_name: "dummy".to_string(),
            sort_order: VarInt(0),
        };

        target.send_editioned(&je_update, &be_update);

        self.objectives.insert(objective.name.clone(), objective);
    }

    pub fn set_display_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        slot: ScoreboardDisplaySlot,
        objective_name: Option<&str>,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let slot_str = match slot {
            ScoreboardDisplaySlot::List => "list",
            ScoreboardDisplaySlot::BelowName => "belowname",
            _ => "sidebar",
        };

        let obj_name_str = objective_name.unwrap_or("");

        let display_name = objective_name
            .and_then(|name| self.objectives.get(name))
            .map_or_else(
                || obj_name_str.to_string(),
                |o| o.display_name.clone().get_text(),
            );

        let je_display = CDisplayObjective::new(slot, obj_name_str.to_string());
        let be_display = BSetDisplayObjective {
            display_slot_name: slot_str.to_string(),
            objective_name: obj_name_str.to_string(),
            objective_display_name: display_name,
            criteria_name: "dummy".to_string(),
            sort_order: VarInt(0),
        };

        target.send_editioned(&je_display, &be_display);

        if let Some(name) = objective_name {
            self.display_slots.insert(slot, name.to_string());
        } else {
            self.display_slots.remove(&slot);
        }
    }

    pub fn clear_display_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        slot: ScoreboardDisplaySlot,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        self.set_display_objective(&target, slot, None);
    }

    pub fn remove_objective(&mut self, target: &impl ScoreboardTarget, name: &str) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if !self.objectives.contains_key(name) {
            warn!(
                "Tried to remove an objective which does not exist: {}",
                name
            );
            return;
        }

        let je_packet = CUpdateObjectives::new(
            name.to_string(),
            Mode::Remove,
            TextComponent::empty(),
            RenderType::Integer,
            None,
        );

        let be_packet = BRemoveObjective {
            objective_name: name.to_string(),
        };

        target.send_editioned(&je_packet, &be_packet);

        self.objectives.remove(name);
        self.scores.remove(name);
        self.publish_scores();
        self.display_slots.retain(|_, obj| obj != name);
    }

    pub fn update_score(&mut self, target: &impl ScoreboardTarget, score: ScoreboardScore) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if !self.objectives.contains_key(&score.objective_name) {
            warn!(
                "Tried to place a score into an objective which does not exist: {}",
                &score.objective_name
            );
            return;
        }

        let je_packet = CUpdateScore::new(
            score.entity_name.clone(),
            score.objective_name.clone(),
            score.value,
            score.display_name.clone(),
            score.number_format.clone(),
        );

        let be_packet = BSetScore {
            action: VarInt(0), // Change
            entries: vec![BScoreEntry {
                scoreboard_id: score_identity(&score.entity_name, &score.objective_name), // Internal ID
                objective_name: score.objective_name.clone(),
                score: score.value,
                entry_type: VarInt(3), // Fake player / Literal
                entity_unique_id: 0,
                custom_name: score.entity_name.clone(),
            }],
        };

        target.send_editioned(&je_packet, &be_packet);

        self.scores
            .entry(score.objective_name.clone())
            .or_default()
            .insert(score.entity_name.clone(), score);
        self.publish_scores();
    }

    pub fn set_score_value(
        &mut self,
        target: &impl ScoreboardTarget,
        entity_name: impl Into<String>,
        objective_name: impl Into<String>,
        value: i32,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let entity_s = entity_name.into();
        let obj_s = objective_name.into();
        let existing = self.get_score(&entity_s, &obj_s).cloned();
        let score = ScoreboardScore {
            entity_name: entity_s,
            objective_name: obj_s,
            value: VarInt(value),
            display_name: existing.as_ref().and_then(|s| s.display_name.clone()),
            number_format: existing.as_ref().and_then(|s| s.number_format.clone()),
            locked: existing.as_ref().is_none_or(|s| s.locked),
        };
        self.update_score(&target, score);
    }

    pub fn add_score(
        &mut self,
        target: &impl ScoreboardTarget,
        entity_name: impl Into<String>,
        objective_name: impl Into<String>,
        delta: i32,
    ) -> i32 {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let entity_s = entity_name.into();
        let obj_s = objective_name.into();
        let current_val = self.get_score_value(&entity_s, &obj_s).unwrap_or(0);
        let new_val = current_val + delta;
        self.set_score_value(&target, entity_s, obj_s, new_val);
        new_val
    }

    pub fn remove_score(
        &mut self,
        target: &impl ScoreboardTarget,
        entity_name: &str,
        objective_name: &str,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let je_packet = CResetScore::new(entity_name.to_string(), Some(objective_name.to_string()));

        let be_packet = BSetScore {
            action: VarInt(1), // Remove
            entries: vec![BScoreEntry {
                scoreboard_id: score_identity(entity_name, objective_name),
                objective_name: objective_name.to_string(),
                score: VarInt(0),
                entry_type: VarInt(3),
                entity_unique_id: 0,
                custom_name: entity_name.to_string(),
            }],
        };

        target.send_editioned(&je_packet, &be_packet);

        if let Some(objective_scores) = self.scores.get_mut(objective_name) {
            objective_scores.remove(entity_name);
        }
        self.publish_scores();
    }

    pub fn reset_scores_for_entity(&mut self, target: &impl ScoreboardTarget, entity_name: &str) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let je_packet = CResetScore::new(entity_name.to_string(), None);

        let mut be_entries = Vec::new();
        for (obj_name, obj_scores) in &self.scores {
            if obj_scores.contains_key(entity_name) {
                be_entries.push(BScoreEntry {
                    scoreboard_id: score_identity(entity_name, obj_name),
                    objective_name: obj_name.clone(),
                    score: VarInt(0),
                    entry_type: VarInt(3),
                    entity_unique_id: 0,
                    custom_name: entity_name.to_string(),
                });
            }
        }

        let be_packet = BSetScore {
            action: VarInt(1), // Remove
            entries: be_entries,
        };

        target.send_editioned(&je_packet, &be_packet);

        for obj_scores in self.scores.values_mut() {
            obj_scores.remove(entity_name);
        }
        self.publish_scores();
    }

    pub fn add_team(&mut self, target: &impl ScoreboardTarget, team: Team) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if self.teams.contains_key(&team.name) {
            warn!("Tried to create Team which already exists, {}", team.name);
            return;
        }

        let parameters = TeamParameters {
            display_name: &team.display_name,
            options: team.options,
            nametag_visibility: team.nametag_visibility.to_str(),
            collision_rule: team.collision_rule.to_str(),
            color: team.color as i32,
            player_prefix: &team.player_prefix,
            player_suffix: &team.player_suffix,
        };

        target.send_je(&CSetPlayerTeam {
            team_name: team.name.clone(),
            method: TeamMethod::Create,
            parameters: Some(parameters),
            players: team.players.clone().into(),
        });

        self.teams.insert(team.name.clone(), team);
        self.publish_teams();
    }

    pub fn create_team(&mut self, target: &impl ScoreboardTarget, team: Team) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        self.add_team(&target, team);
    }

    pub fn update_team(&mut self, target: &impl ScoreboardTarget, team: Team) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if !self.teams.contains_key(&team.name) {
            warn!("Tried to update Team which does not exist, {}", team.name);
            return;
        }

        let parameters = TeamParameters {
            display_name: &team.display_name,
            options: team.options,
            nametag_visibility: team.nametag_visibility.to_str(),
            collision_rule: team.collision_rule.to_str(),
            color: team.color as i32,
            player_prefix: &team.player_prefix,
            player_suffix: &team.player_suffix,
        };

        target.send_je(&CSetPlayerTeam {
            team_name: team.name.clone(),
            method: TeamMethod::Update,
            parameters: Some(parameters),
            players: Box::new([]),
        });

        self.teams.insert(team.name.clone(), team);
        self.publish_teams();
    }

    pub fn remove_team(&mut self, target: &impl ScoreboardTarget, name: &str) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if !self.teams.contains_key(name) {
            warn!("Tried to remove Team which does not exist, {}", name);
            return;
        }

        target.send_je(&CSetPlayerTeam {
            team_name: name.to_string(),
            method: TeamMethod::Remove,
            parameters: None,
            players: Box::new([]),
        });

        self.teams.remove(name);
        self.publish_teams();
    }

    pub fn add_player_to_team(
        &mut self,
        target: &impl ScoreboardTarget,
        team_name: &str,
        player: String,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        if !self.teams.contains_key(team_name) {
            return;
        }
        let previous = self
            .teams
            .values()
            .filter(|team| team.name != team_name && team.players.contains(&player))
            .map(|team| team.name.clone())
            .collect::<Vec<_>>();
        for name in previous {
            self.remove_player_from_team(&target, &name, &player);
        }
        let Some(team) = self.teams.get_mut(team_name) else {
            warn!(
                "Tried to add player to Team which does not exist, {}",
                team_name
            );
            return;
        };

        if team.players.contains(&player) {
            return;
        }

        target.send_je(&CSetPlayerTeam {
            team_name: team_name.to_string(),
            method: TeamMethod::AddPlayers,
            parameters: None,
            players: vec![player.clone()].into(),
        });

        team.players.push(player);
        self.publish_teams();
    }

    pub fn remove_player_from_team(
        &mut self,
        target: &impl ScoreboardTarget,
        team_name: &str,
        player: &str,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let Some(team) = self.teams.get_mut(team_name) else {
            warn!(
                "Tried to remove player from Team which does not exist, {}",
                team_name
            );
            return;
        };

        if !team.players.contains(&player.to_string()) {
            return;
        }

        target.send_je(&CSetPlayerTeam {
            team_name: team_name.to_string(),
            method: TeamMethod::RemovePlayers,
            parameters: None,
            players: vec![player.to_string()].into(),
        });

        team.players.retain(|p| p != player);
        self.publish_teams();
    }

    pub fn clear_team_players(&mut self, target: &impl ScoreboardTarget, team_name: &str) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let Some(team) = self.teams.get_mut(team_name) else {
            warn!(
                "Tried to clear players from Team which does not exist, {}",
                team_name
            );
            return;
        };

        if team.players.is_empty() {
            return;
        }

        let players_to_remove = team.players.clone();
        target.send_je(&CSetPlayerTeam {
            team_name: team_name.to_string(),
            method: TeamMethod::RemovePlayers,
            parameters: None,
            players: players_to_remove.into(),
        });

        team.players.clear();
        self.publish_teams();
    }

    pub(crate) fn valid_record(key: &[String], value: Option<&serde_json::Value>) -> bool {
        if key.len() == 2 {
            return match key[0].as_str() {
                "member" => value.is_none_or(serde_json::Value::is_string),
                "display" => {
                    key[1]
                        .parse::<usize>()
                        .is_ok_and(|slot| slot < DISPLAY_SLOTS.len())
                        && value.is_none_or(serde_json::Value::is_string)
                }
                _ => false,
            };
        }
        if key.len() != 3 {
            return false;
        }
        let field = key[2].as_str();
        if key[0] == "score" {
            let Ok((objective, entity)) = serde_json::from_str::<(String, String)>(&key[1]) else {
                return false;
            };
            if serde_json::to_string(&(&objective, &entity)).ok().as_ref() != Some(&key[1]) {
                return false;
            }
            return match field {
                "objective_name" => value.is_none_or(|value| value.as_str() == Some(&objective)),
                "entity_name" => value.is_none_or(|value| value.as_str() == Some(&entity)),
                "value" => value.is_none_or(|value| {
                    value
                        .as_i64()
                        .is_some_and(|value| i32::try_from(value).is_ok())
                }),
                "locked" => value.is_none_or(serde_json::Value::is_boolean),
                "display_name" => value.is_none_or(|value| {
                    value.is_null()
                        || serde_json::from_value::<TextComponent>(value.clone()).is_ok()
                }),
                "number_format" => value.is_none_or(number_format_serde::valid),
                _ => false,
            };
        }
        match (key[0].as_str(), field) {
            ("objective" | "team", "name") => {
                value.is_none_or(|value| value.as_str() == Some(&key[1]))
            }
            ("objective" | "team", "display_name")
            | ("team", "player_prefix" | "player_suffix") => value
                .is_none_or(|value| serde_json::from_value::<TextComponent>(value.clone()).is_ok()),
            ("objective", "render_type") => value.is_none_or(serde_json::Value::is_boolean),
            ("objective", "criterion") => value.is_none_or(serde_json::Value::is_string),
            ("objective", "number_format") => value.is_none_or(number_format_serde::valid),
            ("team", "options") => value.is_none_or(|value| {
                value
                    .as_i64()
                    .is_some_and(|value| i8::try_from(value).is_ok())
            }),
            ("team", "nametag_visibility") => value.is_none_or(|value| {
                serde_json::from_value::<NameTagVisibility>(value.clone()).is_ok()
            }),
            ("team", "collision_rule") => value
                .is_none_or(|value| serde_json::from_value::<CollisionRule>(value.clone()).is_ok()),
            ("team", "color") => value
                .is_none_or(|value| serde_json::from_value::<NamedColor>(value.clone()).is_ok()),
            ("team", "death_message_visibility") => value.is_none_or(|value| {
                value.as_str().is_some_and(|value| {
                    matches!(
                        value,
                        "always" | "never" | "hideForOtherTeams" | "hideForOwnTeam"
                    )
                })
            }),
            _ => false,
        }
    }

    pub(crate) fn changes_from(
        &self,
        before: &Self,
    ) -> Vec<(Vec<String>, Option<serde_json::Value>)> {
        let before = before.records();
        let after = self.records();
        let mut changes = Vec::new();
        for (key, value) in &after {
            if before.get(key) != Some(value) {
                changes.push((key.clone(), Some(value.clone())));
            }
        }
        for key in before.keys().filter(|key| !after.contains_key(*key)) {
            changes.push((key.clone(), None));
        }
        changes
    }

    pub(crate) fn records(&self) -> std::collections::BTreeMap<Vec<String>, serde_json::Value> {
        let mut records = std::collections::BTreeMap::new();
        for (kind, values) in [
            (
                "objective",
                self.objectives
                    .iter()
                    .map(|(name, value)| (name.clone(), serde_json::to_value(value).unwrap()))
                    .collect::<Vec<_>>(),
            ),
            (
                "team",
                self.teams
                    .iter()
                    .map(|(name, value)| (name.clone(), serde_json::to_value(value).unwrap()))
                    .collect::<Vec<_>>(),
            ),
            (
                "score",
                self.scores
                    .iter()
                    .flat_map(|(objective, scores)| {
                        scores.iter().map(move |(entity, value)| {
                            (
                                serde_json::to_string(&(objective, entity)).unwrap(),
                                serde_json::to_value(value).unwrap(),
                            )
                        })
                    })
                    .collect::<Vec<_>>(),
            ),
        ] {
            for (name, value) in values {
                if let serde_json::Value::Object(fields) = value {
                    for (field, value) in fields {
                        if kind == "team" && field == "players" {
                            continue;
                        }
                        records.insert(vec![kind.into(), name.clone(), field], value);
                    }
                }
            }
        }
        for team in self.teams.values() {
            for player in &team.players {
                records.insert(
                    vec!["member".into(), player.clone()],
                    team.name.clone().into(),
                );
            }
        }
        for (slot, objective) in &self.display_slots {
            records.insert(
                vec!["display".into(), (*slot as u8).to_string()],
                objective.clone().into(),
            );
        }
        records
    }

    pub(crate) fn from_records(
        records: &std::collections::BTreeMap<Vec<String>, serde_json::Value>,
    ) -> Self {
        let mut board = Self::new();
        let mut groups = std::collections::BTreeMap::<
            (String, String),
            serde_json::Map<String, serde_json::Value>,
        >::new();
        for (key, value) in records {
            if key.len() == 3 {
                groups
                    .entry((key[0].clone(), key[1].clone()))
                    .or_default()
                    .insert(key[2].clone(), value.clone());
            }
        }
        for ((kind, name), mut fields) in groups {
            match kind.as_str() {
                "objective" => {
                    if let Ok(value) = serde_json::from_value::<ScoreboardObjective>(fields.into())
                    {
                        if value.name == name {
                            board.objectives.insert(name, value);
                        }
                    }
                }
                "team" => {
                    fields.insert("players".into(), serde_json::json!([]));
                    if let Ok(value) = serde_json::from_value::<Team>(fields.into()) {
                        if value.name == name {
                            board.teams.insert(name, value);
                        }
                    }
                }
                "score" => {
                    if let Ok(value) = serde_json::from_value::<ScoreboardScore>(fields.into()) {
                        if board.objectives.contains_key(&value.objective_name) {
                            board
                                .scores
                                .entry(value.objective_name.clone())
                                .or_default()
                                .insert(value.entity_name.clone(), value);
                        }
                    }
                }
                _ => {}
            }
        }
        for (key, value) in records {
            if key.len() != 2 {
                continue;
            }
            if key[0] == "member" {
                if let Some(team) = value.as_str().and_then(|name| board.teams.get_mut(name)) {
                    team.players.push(key[1].clone());
                }
            } else if key[0] == "display" {
                if let Some(slot) = key[1]
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| DISPLAY_SLOTS.get(index))
                    .copied()
                {
                    if let Some(name) = value
                        .as_str()
                        .filter(|name| board.objectives.contains_key(*name))
                    {
                        board.display_slots.insert(slot, name.to_owned());
                    }
                }
            }
        }
        board
    }

    pub(crate) fn reconcile(&mut self, target: &impl ScoreboardTarget, desired: Self) {
        for name in self
            .objectives
            .keys()
            .filter(|name| !desired.objectives.contains_key(*name))
            .cloned()
            .collect::<Vec<_>>()
        {
            self.remove_objective(target, &name);
        }
        for (name, objective) in desired.objectives {
            match self.objectives.get(&name) {
                None => self.add_objective(target, objective),
                Some(old) if old != &objective => self.update_objective(target, objective),
                _ => {}
            }
        }
        for (objective, scores) in self.scores.clone() {
            for entity in scores.keys() {
                if !desired
                    .scores
                    .get(&objective)
                    .is_some_and(|scores| scores.contains_key(entity))
                {
                    self.remove_score(target, entity, &objective);
                }
            }
        }
        for (objective, scores) in desired.scores {
            for (entity, score) in scores {
                if self.get_score(&entity, &objective) != Some(&score) {
                    self.update_score(target, score);
                }
            }
        }
        for slot in DISPLAY_SLOTS {
            let desired = desired.display_slots.get(&slot).map(String::as_str);
            if self.get_display_objective(slot) != desired {
                self.set_display_objective(target, slot, desired);
            }
        }
        for name in self
            .teams
            .keys()
            .filter(|name| !desired.teams.contains_key(*name))
            .cloned()
            .collect::<Vec<_>>()
        {
            self.remove_team(target, &name);
        }
        for (name, team) in desired.teams {
            match self.teams.get(&name) {
                None => self.add_team(target, team),
                Some(old) if old != &team => {
                    self.remove_team(target, &name);
                    self.add_team(target, team);
                }
                _ => {}
            }
        }
        self.publish_scores();
        self.publish_teams();
    }

    pub fn send_to_player(&self, player: &Player) {
        self.send_to(player);
    }

    pub fn send_to(&self, target: &impl ScoreboardTarget) {
        for objective in self.objectives.values() {
            let je_update = CUpdateObjectives::new(
                objective.name.clone(),
                Mode::Add,
                objective.display_name.clone(),
                objective.render_type,
                objective.number_format.clone(),
            );
            let be_update = BSetDisplayObjective {
                display_slot_name: "sidebar".to_string(),
                objective_name: objective.name.clone(),
                objective_display_name: objective.display_name.clone().get_text(),
                criteria_name: "dummy".to_string(),
                sort_order: VarInt(0),
            };
            target.send_editioned(&je_update, &be_update);
        }

        for (slot, objective_name) in &self.display_slots {
            let slot_str = match slot {
                ScoreboardDisplaySlot::List => "list",
                ScoreboardDisplaySlot::BelowName => "belowname",
                _ => "sidebar",
            };
            let display_name = self.objectives.get(objective_name).map_or_else(
                || objective_name.clone(),
                |o| o.display_name.clone().get_text(),
            );
            let je_display = CDisplayObjective::new(*slot, objective_name.clone());
            let be_display = BSetDisplayObjective {
                display_slot_name: slot_str.to_string(),
                objective_name: objective_name.clone(),
                objective_display_name: display_name,
                criteria_name: "dummy".to_string(),
                sort_order: VarInt(0),
            };
            target.send_editioned(&je_display, &be_display);
        }

        for objective_scores in self.scores.values() {
            for score in objective_scores.values() {
                let je_packet = CUpdateScore::new(
                    score.entity_name.clone(),
                    score.objective_name.clone(),
                    score.value,
                    score.display_name.clone(),
                    score.number_format.clone(),
                );
                let be_packet = BSetScore {
                    action: VarInt(0),
                    entries: vec![BScoreEntry {
                        scoreboard_id: score_identity(&score.entity_name, &score.objective_name),
                        objective_name: score.objective_name.clone(),
                        score: score.value,
                        entry_type: VarInt(3),
                        entity_unique_id: 0,
                        custom_name: score.entity_name.clone(),
                    }],
                };
                target.send_editioned(&je_packet, &be_packet);
            }
        }

        for team in self.teams.values() {
            let parameters = TeamParameters {
                display_name: &team.display_name,
                options: team.options,
                nametag_visibility: team.nametag_visibility.to_str(),
                collision_rule: team.collision_rule.to_str(),
                color: team.color as i32,
                player_prefix: &team.player_prefix,
                player_suffix: &team.player_suffix,
            };
            let je_packet = CSetPlayerTeam {
                team_name: team.name.clone(),
                method: TeamMethod::Create,
                parameters: Some(parameters),
                players: team.players.clone().into(),
            };
            target.send_je(&je_packet);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScoreboardObjective {
    pub name: String,
    pub display_name: TextComponent,
    #[serde(with = "render_type_serde")]
    pub render_type: RenderType,
    #[serde(with = "number_format_serde")]
    pub number_format: Option<NumberFormat>,
    pub criterion: String,
}

impl ScoreboardObjective {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        display_name: TextComponent,
        render_type: RenderType,
        number_format: Option<NumberFormat>,
        criterion: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            display_name,
            render_type,
            number_format,
            criterion: criterion.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScoreboardScore {
    pub entity_name: String,
    pub objective_name: String,
    #[serde(with = "score_value_serde")]
    pub value: VarInt,
    pub display_name: Option<TextComponent>,
    #[serde(with = "number_format_serde")]
    pub number_format: Option<NumberFormat>,
    pub locked: bool,
}

impl ScoreboardScore {
    #[must_use]
    pub fn new(
        entity_name: impl Into<String>,
        objective_name: impl Into<String>,
        value: VarInt,
        display_name: Option<TextComponent>,
        number_format: Option<NumberFormat>,
    ) -> Self {
        Self {
            entity_name: entity_name.into(),
            objective_name: objective_name.into(),
            value,
            display_name,
            number_format,
            locked: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NameTagVisibility {
    Always,
    Never,
    HideForOtherTeams,
    HideForOwnTeam,
}

impl NameTagVisibility {
    #[must_use]
    pub const fn to_str(&self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Never => "never",
            Self::HideForOtherTeams => "hideForOtherTeams",
            Self::HideForOwnTeam => "hideForOwnTeam",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CollisionRule {
    Always,
    Never,
    PushOtherTeams,
    PushOwnTeam,
}

impl CollisionRule {
    #[must_use]
    pub const fn to_str(&self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Never => "never",
            Self::PushOtherTeams => "pushOtherTeams",
            Self::PushOwnTeam => "pushOwnTeam",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Team {
    pub name: String,
    pub display_name: TextComponent,
    pub options: i8,
    pub nametag_visibility: NameTagVisibility,
    pub collision_rule: CollisionRule,
    #[serde(default = "default_visibility")]
    pub death_message_visibility: String,
    pub color: NamedColor,
    pub player_prefix: TextComponent,
    pub player_suffix: TextComponent,
    pub players: Vec<String>,
}

#[cfg(test)]
mod cluster_team_snapshot_tests {
    use super::*;

    #[test]
    fn team_membership_publication_preserves_prior_reader_snapshot() {
        let published = Arc::new(ArcSwap::from_pointee(FxHashMap::default()));
        let mut board = Scoreboard::with_team_snapshot(Arc::clone(&published));
        board.add_team(
            &NoTarget,
            Team {
                name: String::from("red"),
                display_name: TextComponent::text("Red"),
                options: 0,
                nametag_visibility: NameTagVisibility::Always,
                collision_rule: CollisionRule::Always,
                death_message_visibility: default_visibility(),
                color: NamedColor::Red,
                player_prefix: TextComponent::empty(),
                player_suffix: TextComponent::empty(),
                players: Vec::new(),
            },
        );
        let before = published.load_full();
        board.add_player_to_team(&NoTarget, "red", String::from("Alice"));
        assert!(before.get("red").unwrap().players.is_empty());
        assert_eq!(
            published.load().get("red").unwrap().players,
            vec![String::from("Alice")]
        );
        board.remove_team(&NoTarget, "red");
        assert!(published.load().get("red").is_none());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BedrockSortOrder {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BedrockDisplaySlot {
    PlayerList,
    Sidebar,
    BelowName,
}

impl BedrockDisplaySlot {
    #[must_use]
    pub const fn to_str(&self) -> &'static str {
        match self {
            Self::PlayerList => "list",
            Self::Sidebar => "sidebar",
            Self::BelowName => "belowname",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BedrockObjective {
    pub name: String,
    pub display_name: String,
    pub sort_order: BedrockSortOrder,
}

#[derive(Clone, Debug, Default)]
pub struct BedrockScoreboard {
    deferred: bool,
    pub objectives: FxHashMap<String, BedrockObjective>,
    pub display_slots: FxHashMap<BedrockDisplaySlot, String>,
    pub scores: FxHashMap<(String, String), i32>,
}

impl BedrockScoreboard {
    pub fn add_objective(&mut self, target: &impl ScoreboardTarget, objective: BedrockObjective) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let be_update = BSetDisplayObjective {
            display_slot_name: "sidebar".to_string(),
            objective_name: objective.name.clone(),
            objective_display_name: objective.display_name.clone(),
            criteria_name: "dummy".to_string(),
            sort_order: VarInt(match objective.sort_order {
                BedrockSortOrder::Ascending => 0,
                BedrockSortOrder::Descending => 1,
            }),
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CUpdateObjectives::new(
                objective.name.clone(),
                pumpkin_protocol::java::client::play::Mode::Add,
                TextComponent::text(objective.display_name.clone()),
                pumpkin_protocol::java::client::play::RenderType::Integer,
                None,
            ),
            &be_update,
        );

        self.objectives.insert(objective.name.clone(), objective);
    }

    pub fn update_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        objective: BedrockObjective,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let be_update = BSetDisplayObjective {
            display_slot_name: "sidebar".to_string(),
            objective_name: objective.name.clone(),
            objective_display_name: objective.display_name.clone(),
            criteria_name: "dummy".to_string(),
            sort_order: VarInt(match objective.sort_order {
                BedrockSortOrder::Ascending => 0,
                BedrockSortOrder::Descending => 1,
            }),
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CUpdateObjectives::new(
                objective.name.clone(),
                pumpkin_protocol::java::client::play::Mode::Update,
                TextComponent::text(objective.display_name.clone()),
                pumpkin_protocol::java::client::play::RenderType::Integer,
                None,
            ),
            &be_update,
        );

        self.objectives.insert(objective.name.clone(), objective);
    }

    pub fn remove_objective(&mut self, target: &impl ScoreboardTarget, name: &str) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let be_remove = BRemoveObjective {
            objective_name: name.to_string(),
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CUpdateObjectives::new(
                name.to_string(),
                pumpkin_protocol::java::client::play::Mode::Remove,
                TextComponent::text(""),
                pumpkin_protocol::java::client::play::RenderType::Integer,
                None,
            ),
            &be_remove,
        );
        self.objectives.remove(name);
        self.display_slots.retain(|_, v| v != name);
        self.scores.retain(|(_, obj), _| obj != name);
    }

    pub fn set_display_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        slot: BedrockDisplaySlot,
        objective_name: Option<&str>,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let obj_name_str = objective_name.unwrap_or("");
        let display_name = objective_name
            .and_then(|name| self.objectives.get(name))
            .map_or_else(|| obj_name_str.to_string(), |o| o.display_name.clone());

        let be_display = BSetDisplayObjective {
            display_slot_name: slot.to_str().to_string(),
            objective_name: obj_name_str.to_string(),
            objective_display_name: display_name,
            criteria_name: "dummy".to_string(),
            sort_order: VarInt(0),
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CDisplayObjective::new(
                match slot {
                    BedrockDisplaySlot::PlayerList => ScoreboardDisplaySlot::List,
                    BedrockDisplaySlot::Sidebar => ScoreboardDisplaySlot::Sidebar,
                    BedrockDisplaySlot::BelowName => ScoreboardDisplaySlot::BelowName,
                },
                obj_name_str.to_string(),
            ),
            &be_display,
        );

        if let Some(name) = objective_name {
            self.display_slots.insert(slot, name.to_string());
        } else {
            self.display_slots.remove(&slot);
        }
    }

    pub fn clear_display_objective(
        &mut self,
        target: &impl ScoreboardTarget,
        slot: BedrockDisplaySlot,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        self.set_display_objective(&target, slot, None);
    }

    pub fn update_score(
        &mut self,
        target: &impl ScoreboardTarget,
        entity_name: &str,
        objective_name: &str,
        value: i32,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let score = ScoreboardScore::new(
            entity_name.to_string(),
            objective_name.to_string(),
            VarInt(value),
            None,
            None,
        );
        let be_score = BSetScore {
            action: VarInt(0),
            entries: vec![bedrock_score_entry(entity_name, objective_name, value)],
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CUpdateScore::new(
                score.entity_name.clone(),
                score.objective_name.clone(),
                score.value,
                score.display_name.clone(),
                score.number_format,
            ),
            &be_score,
        );

        self.scores
            .insert((entity_name.to_string(), objective_name.to_string()), value);
    }

    pub fn add_score(
        &mut self,
        target: &impl ScoreboardTarget,
        entity_name: impl Into<String>,
        objective_name: impl Into<String>,
        delta: i32,
    ) -> i32 {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let entity_s = entity_name.into();
        let obj_s = objective_name.into();
        let current = self
            .scores
            .get(&(entity_s.clone(), obj_s.clone()))
            .copied()
            .unwrap_or(0);
        let new_val = current + delta;
        self.update_score(&target, &entity_s, &obj_s, new_val);
        new_val
    }

    pub fn remove_score(
        &mut self,
        target: &impl ScoreboardTarget,
        entity_name: &str,
        objective_name: &str,
    ) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let be_score = BSetScore {
            action: VarInt(1),
            entries: vec![bedrock_score_entry(entity_name, objective_name, 0)],
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CResetScore::new(
                entity_name.to_string(),
                Some(objective_name.to_string()),
            ),
            &be_score,
        );
        self.scores
            .remove(&(entity_name.to_string(), objective_name.to_string()));
    }

    pub fn reset_scores_for_entity(&mut self, target: &impl ScoreboardTarget, entity_name: &str) {
        let target = DeferredTarget {
            target,
            suppressed: self.deferred,
        };
        let be_score = BSetScore {
            action: VarInt(1),
            entries: self
                .scores
                .keys()
                .filter(|(entity, _)| entity == entity_name)
                .map(|(entity, objective)| bedrock_score_entry(entity, objective, 0))
                .collect(),
        };
        target.send_editioned(
            &pumpkin_protocol::java::client::play::CResetScore::new(entity_name.to_string(), None),
            &be_score,
        );
        self.scores.retain(|(entity, _), _| entity != entity_name);
    }

    pub fn send_to_player(&self, player: &Player) {
        self.send_to(player);
    }

    pub fn send_to(&self, target: &impl ScoreboardTarget) {
        for objective in self.objectives.values() {
            let be_update = BSetDisplayObjective {
                display_slot_name: "sidebar".to_string(),
                objective_name: objective.name.clone(),
                objective_display_name: objective.display_name.clone(),
                criteria_name: "dummy".to_string(),
                sort_order: VarInt(match objective.sort_order {
                    BedrockSortOrder::Ascending => 0,
                    BedrockSortOrder::Descending => 1,
                }),
            };
            target.send_editioned(
                &pumpkin_protocol::java::client::play::CUpdateObjectives::new(
                    objective.name.clone(),
                    pumpkin_protocol::java::client::play::Mode::Add,
                    TextComponent::text(objective.display_name.clone()),
                    pumpkin_protocol::java::client::play::RenderType::Integer,
                    None,
                ),
                &be_update,
            );
        }

        for (slot, objective_name) in &self.display_slots {
            let display_name = self
                .objectives
                .get(objective_name)
                .map_or_else(|| objective_name.clone(), |o| o.display_name.clone());
            let be_display = BSetDisplayObjective {
                display_slot_name: slot.to_str().to_string(),
                objective_name: objective_name.clone(),
                objective_display_name: display_name,
                criteria_name: "dummy".to_string(),
                sort_order: VarInt(0),
            };
            target.send_editioned(
                &pumpkin_protocol::java::client::play::CDisplayObjective::new(
                    match slot {
                        BedrockDisplaySlot::PlayerList => ScoreboardDisplaySlot::List,
                        BedrockDisplaySlot::Sidebar => ScoreboardDisplaySlot::Sidebar,
                        BedrockDisplaySlot::BelowName => ScoreboardDisplaySlot::BelowName,
                    },
                    objective_name.clone(),
                ),
                &be_display,
            );
        }

        for ((entity_name, objective_name), value) in &self.scores {
            let score = ScoreboardScore::new(
                entity_name.clone(),
                objective_name.clone(),
                VarInt(*value),
                None,
                None,
            );
            let be_score = BSetScore {
                action: VarInt(0),
                entries: vec![bedrock_score_entry(entity_name, objective_name, *value)],
            };
            target.send_editioned(
                &pumpkin_protocol::java::client::play::CUpdateScore::new(
                    score.entity_name,
                    score.objective_name,
                    score.value,
                    score.display_name,
                    score.number_format,
                ),
                &be_score,
            );
        }
    }
}

#[derive(Default, Debug)]
pub struct ScoreboardBuilder {
    scoreboard: Scoreboard,
}

impl ScoreboardBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn objective(mut self, name: impl Into<String>, display_name: TextComponent) -> Self {
        let obj = ScoreboardObjective::new(
            name.into(),
            display_name,
            RenderType::Integer,
            None,
            "dummy",
        );
        self.scoreboard.objectives.insert(obj.name.clone(), obj);
        self
    }

    #[must_use]
    pub fn objective_with_render(
        mut self,
        name: impl Into<String>,
        display_name: TextComponent,
        render_type: RenderType,
    ) -> Self {
        let obj = ScoreboardObjective::new(name.into(), display_name, render_type, None, "dummy");
        self.scoreboard.objectives.insert(obj.name.clone(), obj);
        self
    }

    #[must_use]
    pub fn display_slot(
        mut self,
        slot: ScoreboardDisplaySlot,
        objective_name: impl Into<String>,
    ) -> Self {
        self.scoreboard
            .display_slots
            .insert(slot, objective_name.into());
        self
    }

    #[must_use]
    pub fn score(
        mut self,
        entity_name: impl Into<String>,
        objective_name: impl Into<String>,
        value: i32,
    ) -> Self {
        let entity_s = entity_name.into();
        let obj_s = objective_name.into();
        let score =
            ScoreboardScore::new(entity_s.clone(), obj_s.clone(), VarInt(value), None, None);
        self.scoreboard
            .scores
            .entry(obj_s)
            .or_default()
            .insert(entity_s, score);
        self
    }

    #[must_use]
    pub fn team(mut self, team: Team) -> Self {
        self.scoreboard.teams.insert(team.name.clone(), team);
        self
    }

    #[must_use]
    pub fn build(self) -> Scoreboard {
        self.scoreboard
    }
}

#[derive(Default, Debug)]
pub struct BedrockScoreboardBuilder {
    scoreboard: BedrockScoreboard,
}

impl BedrockScoreboardBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn objective(
        mut self,
        name: impl Into<String>,
        display_name: impl Into<String>,
        sort_order: BedrockSortOrder,
    ) -> Self {
        let name = name.into();
        let obj = BedrockObjective {
            name: name.clone(),
            display_name: display_name.into(),
            sort_order,
        };
        self.scoreboard.objectives.insert(name, obj);
        self
    }

    #[must_use]
    pub fn display_slot(
        mut self,
        slot: BedrockDisplaySlot,
        objective_name: impl Into<String>,
    ) -> Self {
        self.scoreboard
            .display_slots
            .insert(slot, objective_name.into());
        self
    }

    #[must_use]
    pub fn score(
        mut self,
        entity_name: impl Into<String>,
        objective_name: impl Into<String>,
        value: i32,
    ) -> Self {
        self.scoreboard
            .scores
            .insert((entity_name.into(), objective_name.into()), value);
        self
    }

    #[must_use]
    pub fn build(self) -> BedrockScoreboard {
        self.scoreboard
    }
}

const DISPLAY_SLOTS: [ScoreboardDisplaySlot; 19] = [
    ScoreboardDisplaySlot::List,
    ScoreboardDisplaySlot::Sidebar,
    ScoreboardDisplaySlot::BelowName,
    ScoreboardDisplaySlot::TeamBlack,
    ScoreboardDisplaySlot::TeamDarkBlue,
    ScoreboardDisplaySlot::TeamDarkGreen,
    ScoreboardDisplaySlot::TeamDarkAqua,
    ScoreboardDisplaySlot::TeamDarkRed,
    ScoreboardDisplaySlot::TeamDarkPurple,
    ScoreboardDisplaySlot::TeamGold,
    ScoreboardDisplaySlot::TeamGray,
    ScoreboardDisplaySlot::TeamDarkGray,
    ScoreboardDisplaySlot::TeamBlue,
    ScoreboardDisplaySlot::TeamGreen,
    ScoreboardDisplaySlot::TeamAqua,
    ScoreboardDisplaySlot::TeamRed,
    ScoreboardDisplaySlot::TeamLightPurple,
    ScoreboardDisplaySlot::TeamYellow,
    ScoreboardDisplaySlot::TeamWhite,
];

struct DeferredTarget<'a, T> {
    target: &'a T,
    suppressed: bool,
}
impl<T: ScoreboardTarget> ScoreboardTarget for DeferredTarget<'_, T> {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        java: &J,
        bedrock: &B,
    ) {
        if !self.suppressed {
            self.target.send_editioned(java, bedrock);
        }
    }
    fn send_je<J: ClientPacket + Sync>(&self, packet: &J) {
        if !self.suppressed {
            self.target.send_je(packet);
        }
    }
}

mod render_type_serde {
    use super::RenderType;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &RenderType, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bool(matches!(value, RenderType::Hearts))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<RenderType, D::Error> {
        Ok(if bool::deserialize(deserializer)? {
            RenderType::Hearts
        } else {
            RenderType::Integer
        })
    }
}
mod score_value_serde {
    use super::VarInt;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &VarInt, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(value.0)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<VarInt, D::Error> {
        i32::deserialize(deserializer).map(VarInt)
    }
}
mod number_format_serde {
    use super::{NumberFormat, TextComponent};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    #[derive(Serialize, Deserialize)]
    enum Format {
        Blank,
        Styled(pumpkin_util::text::style::Style),
        Fixed(TextComponent),
    }
    pub(super) fn valid(value: &serde_json::Value) -> bool {
        serde_json::from_value::<Option<Format>>(value.clone()).is_ok()
    }
    pub fn serialize<S: Serializer>(
        value: &Option<NumberFormat>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(|value| match value {
                NumberFormat::Blank => Format::Blank,
                NumberFormat::Styled(style) => Format::Styled(style.clone()),
                NumberFormat::Fixed(text) => Format::Fixed(text.clone()),
            })
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<NumberFormat>, D::Error> {
        Ok(
            Option::<Format>::deserialize(deserializer)?.map(|value| match value {
                Format::Blank => NumberFormat::Blank,
                Format::Styled(style) => NumberFormat::Styled(style),
                Format::Fixed(text) => NumberFormat::Fixed(text),
            }),
        )
    }
}

#[derive(Debug)]
pub struct ScoreboardBusy;
impl std::fmt::Display for ScoreboardBusy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Scoreboard mutation capacity exhausted")
    }
}
impl std::error::Error for ScoreboardBusy {}
impl ScoreboardBusy {
    pub fn command_error(
        self,
    ) -> pumpkin_command::errors::command_syntax_error::CommandSyntaxError {
        pumpkin_command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
            .create_without_context(TextComponent::text(self.to_string()))
    }
}

pub struct WorldScoreboard {
    snapshot: ArcSwap<Scoreboard>,
    team_snapshot: Option<Arc<ArcSwap<FxHashMap<String, Team>>>>,
    score_snapshot: Option<Arc<ArcSwap<FxHashMap<String, FxHashMap<String, ScoreboardScore>>>>>,
    server: std::sync::Weak<crate::server::Server>,
    world: String,
}

impl WorldScoreboard {
    pub fn new(
        mut board: Scoreboard,
        server: std::sync::Weak<crate::server::Server>,
        world: String,
    ) -> Self {
        Self {
            team_snapshot: board.team_snapshot.take(),
            score_snapshot: board.score_snapshot.take(),
            snapshot: ArcSwap::from_pointee(board),
            server,
            world,
        }
    }
    pub fn snapshot(&self) -> Arc<Scoreboard> {
        self.snapshot.load_full()
    }
    pub fn edit(&self) -> Result<ScoreboardEdit, ScoreboardBusy> {
        let server = self.server.upgrade().ok_or(ScoreboardBusy)?;
        let permit = server
            .cluster_scoreboard_updates
            .clone()
            .try_reserve_owned()
            .map_err(|_| ScoreboardBusy)?;
        let before = self.snapshot();
        let mut board = before.as_ref().clone();
        board.deferred = true;
        Ok(ScoreboardEdit {
            before,
            board,
            permit: Some(permit),
            world: self.world.clone(),
        })
    }
    pub fn submit_scores(
        &self,
        semantic: crate::server::cluster_scoreboard::ScoreMutation,
        feedback: Option<Arc<crate::command::CommandSource>>,
    ) -> Result<pumpkin_command::deferred::DeferredCommandToken, ScoreboardBusy> {
        let server = self.server.upgrade().ok_or(ScoreboardBusy)?;
        if server
            .cluster_global_shutdown
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(ScoreboardBusy);
        }
        let permit = server
            .cluster_scoreboard_updates
            .clone()
            .try_reserve_owned()
            .map_err(|_| ScoreboardBusy)?;
        admit_score_update(self.world.clone(), semantic, feedback, permit)
    }
    pub fn apply<T>(
        &self,
        operation: impl FnOnce(&mut Scoreboard) -> T,
    ) -> Result<T, ScoreboardBusy> {
        let mut edit = self.edit()?;
        let result = operation(&mut edit);
        edit.commit()?;
        Ok(result)
    }
    pub(crate) fn install(&self, world: &World, desired: Scoreboard) {
        let mut board = self.snapshot().as_ref().clone();
        board.reconcile(world, desired);
        self.publish(board);
    }
    fn publish(&self, mut board: Scoreboard) {
        board.team_snapshot = None;
        board.score_snapshot = None;
        if let Some(snapshot) = &self.team_snapshot {
            snapshot.store(Arc::new(board.teams.clone()));
        }
        if let Some(snapshot) = &self.score_snapshot {
            snapshot.store(Arc::new(board.scores.clone()));
        }
        self.snapshot.store(Arc::new(board));
    }
}

fn admit_score_update(
    world: String,
    semantic: crate::server::cluster_scoreboard::ScoreMutation,
    feedback: Option<Arc<crate::command::CommandSource>>,
    permit: tokio::sync::mpsc::OwnedPermit<crate::server::cluster_scoreboard::ScoreboardUpdate>,
) -> Result<pumpkin_command::deferred::DeferredCommandToken, ScoreboardBusy> {
    let token = pumpkin_command::deferred::DeferredCommandToken(*uuid::Uuid::new_v4().as_bytes());
    let mut update = crate::server::cluster_scoreboard::ScoreboardUpdate {
        world,
        changes: Vec::new(),
        semantic: Some(semantic),
        feedback,
        completion: None,
    };
    if !crate::server::cluster_scoreboard::valid_update(&update) {
        return Err(ScoreboardBusy);
    }
    if let Some(source) = &update.feedback {
        update.completion = Some(
            pumpkin_command::deferred::DeferredCommandCompletion::reserve(
                crate::server::capture_completion_source(Arc::clone(source)),
                token,
            )
            .map_err(|_| ScoreboardBusy)?,
        );
    }
    permit.send(update);
    Ok(token)
}

pub struct ScoreboardEdit {
    before: Arc<Scoreboard>,
    board: Scoreboard,
    permit:
        Option<tokio::sync::mpsc::OwnedPermit<crate::server::cluster_scoreboard::ScoreboardUpdate>>,
    world: String,
}
impl std::ops::Deref for ScoreboardEdit {
    type Target = Scoreboard;
    fn deref(&self) -> &Scoreboard {
        &self.board
    }
}
impl std::ops::DerefMut for ScoreboardEdit {
    fn deref_mut(&mut self) -> &mut Scoreboard {
        &mut self.board
    }
}
impl ScoreboardEdit {
    pub fn commit(&mut self) -> Result<(), ScoreboardBusy> {
        let changes = self.board.changes_from(&self.before);
        if !changes.is_empty() {
            let update = crate::server::cluster_scoreboard::ScoreboardUpdate {
                world: self.world.clone(),
                changes,
                semantic: None,
                feedback: None,
                completion: None,
            };
            if !crate::server::cluster_scoreboard::valid_update(&update) {
                self.permit.take();
                return Err(ScoreboardBusy);
            }
            self.permit.take().ok_or(ScoreboardBusy)?.send(update);
        }
        Ok(())
    }
}

fn default_visibility() -> String {
    "always".into()
}

#[cfg(test)]
mod owned_scoreboard_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn team() -> Team {
        Team {
            name: "red".into(),
            display_name: TextComponent::text("Red"),
            options: 0,
            nametag_visibility: NameTagVisibility::Always,
            collision_rule: CollisionRule::Always,
            death_message_visibility: "always".into(),
            color: NamedColor::Red,
            player_prefix: TextComponent::empty(),
            player_suffix: TextComponent::empty(),
            players: vec![],
        }
    }

    fn merge_change(
        base: &BTreeMap<Vec<String>, serde_json::Value>,
        changed: &Scoreboard,
        into: &mut BTreeMap<Vec<String>, serde_json::Value>,
    ) {
        for (key, value) in changed.changes_from(&Scoreboard::from_records(base)) {
            if let Some(value) = value {
                into.insert(key, value);
            } else {
                into.remove(&key);
            }
        }
    }

    #[test]
    fn terminal_capacity_is_reserved_before_score_descriptor_admission() {
        use pumpkin_command::deferred::{
            DeferredCommandToken, DeferredCompletionTerminal, DeferredCompletionUnavailable,
        };
        use pumpkin_command::source::{ResultValueTaker, ReturnValue, ReturnValueCallable};
        type Event = (DeferredCommandToken, Option<ReturnValue>);
        struct Terminal(tokio::sync::mpsc::OwnedPermit<Event>);
        impl DeferredCompletionTerminal for Terminal {
            fn complete(self: Box<Self>, token: DeferredCommandToken, value: Option<ReturnValue>) {
                let Self(permit) = *self;
                permit.send((token, value));
            }
        }
        struct Capture(tokio::sync::mpsc::Sender<Event>);
        impl ReturnValueCallable for Capture {
            fn call(&self, _: ReturnValue) {}
            fn reserve_deferred_terminal(
                &self,
                _: DeferredCommandToken,
            ) -> Result<Option<Box<dyn DeferredCompletionTerminal>>, DeferredCompletionUnavailable>
            {
                let permit = self
                    .0
                    .clone()
                    .try_reserve_owned()
                    .map_err(|_| DeferredCompletionUnavailable)?;
                Ok(Some(Box::new(Terminal(permit))))
            }
        }
        let (terminal, _terminal_rx) = tokio::sync::mpsc::channel(1);
        let mut source = crate::command::CommandSource::dummy();
        source.command_result_taker = ResultValueTaker(vec![Arc::new(Capture(terminal))]);
        let source = Arc::new(source);
        let (updates, mut received) = tokio::sync::mpsc::channel(2);
        let mutation = || crate::server::cluster_scoreboard::ScoreMutation {
            targets: vec!["Alice".into()],
            objective: "kills".into(),
            action: crate::server::cluster_scoreboard::ScoreAction::Delta(1),
        };
        let permit = updates.clone().try_reserve_owned().unwrap();
        assert!(
            admit_score_update(
                "0:overworld".into(),
                mutation(),
                Some(Arc::clone(&source)),
                permit
            )
            .is_ok()
        );
        let permit = updates.clone().try_reserve_owned().unwrap();
        assert!(
            admit_score_update(
                "0:overworld".into(),
                mutation(),
                Some(Arc::clone(&source)),
                permit
            )
            .is_err()
        );
        let accepted = received.try_recv().unwrap();
        assert!(matches!(
            accepted.semantic.as_ref().unwrap().action,
            crate::server::cluster_scoreboard::ScoreAction::Delta(1)
        ));
        assert!(accepted.completion.is_some());
        assert!(received.try_recv().is_err());
        assert_eq!(updates.capacity(), 2);
        drop(accepted);
        let permit = updates.clone().try_reserve_owned().unwrap();
        assert!(admit_score_update("0:overworld".into(), mutation(), Some(source), permit).is_ok());
    }

    #[test]
    fn immutable_world_read_clone_cannot_publish_mutations_before_owned_install() {
        let teams = Arc::new(ArcSwap::from_pointee(FxHashMap::default()));
        let scores = Arc::new(ArcSwap::from_pointee(FxHashMap::default()));
        let mut board = Scoreboard::with_team_snapshot(Arc::clone(&teams))
            .with_score_snapshot(Arc::clone(&scores));
        board.add_team(&NoTarget, team());
        board.add_objective(
            &NoTarget,
            ScoreboardObjective::new(
                "kills",
                TextComponent::text("Kills"),
                RenderType::Integer,
                None,
                "dummy",
            ),
        );
        board.set_score_value(&NoTarget, "Alice", "kills", 2);
        let owner = WorldScoreboard::new(board, std::sync::Weak::new(), "0:overworld".into());
        let original = owner.snapshot();
        let mut read_clone = original.as_ref().clone();
        read_clone.remove_team(&NoTarget, "red");
        read_clone.set_score_value(&NoTarget, "Alice", "kills", 99);
        assert!(teams.load().contains_key("red"));
        assert_eq!(scores.load()["kills"]["Alice"].value.0, 2);
        assert!(owner.snapshot().get_team("red").is_some());
        owner.publish(read_clone);
        assert!(!teams.load().contains_key("red"));
        assert_eq!(scores.load()["kills"]["Alice"].value.0, 99);
        assert!(original.get_team("red").is_some());
        assert_eq!(original.get_score_value("Alice", "kills"), Some(2));
    }

    #[test]
    fn independent_team_property_and_membership_changes_survive_stale_edits() {
        let mut board = Scoreboard::new();
        board.add_team(&NoTarget, team());
        let base = board.records();
        let mut prefix_edit = board.clone();
        prefix_edit.get_team_mut("red").unwrap().player_prefix = TextComponent::text("[Red] ");
        let mut membership_edit = board.clone();
        membership_edit.add_player_to_team(&NoTarget, "red", "Alice".into());
        let mut first = base.clone();
        merge_change(&base, &prefix_edit, &mut first);
        merge_change(&base, &membership_edit, &mut first);
        let mut second = base.clone();
        merge_change(&base, &membership_edit, &mut second);
        merge_change(&base, &prefix_edit, &mut second);
        let actual = Scoreboard::from_records(&first);
        assert_eq!(
            actual.records(),
            Scoreboard::from_records(&second).records()
        );
        let actual_team = actual.get_team("red").unwrap();
        assert_eq!(actual_team.player_prefix, TextComponent::text("[Red] "));
        assert_eq!(actual_team.players, ["Alice"]);
        assert!(board.get_team("red").unwrap().players.is_empty());
    }

    #[test]
    fn deleted_objective_and_team_do_not_expose_dangling_scores_or_members() {
        let mut board = Scoreboard::new();
        board.add_objective(
            &NoTarget,
            ScoreboardObjective::new(
                "kills",
                TextComponent::text("Kills"),
                RenderType::Integer,
                Some(NumberFormat::Blank),
                "dummy",
            ),
        );
        board.set_score_value(&NoTarget, "Alice", "kills", 7);
        board.add_team(&NoTarget, team());
        board.add_player_to_team(&NoTarget, "red", "Alice".into());
        let retained = board.records();
        let mut deleted = retained.clone();
        deleted.retain(|key, _| key[0] != "objective" && key[0] != "team");
        let actual = Scoreboard::from_records(&deleted);
        assert!(actual.get_entity_team("Alice").is_none());
        assert_eq!(actual.get_score_value("Alice", "kills"), None);
        let original = Scoreboard::from_records(&retained);
        assert_eq!(original.get_score_value("Alice", "kills"), Some(7));
        assert_eq!(original.get_entity_team("Alice").unwrap().name, "red");
    }
}

pub struct CustomScoreboardRuntime {
    snapshot: Arc<ArcSwap<Option<crate::entity::player::CustomScoreboard>>>,
    client: Option<std::sync::Weak<crate::net::ClientPlatform>>,
    world: std::sync::Weak<World>,
    updates: tokio::sync::mpsc::Sender<CustomScoreboardUpdate>,
    uuid: uuid::Uuid,
}

pub struct CustomScoreboardUpdate {
    snapshot: Arc<ArcSwap<Option<crate::entity::player::CustomScoreboard>>>,
    client: Option<std::sync::Weak<crate::net::ClientPlatform>>,
    world: std::sync::Weak<World>,
    _uuid: uuid::Uuid,
    before: Option<crate::entity::player::CustomScoreboard>,
    after: Option<crate::entity::player::CustomScoreboard>,
    delta: Option<CustomScoreDelta>,
}

struct CustomScoreDelta {
    entity: String,
    objective: String,
    delta: i32,
    bedrock: bool,
}

impl CustomScoreboardRuntime {
    pub fn new(
        updates: tokio::sync::mpsc::Sender<CustomScoreboardUpdate>,
        uuid: uuid::Uuid,
    ) -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::from_pointee(None)),
            client: None,
            world: std::sync::Weak::new(),
            updates,
            uuid,
        }
    }
    pub fn with_target(
        mut self,
        client: &Arc<crate::net::ClientPlatform>,
        world: &Arc<World>,
    ) -> Self {
        self.client = Some(Arc::downgrade(client));
        self.world = Arc::downgrade(world);
        self
    }
    pub fn snapshot(&self) -> Arc<Option<crate::entity::player::CustomScoreboard>> {
        self.snapshot.load_full()
    }
    pub fn edit(&self) -> Result<CustomScoreboardEdit, ScoreboardBusy> {
        let permit = self
            .updates
            .clone()
            .try_reserve_owned()
            .map_err(|_| ScoreboardBusy)?;
        let before = self.snapshot();
        Ok(CustomScoreboardEdit {
            before: before.as_ref().clone(),
            after: before.as_ref().clone(),
            uuid: self.uuid,
            snapshot: Arc::clone(&self.snapshot),
            client: self.client.clone(),
            world: self.world.clone(),
            permit: Some(permit),
        })
    }
    pub fn add_score(
        &self,
        entity: String,
        objective: String,
        delta: i32,
        bedrock: bool,
    ) -> Result<i32, ScoreboardBusy> {
        if entity.len() > 4096 || objective.len() > 4096 {
            return Err(ScoreboardBusy);
        }
        let permit = self
            .updates
            .clone()
            .try_reserve_owned()
            .map_err(|_| ScoreboardBusy)?;
        let snapshot = self.snapshot();
        let value = match snapshot.as_ref() {
            Some(crate::entity::player::CustomScoreboard::Java(board)) if !bedrock => {
                board.get_score_value(&entity, &objective).unwrap_or(0)
            }
            Some(crate::entity::player::CustomScoreboard::Bedrock(board)) if bedrock => board
                .scores
                .get(&(entity.clone(), objective.clone()))
                .copied()
                .unwrap_or(0),
            _ => 0,
        };
        permit.send(CustomScoreboardUpdate {
            snapshot: Arc::clone(&self.snapshot),
            client: self.client.clone(),
            world: self.world.clone(),
            _uuid: self.uuid,
            before: None,
            after: None,
            delta: Some(CustomScoreDelta {
                entity,
                objective,
                delta,
                bedrock,
            }),
        });
        Ok(value.wrapping_add(delta))
    }
    pub fn set(
        &self,
        value: Option<crate::entity::player::CustomScoreboard>,
    ) -> Result<(), ScoreboardBusy> {
        let mut edit = self.edit()?;
        edit.after = value;
        edit.commit()
    }
}

pub struct CustomScoreboardEdit {
    snapshot: Arc<ArcSwap<Option<crate::entity::player::CustomScoreboard>>>,
    client: Option<std::sync::Weak<crate::net::ClientPlatform>>,
    world: std::sync::Weak<World>,
    before: Option<crate::entity::player::CustomScoreboard>,
    after: Option<crate::entity::player::CustomScoreboard>,
    uuid: uuid::Uuid,
    permit: Option<tokio::sync::mpsc::OwnedPermit<CustomScoreboardUpdate>>,
}
impl std::ops::Deref for CustomScoreboardEdit {
    type Target = Option<crate::entity::player::CustomScoreboard>;
    fn deref(&self) -> &Self::Target {
        &self.after
    }
}
impl std::ops::DerefMut for CustomScoreboardEdit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.after
    }
}
impl CustomScoreboardEdit {
    pub fn as_mut(&mut self) -> Option<&mut crate::entity::player::CustomScoreboard> {
        match &mut self.after {
            Some(crate::entity::player::CustomScoreboard::Java(board)) => board.deferred = true,
            Some(crate::entity::player::CustomScoreboard::Bedrock(board)) => board.deferred = true,
            None => {}
        }
        self.after.as_mut()
    }
    pub fn commit(&mut self) -> Result<(), ScoreboardBusy> {
        let valid = match &mut self.after {
            Some(crate::entity::player::CustomScoreboard::Java(board)) => {
                board.deferred = false;
                board.team_snapshot = None;
                board.score_snapshot = None;
                let changes = board
                    .records()
                    .into_iter()
                    .map(|(key, value)| (key, Some(value)))
                    .collect();
                crate::server::cluster_scoreboard::valid_update(
                    &crate::server::cluster_scoreboard::ScoreboardUpdate {
                        world: String::new(),
                        changes,
                        semantic: None,
                        feedback: None,
                        completion: None,
                    },
                )
            }
            Some(crate::entity::player::CustomScoreboard::Bedrock(board)) => {
                board.deferred = false;
                let bytes = board
                    .objectives
                    .values()
                    .map(|value| value.name.len() + value.display_name.len() + 128)
                    .sum::<usize>()
                    + board
                        .scores
                        .keys()
                        .map(|(entity, objective)| entity.len() + objective.len() + 64)
                        .sum::<usize>()
                    + board
                        .display_slots
                        .values()
                        .map(|name| name.len() + 64)
                        .sum::<usize>();
                board.objectives.len() + board.scores.len() <= 4096 && bytes <= 800_000
            }
            None => true,
        };
        if !valid {
            self.permit.take();
            return Err(ScoreboardBusy);
        }
        self.permit
            .take()
            .ok_or(ScoreboardBusy)?
            .send(CustomScoreboardUpdate {
                _uuid: self.uuid,
                snapshot: Arc::clone(&self.snapshot),
                client: self.client.clone(),
                world: self.world.clone(),
                before: self.before.take(),
                after: self.after.take(),
                delta: None,
            });
        Ok(())
    }
}

pub(crate) fn apply_custom_scoreboard(
    server: &crate::server::Server,
    update: CustomScoreboardUpdate,
) {
    let target =
        ConnectionScoreboardTarget(update.client.as_ref().and_then(std::sync::Weak::upgrade));
    let current = update.snapshot.load_full();
    let mut current = current.as_ref().clone();
    if let Some(delta) = update.delta {
        apply_custom_delta(&mut current, delta, &target);
    } else {
        reconcile_custom_scoreboard(&mut current, update.before, update.after, &target);
    }
    if current.is_none() {
        let world = server
            .get_player_by_uuid(update._uuid)
            .map(|player| player.world())
            .or_else(|| update.world.upgrade());
        if let Some(world) = world {
            world.scoreboard.snapshot().send_to(&target);
        }
    }
    update.snapshot.store(Arc::new(current));
}

fn apply_custom_delta(
    current: &mut Option<crate::entity::player::CustomScoreboard>,
    delta: CustomScoreDelta,
    target: &impl ScoreboardTarget,
) {
    use crate::entity::player::CustomScoreboard;
    if delta.bedrock {
        if !matches!(current, Some(CustomScoreboard::Bedrock(_))) {
            reconcile_custom_scoreboard(
                current,
                current.clone(),
                Some(CustomScoreboard::Bedrock(BedrockScoreboard::default())),
                target,
            );
        }
        let Some(CustomScoreboard::Bedrock(board)) = current else {
            unreachable!()
        };
        let value = board
            .scores
            .get(&(delta.entity.clone(), delta.objective.clone()))
            .copied()
            .unwrap_or(0)
            .wrapping_add(delta.delta);
        board.update_score(target, &delta.entity, &delta.objective, value);
    } else {
        if !matches!(current, Some(CustomScoreboard::Java(_))) {
            reconcile_custom_scoreboard(
                current,
                current.clone(),
                Some(CustomScoreboard::Java(Scoreboard::new())),
                target,
            );
        }
        let Some(CustomScoreboard::Java(board)) = current else {
            unreachable!()
        };
        let value = board
            .get_score_value(&delta.entity, &delta.objective)
            .unwrap_or(0)
            .wrapping_add(delta.delta);
        board.set_score_value(target, delta.entity, delta.objective, value);
    }
}

struct ConnectionScoreboardTarget(Option<Arc<crate::net::ClientPlatform>>);
impl ScoreboardTarget for ConnectionScoreboardTarget {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        java: &J,
        bedrock: &B,
    ) {
        if let Some(client) = &self.0 {
            client.send_editioned(java, bedrock);
        }
    }
    fn send_je<J: ClientPacket + Sync>(&self, packet: &J) {
        if let Some(client) = &self.0 {
            client.send_je(packet);
        }
    }
}

fn reconcile_custom_scoreboard(
    current: &mut Option<crate::entity::player::CustomScoreboard>,
    before: Option<crate::entity::player::CustomScoreboard>,
    after: Option<crate::entity::player::CustomScoreboard>,
    target: &impl ScoreboardTarget,
) {
    use crate::entity::player::CustomScoreboard;
    let before = before.or_else(|| match &after {
        Some(CustomScoreboard::Java(_)) => Some(CustomScoreboard::Java(Scoreboard::new())),
        Some(CustomScoreboard::Bedrock(_)) => {
            Some(CustomScoreboard::Bedrock(BedrockScoreboard::default()))
        }
        None => None,
    });
    match (current, before, after) {
        (
            Some(CustomScoreboard::Java(current)),
            Some(CustomScoreboard::Java(before)),
            Some(CustomScoreboard::Java(after)),
        ) => {
            let mut records = current.records();
            for (key, value) in after.changes_from(&before) {
                if let Some(value) = value {
                    records.insert(key, value);
                } else {
                    records.remove(&key);
                }
            }
            current.reconcile(target, Scoreboard::from_records(&records));
        }
        (
            Some(CustomScoreboard::Bedrock(current)),
            Some(CustomScoreboard::Bedrock(before)),
            Some(CustomScoreboard::Bedrock(after)),
        ) => {
            for name in before
                .objectives
                .keys()
                .filter(|name| !after.objectives.contains_key(*name))
            {
                current.remove_objective(target, name);
            }
            for (name, objective) in &after.objectives {
                if before.objectives.get(name) != Some(objective) {
                    if current.objectives.contains_key(name) {
                        current.update_objective(target, objective.clone());
                    } else {
                        current.add_objective(target, objective.clone());
                    }
                }
            }
            for ((entity, objective), _) in before
                .scores
                .iter()
                .filter(|(key, _)| !after.scores.contains_key(*key))
            {
                current.remove_score(target, entity, objective);
            }
            for ((entity, objective), value) in &after.scores {
                if before.scores.get(&(entity.clone(), objective.clone())) != Some(value) {
                    current.update_score(target, entity, objective, *value);
                }
            }
            for slot in [
                BedrockDisplaySlot::PlayerList,
                BedrockDisplaySlot::Sidebar,
                BedrockDisplaySlot::BelowName,
            ] {
                if before.display_slots.get(&slot) != after.display_slots.get(&slot) {
                    current.set_display_objective(
                        target,
                        slot,
                        after.display_slots.get(&slot).map(String::as_str),
                    );
                }
            }
        }
        (current, _, after) => {
            match current.as_mut() {
                Some(CustomScoreboard::Java(board)) => board.reconcile(target, Scoreboard::new()),
                Some(CustomScoreboard::Bedrock(board)) => {
                    for name in board.objectives.keys().cloned().collect::<Vec<_>>() {
                        board.remove_objective(target, &name);
                    }
                }
                None => {}
            }
            *current = after;
            match current.as_ref() {
                Some(CustomScoreboard::Java(board)) => board.send_to(target),
                Some(CustomScoreboard::Bedrock(board)) => board.send_to(target),
                None => {}
            }
        }
    }
}

#[cfg(test)]
mod custom_scoreboard_admission_tests {
    use super::*;
    use crate::entity::player::CustomScoreboard;

    struct CountPackets(std::sync::atomic::AtomicUsize);
    impl ScoreboardTarget for CountPackets {
        fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(&self, _: &J, _: &B) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn send_je<J: ClientPacket + Sync>(&self, _: &J) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn queued_custom_java_and_bedrock_additions_keep_intent_and_reject_full_admission() {
        for bedrock in [false, true] {
            let (tx, mut rx) = tokio::sync::mpsc::channel(2);
            let custom = CustomScoreboardRuntime::new(tx, uuid::Uuid::nil());
            let mut current = None;
            apply_custom_delta(
                &mut current,
                CustomScoreDelta {
                    entity: "Alice".into(),
                    objective: "kills".into(),
                    delta: 7,
                    bedrock,
                },
                &NoTarget,
            );
            custom.snapshot.store(Arc::new(current.clone()));
            assert_eq!(
                custom
                    .add_score("Alice".into(), "kills".into(), 1, bedrock)
                    .unwrap(),
                8
            );
            assert_eq!(
                custom
                    .add_score("Alice".into(), "kills".into(), 1, bedrock)
                    .unwrap(),
                8
            );
            assert!(
                custom
                    .add_score("Alice".into(), "kills".into(), 1, bedrock)
                    .is_err()
            );
            for _ in 0..2 {
                let update = rx.try_recv().unwrap();
                apply_custom_delta(&mut current, update.delta.unwrap(), &NoTarget);
            }
            let value = match current.as_ref().unwrap() {
                CustomScoreboard::Java(board) => board.get_score_value("Alice", "kills").unwrap(),
                CustomScoreboard::Bedrock(board) => board.scores[&("Alice".into(), "kills".into())],
            };
            assert_eq!(value, 9);
            let old_value = match custom.snapshot().as_ref().as_ref().unwrap() {
                CustomScoreboard::Java(board) => board.get_score_value("Alice", "kills").unwrap(),
                CustomScoreboard::Bedrock(board) => board.scores[&("Alice".into(), "kills".into())],
            };
            assert_eq!(old_value, 7);
            custom.snapshot.store(Arc::new(current));
            assert_eq!(
                custom
                    .add_score("Alice".into(), "kills".into(), 1, bedrock)
                    .unwrap(),
                10
            );
        }
    }

    #[test]
    fn world_visual_routing_changes_only_with_accepted_custom_publication() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let custom = CustomScoreboardRuntime::new(tx, uuid::Uuid::nil());
        custom
            .set(Some(CustomScoreboard::Java(Scoreboard::new())))
            .unwrap();
        assert!(world_scoreboard_visible(custom.snapshot().as_ref(), false));
        let update = rx.try_recv().unwrap();
        let mut current = None;
        reconcile_custom_scoreboard(&mut current, update.before, update.after, &NoTarget);
        custom.snapshot.store(Arc::new(current));
        assert!(!world_scoreboard_visible(custom.snapshot().as_ref(), false));
        assert!(world_scoreboard_visible(custom.snapshot().as_ref(), true));
        custom.set(None).unwrap();
        assert!(!world_scoreboard_visible(custom.snapshot().as_ref(), false));
        let update = rx.try_recv().unwrap();
        let mut current = custom.snapshot().as_ref().clone();
        reconcile_custom_scoreboard(&mut current, update.before, update.after, &NoTarget);
        custom.snapshot.store(Arc::new(current));
        assert!(world_scoreboard_visible(custom.snapshot().as_ref(), false));
        assert!(world_scoreboard_visible(custom.snapshot().as_ref(), true));
        custom
            .set(Some(CustomScoreboard::Bedrock(BedrockScoreboard::default())))
            .unwrap();
        let update = rx.try_recv().unwrap();
        let mut current = custom.snapshot().as_ref().clone();
        reconcile_custom_scoreboard(&mut current, update.before, update.after, &NoTarget);
        custom.snapshot.store(Arc::new(current));
        assert!(world_scoreboard_visible(custom.snapshot().as_ref(), false));
        assert!(!world_scoreboard_visible(custom.snapshot().as_ref(), true));
    }

    #[test]
    fn reserved_custom_edit_suppresses_packets_until_owner_accepts_descriptor() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let custom = CustomScoreboardRuntime::new(tx, uuid::Uuid::nil());
        let mut edit = custom.edit().unwrap();
        assert!(custom.edit().is_err());
        *edit = Some(CustomScoreboard::Java(Scoreboard::new()));
        let target = CountPackets(std::sync::atomic::AtomicUsize::new(0));
        let Some(CustomScoreboard::Java(board)) = edit.as_mut() else {
            panic!();
        };
        board.add_objective(
            &target,
            ScoreboardObjective::new(
                "kills",
                TextComponent::text("Kills"),
                RenderType::Integer,
                None,
                "dummy",
            ),
        );
        board.set_score_value(&target, "Alice", "kills", 7);
        assert_eq!(target.0.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(custom.snapshot().as_ref().is_none());
        edit.commit().unwrap();
        let update = rx.try_recv().unwrap();
        let Some(CustomScoreboard::Java(board)) = update.after else {
            panic!();
        };
        assert_eq!(board.get_score_value("Alice", "kills"), Some(7));
        assert!(!board.deferred);
        assert!(custom.edit().is_ok());
    }

    #[test]
    fn concurrent_initial_custom_edits_merge_without_waiting_for_publication() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let custom = CustomScoreboardRuntime::new(tx, uuid::Uuid::nil());
        for name in ["kills", "points"] {
            let mut edit = custom.edit().unwrap();
            *edit = Some(CustomScoreboard::Java(Scoreboard::new()));
            let Some(CustomScoreboard::Java(board)) = edit.as_mut() else {
                panic!();
            };
            board.add_objective(
                &NoTarget,
                ScoreboardObjective::new(
                    name,
                    TextComponent::text(name.to_owned()),
                    RenderType::Integer,
                    None,
                    "dummy",
                ),
            );
            edit.commit().unwrap();
        }
        let mut current = None;
        while let Ok(update) = rx.try_recv() {
            reconcile_custom_scoreboard(&mut current, update.before, update.after, &NoTarget);
        }
        let Some(CustomScoreboard::Java(board)) = current else {
            panic!();
        };
        assert!(board.get_objective("kills").is_some());
        assert!(board.get_objective("points").is_some());
    }

    #[test]
    fn invalid_custom_edit_releases_admission_without_emitting_a_descriptor() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let custom = CustomScoreboardRuntime::new(tx, uuid::Uuid::nil());
        let mut edit = custom.edit().unwrap();
        *edit = Some(CustomScoreboard::Java(Scoreboard::new()));
        let Some(CustomScoreboard::Java(board)) = edit.as_mut() else {
            panic!();
        };
        board.add_objective(
            &NoTarget,
            ScoreboardObjective::new(
                "kills",
                TextComponent::text("x".repeat(65537)),
                RenderType::Integer,
                None,
                "dummy",
            ),
        );
        assert!(edit.commit().is_err());
        assert!(rx.try_recv().is_err());
        assert!(custom.snapshot().as_ref().is_none());
        assert!(custom.edit().is_ok());
    }
}

impl ScoreboardTarget for crate::net::ClientPlatform {
    fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
        &self,
        java: &J,
        bedrock: &B,
    ) {
        self.try_enqueue_packet_editioned(java, bedrock);
    }
    fn send_je<J: ClientPacket + Sync>(&self, packet: &J) {
        if let Self::Java(client) = self {
            client.try_send_packet(packet);
        }
    }
}

fn score_identity(entity: &str, objective: &str) -> i64 {
    xxhash_rust::xxh64::xxh64(&serde_json::to_vec(&(objective, entity)).unwrap(), 0) as i64
}

fn bedrock_score_entry(entity: &str, objective: &str, value: i32) -> BScoreEntry {
    BScoreEntry {
        scoreboard_id: score_identity(entity, objective),
        objective_name: objective.to_owned(),
        score: VarInt(value),
        entry_type: VarInt(3),
        entity_unique_id: 0,
        custom_name: entity.to_owned(),
    }
}

#[cfg(test)]
mod bedrock_scoreboard_packet_tests {
    use super::*;
    use pumpkin_protocol::codec::{var_long::VarLong, var_uint::VarUInt};
    use pumpkin_protocol::serial::PacketRead;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    struct Capture {
        change: AtomicI64,
        remove: AtomicI64,
        packets: AtomicUsize,
    }
    impl ScoreboardTarget for Capture {
        fn send_editioned<J: ClientPacket + Sync, B: BClientPacket + Sync>(
            &self,
            _: &J,
            packet: &B,
        ) {
            if B::PACKET_ID != 108 {
                return;
            }
            let mut bytes = Vec::new();
            packet.write_packet(&mut bytes).unwrap();
            let mut reader = std::io::Cursor::new(bytes);
            assert_eq!(VarUInt::read(&mut reader).unwrap().0, 1);
            let kind = VarUInt::read(&mut reader).unwrap().0;
            let _ = String::read(&mut reader).unwrap();
            let id = VarLong::read(&mut reader).unwrap().0;
            if kind == 0 {
                self.remove.store(id, Ordering::Relaxed);
            } else {
                self.change.store(id, Ordering::Relaxed);
            }
            self.packets.fetch_add(1, Ordering::Relaxed);
        }
        fn send_je<J: ClientPacket + Sync>(&self, _: &J) {}
    }

    #[test]
    fn bedrock_changes_and_removals_keep_score_identity_across_owned_snapshots() {
        let target = Capture {
            change: AtomicI64::new(0),
            remove: AtomicI64::new(0),
            packets: AtomicUsize::new(0),
        };
        let mut board = BedrockScoreboard::default();
        board.update_score(&target, "Alice", "kills", 7);
        let original_id = target.change.load(Ordering::Relaxed);
        let mut next = board.clone();
        next.update_score(&target, "Alice", "kills", 8);
        assert_eq!(target.change.load(Ordering::Relaxed), original_id);
        next.remove_score(&target, "Alice", "kills");
        assert_eq!(target.remove.load(Ordering::Relaxed), original_id);
        assert_eq!(target.packets.load(Ordering::Relaxed), 3);
    }
}
