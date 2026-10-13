mod option;
pub mod parser;

use crate::command::argument_types::entity;
use crate::command::argument_types::entity::ENTITY_SELECTOR_PERMISSION;
use crate::command::argument_types::entity_selector::parser::SELECTORS_NOT_ALLOWED_ERROR_TYPE;
use crate::command::context::command_source::CommandSource;
use crate::command::errors::command_syntax_error::CommandSyntaxError;
use crate::entity::EntityBase;
use crate::entity::player::Player;
use crate::world::World;
use pumpkin_data::Advancement;
use pumpkin_data::entity::EntityType;
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_nbt::tag::NbtTag;
use pumpkin_util::GameMode;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::bounds::{DoubleBounds, FloatDegreeBounds, IntBounds};
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::math::wrap_degrees;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// Represents a structure that can target entities.
///
/// Not all selectors represented by this object start with `@`; this is also used for
/// selectors like *plain player names* and *bare UUIDs*.
pub struct EntitySelector {
    /// The maximum number of possible entities that can be selected.
    pub max_selected: i32,
    /// Whether this selector includes all entities rather than just players.
    pub includes_entities: bool,
    /// A list of predicates that must be satisfied by an entity to be part
    /// of this selector. Used for things like checking a game mode.
    pub predicates: Vec<EntitySelectorPredicate>,
    /// The distance range that this selector allows.
    pub distance: Option<DoubleBounds>,
    /// A function corresponding to how positions are provided for this selector, provided
    /// an initial [`Vector3<f64>`].
    pub position_function: PositionFunction,
    /// The limiting bounding box for this selector.
    pub bounding_box: Option<BoundingBox>,
    /// The sorting order of this selector.
    pub order: Order,
    /// Whether the selector represents the currently executing entity. Only true if `@s` is used.
    pub is_current_entity: bool,
    /// The limiting player name of this selector.
    pub player_name: Option<String>,
    /// The limiting UUID of this selector.
    pub entity_uuid: Option<Uuid>,
    /// The limiting entity type or tag for this selector.
    pub entity_type: Option<&'static EntityType>,
    /// Whether this selector uses a selector variable (like `@p`).
    pub uses_selector_variable: bool,
    /// Whether this selector limits entities to a certain world.
    pub is_world_limited: bool,
}

impl EntitySelector {
    /// Returns an [`Err`] if a [`CommandSource`] does not have permission to use
    /// this entity selector.
    pub fn check_permissions(&self, source: &CommandSource) -> Result<(), CommandSyntaxError> {
        if self.uses_selector_variable && !source.has_permission(ENTITY_SELECTOR_PERMISSION) {
            Err(SELECTORS_NOT_ALLOWED_ERROR_TYPE.create_without_context())
        } else {
            Ok(())
        }
    }

    const fn result_limit(&self) -> usize {
        if matches!(self.order, Order::Arbitrary) {
            self.max_selected as usize
        } else {
            usize::MAX
        }
    }

    /// Tries to find a single entity represented by this selector.
    pub fn find_single_entity(
        &self,
        source: &CommandSource,
    ) -> Result<Arc<dyn EntityBase>, CommandSyntaxError> {
        let list = self.find_entities(source)?;
        match list.as_slice() {
            [] => Err(entity::NO_ENTITIES_ERROR_TYPE.create_without_context()),
            [entity] => Ok(entity.clone()),
            _ => Err(entity::NOT_SINGLE_ENTITY_ERROR_TYPE.create_without_context()),
        }
    }

    /// Tries to find any entities represented by this selector. If none are found, an empty `Vec` will still be returned.
    pub fn find_entities(
        &self,
        source: &CommandSource,
    ) -> Result<Vec<Arc<dyn EntityBase>>, CommandSyntaxError> {
        self.check_permissions(source)?;
        if !self.includes_entities && !source.server().advanced_config.cluster.enabled {
            self.find_players(source)
                .map(|v| v.into_iter().map(|p| p as Arc<dyn EntityBase>).collect())
        } else if let Some(name) = self.player_name.as_ref() {
            if source.server().advanced_config.cluster.enabled {
                let server = source.server();
                let presence = server.cluster_presence.snapshot();
                let Some(gid) = presence.by_name(name) else { return Ok(Vec::new()); };
                let Some(profile) = presence.entries.get(&gid).filter(|profile| !profile.in_lobby) else { return Ok(Vec::new()); };
                if let Some(replica) = server.cluster_remote_players.load().get(&gid) {
                    return Ok(vec![replica.clone() as Arc<dyn EntityBase>]);
                }
                return Ok(server.get_player_by_uuid(Uuid::from_bytes(profile.uuid))
                    .filter(|player| player.cluster_gid() == Some(gid))
                    .map_or_else(Vec::new, |player| vec![player as Arc<dyn EntityBase>]));
            }
            // Try to get the player by name.
            let player = source
                .server
                .as_ref()
                .and_then(|s| s.get_player_by_name(name));
            if let Some(player) = player {
                return Ok(vec![player as Arc<dyn EntityBase>]);
            }
            Ok(Vec::new())
        } else if let Some(uuid) = self.entity_uuid.as_ref() {
            // Try to get an entity by UUID.
            for world in source.server().worlds.load().iter() {
                if let Some(entity) = world.get_entity_by_uuid(*uuid) {
                    return Ok(vec![entity]);
                }
            }
            Ok(Vec::new())
        } else {
            let origin = self.position_function.apply(source.position);
            let bounding_box = self.absolute_bounding_box(origin);
            let predicate = self.predicate(origin, bounding_box);
            if self.is_current_entity {
                Ok(source
                    .entity
                    .as_ref()
                    .filter(|p| predicate.test(p.as_ref()))
                    .map_or_else(Vec::new, |p| vec![p.clone()]))
            } else {
                let mut list = Vec::new();
                if self.is_world_limited {
                    self.add_entities(&mut list, source.world().as_ref(), bounding_box, &predicate);
                } else {
                    for world in source.server().worlds.load().iter() {
                        self.add_entities(&mut list, world, bounding_box, &predicate);
                    }
                }

                Ok(self.sort_and_limit(origin, list))
            }
        }
    }

    fn add_entities(
        &self,
        list: &mut Vec<Arc<dyn EntityBase>>,
        world: &World,
        bounding_box: Option<BoundingBox>,
        predicate: &EntitySelectorPredicate,
    ) {
        let limit = self.result_limit();
        if let Some(b) = bounding_box {
            world.extend_entities_in_box_where(list, limit, b, |e| predicate.test(e));
        } else {
            world.extend_entities_where(list, limit, |e| predicate.test(e));
        }
    }

    /// Tries to find a single player represented by this selector.
    pub fn find_single_player(
        &self,
        source: &CommandSource,
    ) -> Result<Arc<Player>, CommandSyntaxError> {
        let mut list = self.find_players(source)?;
        match list.len() {
            0 => Err(entity::NO_PLAYERS_ERROR_TYPE.create_without_context()),
            1 => Ok(list.pop().unwrap()),
            _ => Err(entity::NOT_SINGLE_PLAYER_ERROR_TYPE.create_without_context()),
        }
    }

    /// Tries to find any players represented by this selector.
    pub fn find_players(
        &self,
        source: &CommandSource,
    ) -> Result<Vec<Arc<Player>>, CommandSyntaxError> {
        self.check_permissions(source)?;
        if let Some(name) = self.player_name.as_ref() {
            // Try to get the player by name.
            let player = source
                .server
                .as_ref()
                .and_then(|s| s.get_player_by_name(name))
                .filter(|p| !p.is_in_cluster_lobby());
            Ok(player.map_or_else(Vec::new, |p| vec![p]))
        } else if let Some(uuid) = self.entity_uuid.as_ref() {
            // Try to get an entity by UUID.
            for world in source.server().worlds.load().iter() {
                if let Some(player) = world.get_player_by_uuid(*uuid)
                    && !player.is_in_cluster_lobby()
                {
                    return Ok(vec![player]);
                }
            }
            Ok(Vec::new())
        } else {
            let origin = self.position_function.apply(source.position);
            let bounding_box = self.absolute_bounding_box(origin);
            let predicate = self.predicate(origin, bounding_box);
            if self.is_current_entity {
                Ok(source
                    .entity
                    .as_ref()
                    .and_then(|e| {
                        source
                            .server()
                            .get_player_by_uuid(e.get_entity().entity_uuid)
                    })
                    .filter(|p| predicate.test(p.as_ref()))
                    .map_or_else(Vec::new, |p| vec![p]))
            } else {
                let limit = self.result_limit();
                let mut list = Vec::new();
                if limit > 0 {
                    if self.is_world_limited {
                        Self::add_players_from_world(source.world(), &mut list, &predicate, limit);
                    } else {
                        for world in source.server().worlds.load().iter() {
                            Self::add_players_from_world(
                                world.as_ref(),
                                &mut list,
                                &predicate,
                                limit,
                            );
                        }
                    }
                }

                Ok(self.sort_and_limit(origin, list))
            }
        }
    }

    fn add_players_from_world(
        world: &World,
        list: &mut Vec<Arc<Player>>,
        predicate: &EntitySelectorPredicate,
        limit: usize,
    ) {
        for player in world.players.load().iter() {
            if player.is_in_cluster_lobby() {
                continue;
            }
            if predicate.test(player.as_ref()) {
                list.push(player.clone());
                if list.len() >= limit {
                    return;
                }
            }
        }
    }

    #[must_use]
    pub fn absolute_bounding_box(&self, pos: Vector3<f64>) -> Option<BoundingBox> {
        self.bounding_box.map(|b| b.shift(pos))
    }

    /// Returns a [`EntitySelectorPredicate`] to test against an entity.
    #[must_use]
    fn predicate(
        &self,
        pos: Vector3<f64>,
        bounding_box: Option<BoundingBox>,
    ) -> EntitySelectorPredicate {
        let mut list = self.predicates.clone();
        if !self.includes_entities {
            list.push(EntitySelectorPredicate::EntityType(
                &EntityType::PLAYER,
                false,
            ));
        }

        if let Some(bounding_box) = bounding_box {
            list.push(EntitySelectorPredicate::BoundingBox(bounding_box));
        }
        if let Some(distance_bounds) = self.distance {
            list.push(EntitySelectorPredicate::Distance(distance_bounds, pos));
        }

        EntitySelectorPredicate::new_all_of(list)
    }

    /// Sorts the provided entities according to this selector's order ([`Order::Arbitrary`] by default)
    /// and limits the number of entries depending on this selector's limit.
    fn sort_and_limit<T: EntityBase + ?Sized>(
        &self,
        origin: Vector3<f64>,
        entities: Vec<Arc<T>>,
    ) -> Vec<Arc<T>> {
        self.order.sort_and_limit(
            entities.len().min(self.max_selected as usize),
            origin,
            entities,
        )
    }
}

/// A function that may or may not manipulate a provided position to be used by a parser.
pub enum PositionFunction {
    /// A function that does not affect a position and simply returns it.
    Identity,
    /// A function that may override one or more coordinates of a position depending
    /// on the position of the parse.
    ///
    /// If a position coordinate of the parser is set, the provided position's
    /// corresponding coordinate is replaced, and the new position is returned.
    OverrideWithParser(Vector3<Option<f64>>),
}

impl PositionFunction {
    fn apply(&self, pos: Vector3<f64>) -> Vector3<f64> {
        match self {
            Self::Identity => pos,
            Self::OverrideWithParser(function_pos) => Vector3::new(
                function_pos.x.unwrap_or(pos.x),
                function_pos.y.unwrap_or(pos.y),
                function_pos.z.unwrap_or(pos.z),
            ),
        }
    }
}

/// An order to choose entities that could be selected by an entity selector.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Order {
    Nearest,
    Furthest,
    Random,
    Arbitrary,
}

impl Order {
    fn sort_with_comparator_usage_function<T: EntityBase + ?Sized>(
        mut entities: Vec<Arc<T>>,
        f: impl Fn(&Arc<T>, &Arc<T>) -> std::cmp::Ordering,
    ) -> Vec<Arc<T>> {
        entities.sort_by(f);
        entities
    }

    fn distance_comparator<T: EntityBase + ?Sized>(
        origin: &Vector3<f64>,
        a: &Arc<T>,
        b: &Arc<T>,
    ) -> std::cmp::Ordering {
        a.get_entity()
            .pos
            .load()
            .squared_distance_to_vec(origin)
            .total_cmp(&b.get_entity().pos.load().squared_distance_to_vec(origin))
    }

    #[must_use]
    pub fn sort_and_limit<T: EntityBase + ?Sized>(
        &self,
        limit: usize,
        origin: Vector3<f64>,
        mut entities: Vec<Arc<T>>,
    ) -> Vec<Arc<T>> {
        match self {
            Self::Nearest => Self::sort_with_comparator_usage_function(entities, |a, b| {
                Self::distance_comparator(&origin, a, b)
            }),
            Self::Furthest => Self::sort_with_comparator_usage_function(entities, |a, b| {
                Self::distance_comparator(&origin, b, a)
            }),
            Self::Random => {
                let mut rng = rand::rng();
                entities.shuffle(&mut rng);
                entities
            }
            Self::Arbitrary => entities,
        }
        .into_iter()
        .take(limit)
        .collect()
    }
}

/// A predicate for an entity selector.
#[derive(Debug, Clone)]
pub enum EntitySelectorPredicate {
    /// A predicate to check whether an entity is alive.
    IsAlive,
    /// A predicate to check the game mode of a player. This check can also be inverted.
    GameMode(GameMode, bool),
    /// A predicate to check the experience level of an entity, if any.
    ExperienceLevel(IntBounds),
    /// A predicate to check the rotation coordinate of an entity.
    Rotation(FloatDegreeBounds, RotationType),
    /// A predicate to check whether an entity intersects a bounding box.
    BoundingBox(BoundingBox),
    /// A predicate to check whether an entity is within a specified range from some position.
    Distance(DoubleBounds, Vector3<f64>),
    /// A predicate to check the entity type. This check can also be inverted.
    EntityType(&'static EntityType, bool),
    /// A predicate to check the entity's name (custom or profile name).
    Name(String, bool),
    /// A predicate to check the entity's scoreboard tags.
    Tag(String, bool),
    /// A predicate to check the entity's team.
    Team(String, bool),
    /// A predicate to check the entity's scores.
    Scores(HashMap<String, IntBounds>),
    /// A predicate to check the player's advancements.
    Advancements(HashMap<String, bool>),
    /// A predicate to check the entity's raw NBT data.
    Nbt(NbtCompound, bool),
    /// A predicate to check loot table conditions / predicates.
    Predicate(String, bool),

    /// Used to combine sub-predicates.
    AllOf(Vec<Self>),
}

#[derive(Debug, Clone, Copy)]
pub enum RotationType {
    Yaw,
    Pitch,
}

impl RotationType {
    /// Returns the rotation value corresponding to this rotation type
    /// of the provided entity.
    pub fn value_from_entity(self, entity: &dyn EntityBase) -> f32 {
        match self {
            Self::Yaw => entity.get_entity().yaw.load(),
            Self::Pitch => entity.get_entity().pitch.load(),
        }
    }
}

fn matches_nbt(expected: &NbtTag, actual: &NbtTag) -> bool {
    match (expected, actual) {
        (NbtTag::Compound(expected_comp), NbtTag::Compound(actual_comp)) => {
            for (key, expected_val) in &expected_comp.child_tags {
                if let Some(actual_val) = actual_comp.child_tags.get(key) {
                    if !matches_nbt(expected_val, actual_val) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        }
        (NbtTag::List(expected_list), NbtTag::List(actual_list)) => {
            for expected_val in expected_list {
                if !actual_list
                    .iter()
                    .any(|actual_val| matches_nbt(expected_val, actual_val))
                {
                    return false;
                }
            }
            true
        }
        (a, b) => a == b,
    }
}

fn matches_nbt_compound(expected: &NbtCompound, actual: &NbtCompound) -> bool {
    for (key, expected_val) in &expected.child_tags {
        if let Some(actual_val) = actual.child_tags.get(key) {
            if !matches_nbt(expected_val, actual_val) {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

#[allow(clippy::option_if_let_else)]
fn entity_actual_name(entity: &dyn EntityBase) -> String {
    if let Some(replica) = entity
        .cast_any()
        .downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>()
    {
        replica.frame.profile.name.clone()
    } else if let Some(player) = entity.get_player() {
        player.gameprofile.name.clone()
    } else if let Some(custom_name) = &**entity.get_entity().custom_name.load() {
        custom_name.clone().get_text()
    } else {
        entity.get_entity().entity_type.resource_name.to_string()
    }
}

impl EntitySelectorPredicate {
    #[must_use]
    pub const fn new_all_of(predicates: Vec<Self>) -> Self {
        Self::AllOf(predicates)
    }

    #[allow(clippy::too_many_lines)]
    pub fn test(&self, entity: &dyn EntityBase) -> bool {
        match self {
            Self::IsAlive => {
                if let Some(shell) = entity
                    .cast_any()
                    .downcast_ref::<crate::server::cluster_entity_registry::CanonicalEntityShell>()
                {
                    return match &shell.body.payload {
                        pumpkin_cluster::entity_body::EntityPayload::Living(living) => {
                            living.combat.health_milli != 0
                        }
                        _ => true,
                    };
                }
                entity
                    .cast_any()
                    .downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>()
                    .map_or_else(
                        || entity.get_entity().is_alive(),
                        |replica| replica.frame.living.combat.health_milli != 0,
                    )
            }
            Self::GameMode(mode, invert) => {
                entity
                    .get_player()
                    .map(|p| p.gamemode.load())
                    .or_else(|| {
                        entity.cast_any()
                    .downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>()
                    .and_then(|replica| GameMode::try_from(replica.frame.spatial.game_mode).ok())
                    })
                    .is_some_and(|actual| (actual == *mode) ^ invert)
            }
            Self::ExperienceLevel(bounds) => entity
                .get_player()
                .map(|p| p.experience_level.load(Ordering::Relaxed))
                .or_else(|| {
                    entity
                        .cast_any()
                        .downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>()
                        .and_then(|replica| {
                            replica
                                .frame
                                .living
                                .avatar
                                .as_ref()
                                .and_then(|avatar| i32::try_from(avatar.experience.level).ok())
                        })
                })
                .is_some_and(|level| bounds.matches(level)),
            Self::Rotation(bounds, f) => {
                let min = wrap_degrees(bounds.min().unwrap_or(0.0f32));
                let max = wrap_degrees(bounds.max().unwrap_or(360.0f32));
                let degrees = wrap_degrees(f.value_from_entity(entity));
                if min > max {
                    degrees >= min || degrees <= max
                } else {
                    degrees >= min && degrees <= max
                }
            }
            Self::BoundingBox(bounding_box) => entity
                .get_entity()
                .bounding_box
                .load()
                .intersects(bounding_box),
            Self::Distance(bounds, pos) => {
                bounds.matches_square(entity.get_entity().pos.load().squared_distance_to_vec(pos))
            }
            Self::EntityType(expected_type, invert) => {
                let actual_type = entity.get_entity().entity_type;
                (actual_type.id == expected_type.id) ^ invert
            }
            Self::Name(expected_name, invert) => {
                let actual_name = entity_actual_name(entity);
                (actual_name == *expected_name) ^ invert
            }
            Self::Tag(expected_tag, invert) => {
                if let Some(shell) = entity
                    .cast_any()
                    .downcast_ref::<crate::server::cluster_entity_registry::CanonicalEntityShell>()
                {
                    return shell.body.tags.contains(expected_tag) ^ invert;
                }
                if let Some(replica) = entity
                    .cast_any()
                    .downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>(
                ) {
                    return replica.frame.living.tags.contains(expected_tag) ^ invert;
                }
                if let Some(server) = entity.get_entity().world.load().server.upgrade()
                    && server.advanced_config.cluster.enabled
                {
                    let has_tag = server
                        .cluster_combat_client_targets
                        .load()
                        .get(&entity.get_entity().entity_id)
                        .filter(|target| {
                            target.contact.valid()
                                && server
                                    .cluster_peer_incarnations
                                    .load()
                                    .get(&target.contact.owner.server)
                                    == Some(&target.contact.owner.generation)
                        })
                        .is_some_and(|target| target.tags.contains(expected_tag));
                    return has_tag ^ invert;
                }
                let has_tag = entity
                    .get_entity()
                    .scoreboard_tags
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains(expected_tag);
                has_tag ^ invert
            }
            Self::Team(expected_team, invert) => {
                let actual_name = entity_actual_name(entity);
                let world = entity.get_entity().world.load();
                let teams = world.team_chat_snapshot.load();
                let has_team = teams.iter().any(|(name, team)| {
                    name == expected_team && team.players.contains(&actual_name)
                });
                has_team ^ invert
            }
            Self::Scores(scores_map) => {
                let actual_name = entity_actual_name(entity);
                let world = entity.get_entity().world.load();
                if world
                    .server
                    .upgrade()
                    .is_some_and(|server| server.advanced_config.cluster.enabled)
                {
                    let scores = world.score_query_snapshot.load();
                    return scores_map.iter().all(|(objective, bounds)| {
                        bounds.matches(
                            scores
                                .get(objective)
                                .and_then(|entities| entities.get(&actual_name))
                                .map_or(0, |score| score.value.0),
                        )
                    });
                }
                let scoreboard = world
                    .scoreboard.snapshot();
                for (objective, bounds) in scores_map {
                    let score_val = scoreboard.get_score_value(&actual_name, objective).unwrap_or(0);
                    if !bounds.matches(score_val) {
                        return false;
                    }
                }
                true
            }
            Self::Advancements(advancements_map) => {
                if let Some(replica) = entity
                    .cast_any()
                    .downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>(
                ) {
                    return advancements_map.iter().all(|(id, expected)| {
                        let Some(advancement) = Advancement::from_minecraft_name(id).or_else(|| Advancement::from_name(id)) else { return false; };
                        replica
                                .frame
                                .living
                                .advancements
                                .progress
                                .get(&advancement.id.to_string())
                                .is_some_and(|progress| progress.completed)
                                == *expected
                    });
                }
                if let Some(server) = entity.get_entity().world.load().server.upgrade()
                    && server.advanced_config.cluster.enabled
                {
                    let Some(gid) = entity.get_player().and_then(|player| player.cluster_gid())
                    else {
                        return false;
                    };
                    let snapshot = server.cluster_actor_living.load();
                    let Some(state) = snapshot.get(&pumpkin_cluster::actor::ActorId::Player(gid))
                    else {
                        return false;
                    };
                    return advancements_map.iter().all(|(id, expected)| {
                        let Some(advancement) = Advancement::from_minecraft_name(id).or_else(|| Advancement::from_name(id)) else { return false; };
                        state
                                .advancements
                                .progress
                                .get(&advancement.id.to_string())
                                .is_some_and(|progress| progress.completed)
                                == *expected
                    });
                }
                let Some(player) = entity.get_player() else {
                    return false;
                };
                for (adv_id, expected_done) in advancements_map {
                    if let Some(advancement) = Advancement::from_minecraft_name(adv_id).or_else(|| Advancement::from_name(adv_id)) {
                        let is_done = player.has_advancement(advancement);
                        if is_done != *expected_done {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            Self::Nbt(expected_nbt, invert) => {
                if expected_nbt.child_tags.contains_key("EnderItems")
                    && let Some(replica) = entity.cast_any().downcast_ref::<crate::entity::cluster_player_ghost::RemotePlayerReplica>()
                {
                    let Some(server) = entity.get_entity().world.load().server.upgrade() else { return false; };
                    if replica.frame.canonical_ender_cells(&server).is_none() { return false; }
                }
                let mut actual_nbt = NbtCompound::default();
                entity.write_nbt(&mut actual_nbt);
                matches_nbt_compound(expected_nbt, &actual_nbt) ^ invert
            }
            Self::Predicate(_predicate_id, invert) => {
                // Since loot-table predicates are not yet fully implemented, we default to false (or true ^ invert)
                false ^ invert
            }
            Self::AllOf(predicates) => predicates.iter().all(|predicate| predicate.test(entity)),
        }
    }
}
