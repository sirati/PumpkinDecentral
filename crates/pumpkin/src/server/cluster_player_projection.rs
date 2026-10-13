use std::collections::{HashMap, HashSet, VecDeque};

use pumpkin_cluster::action_graph::LocalDeltaStep;
use pumpkin_cluster::canonical_action::{ChunkPart, SpatialEpoch, Target, Value};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_util::math::vector3::Vector3;

use crate::entity::EntityBase;
use crate::net::ClientPlatform;

#[derive(Default)]
pub(super) struct OwnedPlayerClientProjection {
    teleport_ids: HashMap<GlobalPlayerId, i32>,
    epochs: HashMap<(GlobalPlayerId, u64), SpatialEpoch>,
    pending: VecDeque<(GlobalPlayerId, ActorLivingState)>,
    overflowed: HashSet<GlobalPlayerId>,
    velocities: HashMap<(GlobalPlayerId, u64), [i64; 3]>,
    delivered: HashMap<(GlobalPlayerId, u64), ActorLivingState>,
    experience:
        HashMap<(GlobalPlayerId, u64), pumpkin_cluster::player_experience::PlayerExperienceState>,
}

impl OwnedPlayerClientProjection {
    pub(super) fn publish(
        &mut self,
        server: &super::Server,
        steps: &[LocalDeltaStep],
        camera_id: impl Fn(&pumpkin_cluster::item_contact::ContactObservation) -> Option<i32>,
        current_living: impl Fn(GlobalPlayerId) -> Option<ActorLivingState>,
    ) {
        let mut chronological = HashMap::new();
        for write in steps.iter().flat_map(|step| &step.writes) {
            let Some((_, gid)) = write.target.player_avatar() else {
                continue;
            };
            if let Some(state) = current_living(gid) {
                chronological.entry(gid).or_insert(state);
            }
        }
        for write in steps.iter().rev().flat_map(|step| step.writes.iter().rev()) {
            apply_player_image(&mut chronological, write.target, write.before.as_ref());
        }
        for (gid, state) in &chronological {
            if let Some(avatar) = &state.avatar {
                let Some(incarnation) = target_incarnation(server, *gid, avatar.spatial_epoch)
                else {
                    continue;
                };
                self.epochs
                    .entry((*gid, incarnation))
                    .or_insert(avatar.spatial_epoch);
            }
        }
        for step in steps {
            let mut changed = std::collections::BTreeSet::new();
            for write in &step.writes {
                if let Some((_, gid)) = write.target.player_avatar() {
                    apply_player_image(&mut chronological, write.target, write.after.as_ref());
                    changed.insert(gid);
                }
            }
            for gid in changed {
                if let Some(state) = chronological.get(&gid) {
                    if self.pending.len() >= 16384 {
                        if self.overflowed.insert(gid) {
                            if let Some(player) = server
                                .get_all_players()
                                .iter()
                                .find(|player| player.cluster_gid() == Some(gid))
                            {
                                player.client.try_kick(
                                    crate::net::DisconnectReason::Kicked,
                                    &pumpkin_util::text::TextComponent::text(
                                        "Client state delivery capacity exceeded",
                                    ),
                                );
                            }
                        }
                        self.pending.retain(|(actor, _)| *actor != gid);
                    } else if !self.overflowed.contains(&gid) {
                        self.pending.push_back((gid, state.clone()));
                    }
                }
            }
        }
        let players = server.get_all_players();
        self.overflowed.retain(|gid| {
            players
                .iter()
                .any(|player| player.cluster_gid() == Some(*gid))
        });
        self.velocities.retain(|(gid, incarnation), _| {
            players.iter().any(|player| {
                player.cluster_gid() == Some(*gid)
                    && super::cluster_world_owner::player_action_lease(server, player)
                        .is_some_and(|lease| lease.incarnation == *incarnation)
            })
        });
        self.experience.retain(|(gid, incarnation), _| {
            players.iter().any(|player| {
                player.cluster_gid() == Some(*gid)
                    && super::cluster_world_owner::player_action_lease(server, player)
                        .is_some_and(|lease| lease.incarnation == *incarnation)
            })
        });
        self.epochs.retain(|(gid, incarnation), _| {
            players.iter().any(|player| {
                player.cluster_gid() == Some(*gid)
                    && super::cluster_world_owner::player_action_lease(server, player)
                        .is_some_and(|lease| lease.incarnation == *incarnation)
            })
        });
        self.delivered.retain(|(gid, incarnation), _| {
            players.iter().any(|player| {
                player.cluster_gid() == Some(*gid)
                    && super::cluster_world_owner::player_action_lease(server, player)
                        .is_some_and(|lease| lease.incarnation == *incarnation)
            })
        });
        self.teleport_ids.retain(|gid, _| {
            players
                .iter()
                .any(|player| player.cluster_gid() == Some(*gid))
        });
        let mut completed = Vec::new();
        let mut blocked = HashSet::new();
        for (index, (gid, state)) in self.pending.iter().enumerate() {
            if !blocked.insert(*gid) {
                continue;
            }
            let Some(player) = players
                .iter()
                .find(|player| player.cluster_gid() == Some(*gid))
            else {
                completed.push(index);
                blocked.remove(gid);
                continue;
            };
            if player
                .cluster_departing
                .load(std::sync::atomic::Ordering::Acquire)
            {
                completed.push(index);
                blocked.remove(gid);
                continue;
            }
            let Some(avatar) = &state.avatar else {
                completed.push(index);
                blocked.remove(gid);
                continue;
            };
            let Some(incarnation) = target_incarnation(server, *gid, avatar.spatial_epoch) else {
                completed.push(index);
                blocked.remove(gid);
                continue;
            };
            if super::cluster_world_owner::player_action_lease(server, player)
                .is_none_or(|lease| lease.incarnation != incarnation)
            {
                completed.push(index);
                blocked.remove(gid);
                continue;
            }
            let lease = super::cluster_world_owner::player_action_lease(server, player).unwrap();
            if !server.cluster_controller.active_for_simulation(
                lease,
                player
                    .cluster_owner_install_generation
                    .load(std::sync::atomic::Ordering::Acquire),
            ) {
                continue;
            }
            let camera = match avatar.presentation.camera_target.as_ref() {
                Some(contact) => camera_id(contact),
                None => Some(player.entity_id()),
            };
            if camera.is_none() {
                continue;
            }
            let mut first = state.clone();
            first.effects.clear();
            first.hidden_effects.clear();
            first.advancements = Default::default();
            let before = self.delivered.get(&(*gid, incarnation)).unwrap_or(&first);
            let bed_leave = if avatar.presentation.sleeping.is_none() {
                if let Some(previous) = before.avatar.as_ref()
                    && let Some(sleeping) = previous.presentation.sleeping
                {
                    let Some(world) = server
                        .worlds
                        .load()
                        .iter()
                        .find(|world| super::cluster_world_delta::world_id(world) == previous.world)
                        .cloned()
                    else {
                        continue;
                    };
                    let Some(owner) = world.server.upgrade() else {
                        continue;
                    };
                    let Some(permit) = super::cluster_player_lifecycle::reserve_bed_leave(server)
                    else {
                        continue;
                    };
                    let position = previous.position.map(f64::from_bits);
                    let source = crate::command::CommandSource::new(
                        crate::command::CommandSender::Player(player.clone()),
                        world,
                        Some(player.clone()),
                        Vector3::new(position[0], position[1], position[2]),
                        pumpkin_util::math::vector2::Vector2::new(
                            f32::from_bits(previous.yaw),
                            f32::from_bits(previous.pitch),
                        ),
                        player.gameprofile.name.clone(),
                        pumpkin_util::text::TextComponent::text(player.gameprofile.name.clone()),
                        owner,
                    );
                    Some((
                        permit,
                        source,
                        pumpkin_util::math::position::BlockPos::new(
                            sleeping.bed.x,
                            sleeping.bed.y,
                            sleeping.bed.z,
                        ),
                    ))
                } else {
                    None
                }
            } else {
                None
            };
            let reset_client = before.combat.health_milli == 0 && state.combat.health_milli > 0
                || before
                    .avatar
                    .as_ref()
                    .is_some_and(|previous| previous.world != avatar.world);
            let cross_world = before
                .avatar
                .as_ref()
                .is_some_and(|previous| previous.world != avatar.world);
            if reset_client && !publish_owned_respawn(server, player, state, cross_world) {
                continue;
            }
            if !super::cluster_living_metadata::publish_player_effect_delta(
                player.client.as_ref(),
                player.entity_id(),
                before,
                state,
                avatar.movement_tick,
            ) {
                continue;
            }
            if !super::cluster_living_metadata::publish_player_sleep_transition(
                player.client.as_ref(),
                player.entity_id(),
                Some(before),
                state,
            ) {
                continue;
            }
            if !super::cluster_advancements::publish_delta(
                server,
                player,
                &before.advancements,
                &state.advancements,
                !self.delivered.contains_key(&(*gid, incarnation)),
            ) {
                continue;
            }
            if !super::cluster_living_metadata::publish_player_attributes(
                player.client.as_ref(),
                player.entity_id(),
                Some(before),
                state,
                avatar.movement_tick,
                false,
            ) {
                continue;
            }
            if self.velocities.get(&(*gid, incarnation)) != Some(&state.combat.velocity_micro) {
                let velocity = state
                    .combat
                    .velocity_micro
                    .map(|value| value as f64 / 1_000_000.0);
                match player.client.as_ref() {
                    ClientPlatform::Java(client) => client.try_send_packet(
                        &pumpkin_protocol::java::client::play::CEntityVelocity::new(
                            player.entity_id().into(),
                            Vector3::new(velocity[0], velocity[1], velocity[2]),
                        ),
                    ),
                    ClientPlatform::Bedrock(client) => {
                        let packet = pumpkin_protocol::bedrock::client::CSetActorMotion {
                            target_runtime_id: (player.entity_id() as u64).into(),
                            motion: Vector3::new(
                                velocity[0] as f32,
                                velocity[1] as f32,
                                velocity[2] as f32,
                            ),
                            tick: u64::from(avatar.movement_tick.0).into(),
                        };
                        if let Ok(bytes) = client.serialize_packet(&packet) {
                            client.try_enqueue_packet(bytes);
                        }
                    }
                }
                self.velocities
                    .insert((*gid, incarnation), state.combat.velocity_micro);
            }
            if self
                .epochs
                .get(&(*gid, incarnation))
                .is_some_and(|epoch| *epoch != avatar.spatial_epoch)
            {
                let id = self.teleport_ids.entry(*gid).or_insert_with(|| {
                    player
                        .teleport_id_count
                        .load(std::sync::atomic::Ordering::Relaxed)
                });
                let next = player
                    .teleport_id_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    .wrapping_add(1);
                let position = avatar.position.map(f64::from_bits);
                let position = Vector3::new(position[0], position[1], position[2]);
                if let ClientPlatform::Java(client) = player.client.as_ref() {
                    if !server.cluster_controller.note_epoch_teleport(
                        *gid,
                        avatar.spatial_epoch,
                        next.into(),
                        position,
                    ) {
                        continue;
                    }
                    client.try_send_packet(
                        &pumpkin_protocol::java::client::play::CPlayerPosition::new(
                            next.into(),
                            position,
                            Vector3::new(0.0, 0.0, 0.0),
                            f32::from_bits(avatar.yaw),
                            f32::from_bits(avatar.pitch),
                            Vec::new(),
                        ),
                    );
                } else if let ClientPlatform::Bedrock(client) = player.client.as_ref() {
                    let packet = pumpkin_protocol::bedrock::client::CMovePlayer::new(
                        (player.entity_id() as u64).into(),
                        Vector3::new(
                            position.x as f32,
                            position.y as f32
                                + super::cluster_living_metadata::player_eye_height(state),
                            position.z as f32,
                        ),
                        f32::from_bits(avatar.pitch),
                        f32::from_bits(avatar.yaw),
                        f32::from_bits(avatar.head_yaw),
                        pumpkin_protocol::bedrock::client::CMovePlayer::MODE_TELEPORT,
                        avatar.on_ground,
                        0u64.into(),
                        0,
                        0,
                        u64::from(avatar.movement_tick.0).into(),
                    );
                    if let Ok(bytes) = client.serialize_packet(&packet) {
                        client.try_enqueue_packet(bytes);
                    }
                }
                *id = next;
                self.epochs
                    .insert((*gid, incarnation), avatar.spatial_epoch);
            }
            if self.experience.get(&(*gid, incarnation)) != Some(&avatar.experience)
                && publish_experience(
                    player.client.as_ref(),
                    player.entity_id(),
                    avatar.experience,
                    avatar.movement_tick,
                )
            {
                self.experience
                    .insert((*gid, incarnation), avatar.experience);
            }
            match player.client.as_ref() {
                ClientPlatform::Java(client) => {
                    if let Ok(metadata) = super::cluster_living_metadata::java_player_presentation(
                        state,
                        client.version.load(),
                    ) {
                        client.try_send_packet(
                            &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(
                                player.entity_id().into(),
                                metadata.into_boxed_slice(),
                            ),
                        );
                    }
                    client.try_send_packet(&pumpkin_protocol::java::client::play::CSetCamera::new(
                        camera.unwrap().into(),
                    ));
                    client.try_send_packet(&pumpkin_protocol::java::client::play::CSetHealth::new(
                        state.combat.health_milli as f32 / 1_000.0,
                        i32::from(state.food.food).into(),
                        state.food.saturation_milli as f32 / 1_000.0,
                    ));
                    if state.combat.health_milli == 0
                        && self
                            .delivered
                            .get(&(*gid, incarnation))
                            .is_none_or(|before| before.combat.health_milli != 0)
                    {
                        client.try_send_packet(
                            &pumpkin_protocol::java::client::play::CCombatDeath::new(
                                player.entity_id().into(),
                                &pumpkin_util::text::TextComponent::text("You died"),
                            ),
                        );
                    }
                    client.try_send_packet(&pumpkin_protocol::java::client::play::CGameEvent::new(
                        pumpkin_protocol::java::client::play::GameEvent::ChangeGameMode,
                        f32::from(avatar.game_mode),
                    ));
                }
                ClientPlatform::Bedrock(client) => {
                    let metadata = pumpkin_protocol::bedrock::client::CSetActorData {
                        target_runtime_id: (player.entity_id() as u64).into(),
                        actor_data: super::cluster_living_metadata::bedrock_player_presentation(
                            state,
                        ),
                        synced_properties: Default::default(),
                        tick: u64::from(avatar.movement_tick.0).into(),
                    };
                    if let Ok(bytes) = client.serialize_packet(&metadata) {
                        client.try_enqueue_packet(bytes);
                    }
                    let packet = pumpkin_protocol::bedrock::client::CSetHealth {
                        health: i32::try_from(state.combat.health_milli.div_ceil(1_000))
                            .unwrap_or(i32::MAX)
                            .into(),
                    };
                    if let Ok(bytes) = client.serialize_packet(&packet) {
                        client.try_enqueue_packet(bytes);
                    }
                }
            }
            if !publish_bedrock_hud(player, state) {
                continue;
            }
            if let Some((permit, source, bed)) = bed_leave {
                super::cluster_player_lifecycle::publish_bed_leave(
                    server, player, permit, source, bed,
                );
            }
            self.delivered.insert((*gid, incarnation), state.clone());
            self.epochs
                .entry((*gid, incarnation))
                .or_insert(avatar.spatial_epoch);
            completed.push(index);
            blocked.remove(gid);
        }
        let completed: HashSet<_> = completed.into_iter().collect();
        self.pending = self
            .pending
            .drain(..)
            .enumerate()
            .filter_map(|(index, event)| (!completed.contains(&index)).then_some(event))
            .collect();
    }
}

fn publish_owned_respawn(
    server: &super::Server,
    player: &crate::entity::player::Player,
    state: &ActorLivingState,
    cross_world: bool,
) -> bool {
    let Some(avatar) = state.avatar.as_ref() else {
        return false;
    };
    let worlds = server.worlds.load();
    let Some(world) = worlds
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == avatar.world)
    else {
        return false;
    };
    match player.client.as_ref() {
        ClientPlatform::Java(client) => {
            use pumpkin_protocol::java::client::play::{CRespawn, PlayerSpawnData};
            let packet = CRespawn::new(
                PlayerSpawnData::new(
                    world.dimension.clone(),
                    pumpkin_world::biome::hash_seed(world.level.seed.0),
                    avatar.game_mode,
                    -1,
                    false,
                    false,
                    None,
                    0.into(),
                    world.sea_level.into(),
                ),
                CRespawn::KEEP_ALL_DATA,
            );
            let Ok(bytes) = client.serialize_packet(&packet) else {
                return false;
            };
            client.try_enqueue_packet(bytes);
        }
        ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::server::respawn::{RespawnState, SRespawn};
            let position = avatar.position.map(f64::from_bits);
            if cross_world {
                let dimension_id = match world.dimension.minecraft_name {
                    "minecraft:the_nether" => 1,
                    "minecraft:the_end" => 2,
                    _ => 0,
                };
                let packet = pumpkin_protocol::bedrock::client::CChangeDimension {
                    dimension_id: dimension_id.into(),
                    position: Vector3::new(
                        position[0] as f32,
                        position[1] as f32,
                        position[2] as f32,
                    ),
                    respawn: true,
                    loading_screen_id: None,
                };
                let Ok(bytes) = client.serialize_packet(&packet) else {
                    return false;
                };
                client.try_enqueue_packet(bytes);
            }
            let packet = SRespawn {
                position: Vector3::new(
                    position[0] as f32,
                    position[1] as f32 + super::cluster_living_metadata::player_eye_height(state),
                    position[2] as f32,
                ),
                state: RespawnState::ReadyToSpawn,
                player_runtime_id: (player.entity_id() as u64).into(),
            };
            let Ok(bytes) = client.serialize_packet(&packet) else {
                return false;
            };
            client.try_enqueue_packet(bytes);
        }
    }
    if cross_world {
        return super::cluster_player_lifecycle::publish_world_bootstrap(player, world);
    }
    true
}

fn apply_player_image(
    states: &mut HashMap<GlobalPlayerId, ActorLivingState>,
    target: Target,
    image: Option<&pumpkin_cluster::canonical_action::Cell>,
) {
    let Some((_, gid)) = target.player_avatar() else {
        return;
    };
    match (target, image.map(|cell| &cell.value)) {
        (
            Target::Chunk {
                part: ChunkPart::Player(_),
                ..
            },
            Some(Value::Living(state)),
        ) => {
            states.insert(gid, state.as_ref().clone());
        }
        (
            Target::Chunk {
                part: ChunkPart::Player(_),
                ..
            },
            None,
        ) => {
            states.remove(&gid);
        }
        (
            Target::Chunk {
                part: ChunkPart::LivingProperty { property, .. },
                ..
            },
            Some(Value::LivingProperty(value)),
        ) => {
            if let Some(state) = states.get_mut(&gid) {
                assert!(property.write(state, value));
            }
        }
        (
            Target::Chunk {
                part: ChunkPart::ActorRelation { relation, .. },
                ..
            },
            Some(Value::ActorRelation(value)),
        ) => {
            if let Some(state) = states.get_mut(&gid) {
                let mut body = Value::Living((state.clone()).into());
                assert!(pumpkin_cluster::actor_relation::patch(
                    &mut body,
                    relation,
                    value.clone()
                ));
                if let Value::Living(next) = body {
                    *state = *next;
                }
            }
        }
        _ => {}
    }
}

fn publish_experience(
    client: &ClientPlatform,
    entity_id: i32,
    state: pumpkin_cluster::player_experience::PlayerExperienceState,
    tick: pumpkin_cluster::time::TickStamp,
) -> bool {
    if !state.is_valid() {
        return false;
    }
    match client {
        ClientPlatform::Java(client) => {
            let packet = pumpkin_protocol::java::client::play::CSetExperience::new(
                f32::from_bits(state.progress),
                (state.level as i32).into(),
                (state.total as i32).into(),
            );
            let Ok(bytes) = client.serialize_packet(&packet) else {
                return false;
            };
            client.try_enqueue_packet(bytes);
        }
        ClientPlatform::Bedrock(client) => {
            let attribute = |name: &str, current_value, max_value| {
                pumpkin_protocol::bedrock::client::update_attributes::AttributeData {
                    min_value: 0.0,
                    max_value,
                    current_value,
                    default_min_value: 0.0,
                    default_max_value: max_value,
                    default_value: 0.0,
                    name: name.to_owned(),
                    modifiers: Vec::new(),
                }
            };
            let packet = pumpkin_protocol::bedrock::client::update_attributes::CUpdateAttributes {
                target_runtime_id: (entity_id as u64).into(),
                attribute_list: vec![
                    attribute(
                        "minecraft:player.experience",
                        f32::from_bits(state.progress),
                        1.0,
                    ),
                    attribute(
                        "minecraft:player.level",
                        state.level as f32,
                        i32::MAX as f32,
                    ),
                ],
                tick: u64::from(tick.0).into(),
            };
            let Ok(bytes) = client.serialize_packet(&packet) else {
                return false;
            };
            client.try_enqueue_packet(bytes);
        }
    }
    true
}

pub(crate) fn write_owned_player_nbt(
    server: &super::Server,
    state: &ActorLivingState,
    spatial: super::cluster_world_actor::ActorSpatialState,
    nbt: &mut pumpkin_nbt::compound::NbtCompound,
) -> Result<(), &'static str> {
    use crate::entity::NBTStorage;
    use pumpkin_data::data_component_impl::IDSetContent;
    use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};
    let avatar = state.avatar.as_ref().ok_or("missing owned player avatar")?;
    if !avatar.is_valid() || state.food.actor != state.combat.actor {
        return Err("invalid owned player state");
    }
    if !pumpkin_cluster::actor_tags::valid(&state.tags)
        || !super::cluster_advancements::write_nbt(nbt, &state.advancements)
    {
        return Err("invalid owned player metadata");
    }
    nbt.put_list(
        "Tags",
        state
            .tags
            .iter()
            .cloned()
            .map(|tag| NbtTag::String(tag.into_boxed_str()))
            .collect(),
    );
    let worlds = server.worlds.load();
    let world = worlds
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == spatial.world)
        .ok_or("unknown owned player dimension")?;
    let effective = spatial.frame.effective;
    nbt.put(
        "Pos",
        NbtTag::List(effective.position.into_iter().map(NbtTag::Double).collect()),
    );
    nbt.put(
        "Motion",
        NbtTag::List(effective.velocity.into_iter().map(NbtTag::Double).collect()),
    );
    nbt.put(
        "Rotation",
        NbtTag::List(vec![
            NbtTag::Float(effective.yaw),
            NbtTag::Float(effective.pitch),
        ]),
    );
    nbt.put_bool("OnGround", effective.on_ground);
    nbt.put_string("Dimension", world.dimension.minecraft_name.to_owned());
    nbt.put_float("Health", state.combat.health_milli as f32 / 1000.0);
    nbt.put_short("Air", state.environment.air_ticks);
    nbt.put_bool(
        "ClusterDeathInventoryProcessed",
        state.combat.timing.death_inventory_processed,
    );
    nbt.put_int("TicksFrozen", i32::from(state.environment.frozen_ticks));
    nbt.put_float(
        "AbsorptionAmount",
        state.combat.absorption_milli as f32 / 1000.0,
    );
    nbt.put_float(
        "FallDistance",
        if state.combat.health_milli == 0 {
            0.0
        } else {
            f32::from_bits(avatar.fall_distance)
        },
    );
    nbt.put_int("foodLevel", i32::from(state.food.food));
    nbt.put_float(
        "foodSaturationLevel",
        state.food.saturation_milli as f32 / 1000.0,
    );
    nbt.put_float(
        "foodExhaustionLevel",
        state.food.exhaustion_milli as f32 / 1000.0,
    );
    nbt.put_int("foodTickTimer", i32::from(state.food.tick_timer));
    nbt.put_int("playerGameType", i32::from(avatar.game_mode));
    nbt.put_int("XpTotal", avatar.experience.total as i32);
    nbt.put_int("XpLevel", avatar.experience.level as i32);
    nbt.put_float("XpP", f32::from_bits(avatar.experience.progress));
    nbt.put_short(
        "Fire",
        state.combat.timing.fire_ticks.min(i16::MAX as u32) as i16,
    );
    nbt.put_short(
        "HurtTime",
        state.combat.timing.hurt_ticks.min(i16::MAX as u16) as i16,
    );
    nbt.put_short(
        "DeathTime",
        state.combat.timing.death_ticks.min(i16::MAX as u16) as i16,
    );
    nbt.put_bool("Invulnerable", state.combat.status.invulnerable);
    let encode_effect = |effect_state: &pumpkin_cluster::player_admin::EffectState| {
        let effect_type = pumpkin_data::effect::StatusEffect::from_id(effect_state.kind)
            .ok_or("unknown owned player effect")?;
        let effect = pumpkin_data::potion::Effect {
            effect_type,
            duration: effect_state.duration,
            amplifier: effect_state.amplifier,
            ambient: effect_state.ambient,
            show_particles: effect_state.show_particles,
            show_icon: effect_state.show_icon,
            blend: effect_state.blend,
        };
        let mut value = NbtCompound::new();
        effect.write_nbt(&mut value);
        Ok::<_, &'static str>(value)
    };
    let effects = state
        .effects
        .iter()
        .map(|effect| {
            let mut value = encode_effect(effect)?;
            let mut hidden = None;
            for layer in state
                .hidden_effects
                .get(&effect.kind)
                .into_iter()
                .flatten()
                .rev()
            {
                let mut next = encode_effect(layer)?;
                if let Some(child) = hidden {
                    next.put_compound("hidden_effect", child);
                }
                hidden = Some(next);
            }
            if let Some(hidden) = hidden {
                value.put_compound("hidden_effect", hidden);
            }
            Ok(NbtTag::Compound(value))
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    nbt.put("active_effects", NbtTag::List(effects));
    let mut attributes = Vec::with_capacity(state.combat.effective_attributes.len());
    for (id, bits) in &state.combat.effective_attributes {
        let attribute = pumpkin_data::attributes::Attributes::ALL
            .iter()
            .find(|attribute| u16::from(attribute.id) == *id)
            .ok_or("unknown owned player attribute")?;
        let base = f64::from_bits(*bits);
        if !base.is_finite() {
            return Err("invalid owned player attribute");
        }
        let mut value = NbtCompound::new();
        value.put_string("id", attribute.name.to_owned());
        value.put_double("base", base);
        let modifiers = state
            .permanent_attribute_modifiers
            .get(id)
            .into_iter()
            .flatten()
            .map(|modifier| {
                let mut value = NbtCompound::new();
                value.put_string("id", modifier.id.clone());
                value.put_double("amount", f64::from_bits(modifier.amount));
                value.put_string(
                    "operation",
                    match modifier.operation {
                        pumpkin_cluster::combat_action::CombatModifierOperation::Add => "add_value",
                        pumpkin_cluster::combat_action::CombatModifierOperation::MultiplyBase => {
                            "add_multiplied_base"
                        }
                        pumpkin_cluster::combat_action::CombatModifierOperation::MultiplyTotal => {
                            "add_multiplied_total"
                        }
                    }
                    .to_owned(),
                );
                NbtTag::Compound(value)
            })
            .collect();
        value.put("modifiers", NbtTag::List(modifiers));
        attributes.push(NbtTag::Compound(value));
    }
    nbt.child_tags.remove("Attributes");
    nbt.put("attributes", NbtTag::List(attributes));
    let mut statistics = NbtCompound::new();
    for ((category, key), value) in &avatar.statistics {
        statistics.put_int(
            &format!("{category}:{key}"),
            (*value).min(i32::MAX as u32) as i32,
        );
    }
    nbt.put_compound("Statistics", statistics);
    for key in [
        "SpawnX",
        "SpawnY",
        "SpawnZ",
        "SpawnDimension",
        "SpawnForced",
        "respawn",
    ] {
        nbt.child_tags.remove(key);
    }
    if let Some(respawn) = avatar.presentation.respawn {
        let world = worlds
            .iter()
            .find(|world| super::cluster_world_delta::world_id(world) == respawn.world)
            .ok_or("unknown owned respawn dimension")?;
        nbt.put_int("SpawnX", respawn.position.x);
        nbt.put_int("SpawnY", respawn.position.y);
        nbt.put_int("SpawnZ", respawn.position.z);
        nbt.put_string("SpawnDimension", world.dimension.minecraft_name.to_owned());
        nbt.put_bool("SpawnForced", respawn.forced);
        let mut value = NbtCompound::new();
        value.put_string("dimension", world.dimension.minecraft_name.to_owned());
        value.put(
            "pos",
            NbtTag::IntArray(vec![
                respawn.position.x,
                respawn.position.y,
                respawn.position.z,
            ]),
        );
        value.put_float("angle", f32::from_bits(respawn.yaw));
        value.put_bool("forced", respawn.forced);
        nbt.put_compound("respawn", value);
    }
    Ok(())
}

pub(crate) fn publish_bedrock_handoff(
    server: &super::Server,
    player: &std::sync::Arc<crate::entity::player::Player>,
    world: &std::sync::Arc<crate::world::World>,
    _announce_join: bool,
) -> bool {
    let ClientPlatform::Bedrock(client) = player.client.as_ref() else {
        return false;
    };
    let Some(lease) = super::cluster_world_owner::player_action_lease(server, player) else {
        return false;
    };
    let generation = player
        .cluster_owner_install_generation
        .load(std::sync::atomic::Ordering::Acquire);
    if !server
        .cluster_controller
        .installed_for_handoff(lease, generation)
    {
        return false;
    }
    let Some(completion) = server
        .cluster_controller
        .reserve_handoff_completion(lease, generation)
    else {
        return false;
    };
    let living = server.cluster_actor_living.load();
    let Some(state) = living.get(&pumpkin_cluster::actor::ActorId::Player(lease.gid)) else {
        return false;
    };
    let Some(avatar) = state.avatar.as_ref() else {
        return false;
    };
    let Some(incarnation) = target_incarnation(server, lease.gid, avatar.spatial_epoch) else {
        return false;
    };
    if incarnation != lease.incarnation
        || avatar.world != super::cluster_world_delta::world_id(world)
    {
        return false;
    }
    let Some(inventory) =
        super::cluster_inventory_protocol::prepare_initial_inventory(server, player)
    else {
        return false;
    };
    let Ok(attributes) = super::cluster_living_metadata::prepare_player_attributes(
        player.client.as_ref(),
        player.entity_id(),
        None,
        state,
        avatar.movement_tick,
        false,
    ) else {
        return false;
    };
    let Ok(sleep_packets) = super::cluster_living_metadata::prepare_player_sleep_transition(
        player.client.as_ref(),
        player.entity_id(),
        None,
        state,
    ) else {
        return false;
    };
    let Some(bootstrap_server) = world
        .server
        .upgrade()
        .filter(|owner| std::ptr::eq(owner.as_ref(), server))
    else {
        return false;
    };
    let Some(bootstrap) = super::cluster_player_lifecycle::prepare_bootstrap(server, player, world)
    else {
        return false;
    };
    let position = avatar.position.map(f64::from_bits);
    let pose = pumpkin_protocol::bedrock::client::CMovePlayer::new(
        (player.entity_id() as u64).into(),
        Vector3::new(
            position[0] as f32,
            position[1] as f32 + super::cluster_living_metadata::player_eye_height(state),
            position[2] as f32,
        ),
        f32::from_bits(avatar.pitch),
        f32::from_bits(avatar.yaw),
        f32::from_bits(avatar.head_yaw),
        pumpkin_protocol::bedrock::client::CMovePlayer::MODE_TELEPORT,
        avatar.on_ground,
        0u64.into(),
        0,
        0,
        u64::from(avatar.movement_tick.0).into(),
    );
    let metadata = pumpkin_protocol::bedrock::client::CSetActorData {
        target_runtime_id: (player.entity_id() as u64).into(),
        actor_data: super::cluster_living_metadata::bedrock_player_presentation(state),
        synced_properties: Default::default(),
        tick: u64::from(avatar.movement_tick.0).into(),
    };
    let health = pumpkin_protocol::bedrock::client::CSetHealth {
        health: i32::try_from(state.combat.health_milli.div_ceil(1_000))
            .unwrap_or(i32::MAX)
            .into(),
    };
    let Ok(mode) = pumpkin_util::gamemode::GameMode::try_from(i32::from(avatar.game_mode)) else {
        return false;
    };
    let gamemode = pumpkin_protocol::bedrock::client::set_player_gamemode::CSetPlayerGameType {
        player_game_type: mode.into(),
    };
    let abilities = bedrock_handoff_abilities(player, avatar.game_mode);
    let (Ok(pose), Ok(metadata), Ok(health), Ok(gamemode), Ok(abilities)) = (
        client.serialize_packet(&pose),
        client.serialize_packet(&metadata),
        client.serialize_packet(&health),
        client.serialize_packet(&gamemode),
        client.serialize_packet(&abilities),
    ) else {
        return false;
    };
    if !publish_owned_respawn(server, player, state, false) {
        return false;
    }
    client.try_enqueue_packet(pose);
    for packet in inventory {
        client.try_enqueue_packet(packet);
    }
    client.try_enqueue_packet(metadata);
    for packet in attributes {
        client.try_enqueue_packet(packet);
    }
    for packet in sleep_packets {
        client.try_enqueue_packet(packet);
    }
    client.try_enqueue_packet(health);
    client.try_enqueue_packet(gamemode);
    client.try_enqueue_packet(abilities);
    if !publish_bedrock_hud(player, state) {
        return false;
    }
    if !publish_experience(
        player.client.as_ref(),
        player.entity_id(),
        avatar.experience,
        avatar.movement_tick,
    ) {
        return false;
    }
    let mut before = state.clone();
    before.effects.clear();
    before.hidden_effects.clear();
    if !super::cluster_living_metadata::publish_player_effect_delta(
        player.client.as_ref(),
        player.entity_id(),
        &before,
        state,
        avatar.movement_tick,
    ) {
        return false;
    }
    super::cluster_player_lifecycle::publish_bootstrap(&bootstrap_server, player, bootstrap);
    completion.complete();
    true
}

fn bedrock_handoff_abilities(
    player: &crate::entity::player::Player,
    game_mode: u8,
) -> pumpkin_protocol::bedrock::client::update_abilities::CUpdateAbilities {
    use pumpkin_protocol::bedrock::client::update_abilities::{Ability, CUpdateAbilities};
    use pumpkin_protocol::bedrock::client::{
        CommandPermissionLevel, PlayerPermissionLevel, SerializedAbilitiesData,
        SerializedAbilitiesDataSerializedLayer,
    };
    let spectator = game_mode == 3;
    let creative = game_mode == 1;
    let operator = player.permission_lvl.load() == pumpkin_util::permission::PermissionLvl::Four;
    let mut value = 0;
    let mut set = |ability: Ability, enabled: bool| {
        if enabled {
            value |= 1 << ability as u32;
        }
    };
    set(Ability::MayFly, creative || spectator);
    set(
        Ability::Flying,
        spectator || player.cluster_handoff_abilities.flying,
    );
    set(Ability::Invulnerable, creative || spectator);
    set(Ability::Instabuild, creative);
    set(Ability::NoClip, spectator);
    set(Ability::OperatorCommands, operator);
    set(Ability::Teleport, operator);
    for ability in [
        Ability::Build,
        Ability::Mine,
        Ability::DoorsAndSwitches,
        Ability::OpenContainers,
        Ability::AttackPlayers,
        Ability::AttackMobs,
    ] {
        set(ability, !spectator);
    }
    CUpdateAbilities {
        data: SerializedAbilitiesData {
            target_player_raw_id: i64::from(player.entity_id()),
            player_permissions: if operator {
                PlayerPermissionLevel::Operator
            } else {
                PlayerPermissionLevel::Member
            },
            command_permissions: if operator {
                CommandPermissionLevel::GameDirectors
            } else {
                CommandPermissionLevel::Any
            },
            layers: vec![SerializedAbilitiesDataSerializedLayer {
                serialized_layer: 0,
                abilities_set: (1 << Ability::AbilityCount as u32) - 1,
                ability_value: value,
                fly_speed: 0.05,
                vertical_fly_speed: 1.0,
                walk_speed: 0.1,
            }],
        },
    }
}

pub(crate) fn publish_java_handoff(
    server: &super::Server,
    player: &std::sync::Arc<crate::entity::player::Player>,
    world: &std::sync::Arc<crate::world::World>,
    _announce_join: bool,
) -> bool {
    use pumpkin_protocol::java::client::play::{
        CGameEvent, CPlayerAbilities, CPlayerPosition, CRemoveEntities, CSetCamera,
        CSetEntityMetadata, CSetHealth, GameEvent,
    };
    let ClientPlatform::Java(client) = player.client.as_ref() else {
        return false;
    };
    let Some(lease) = super::cluster_world_owner::player_action_lease(server, player) else {
        return false;
    };
    let generation = player
        .cluster_owner_install_generation
        .load(std::sync::atomic::Ordering::Acquire);
    if !server
        .cluster_controller
        .installed_for_handoff(lease, generation)
    {
        return false;
    }
    let Some(completion) = server
        .cluster_controller
        .reserve_handoff_completion(lease, generation)
    else {
        return false;
    };
    let living = server.cluster_actor_living.load();
    let Some(state) = living.get(&pumpkin_cluster::actor::ActorId::Player(lease.gid)) else {
        return false;
    };
    let Some(avatar) = state.avatar.as_ref() else {
        return false;
    };
    let Some(incarnation) = target_incarnation(server, lease.gid, avatar.spatial_epoch) else {
        return false;
    };
    if incarnation != lease.incarnation
        || avatar.world != super::cluster_world_delta::world_id(world)
    {
        return false;
    }
    let Some(inventory) =
        super::cluster_inventory_protocol::prepare_initial_inventory(server, player)
    else {
        return false;
    };
    let Ok(attributes) = super::cluster_living_metadata::prepare_player_attributes(
        player.client.as_ref(),
        player.entity_id(),
        None,
        state,
        avatar.movement_tick,
        false,
    ) else {
        return false;
    };
    let Ok(sleep_packets) = super::cluster_living_metadata::prepare_player_sleep_transition(
        player.client.as_ref(),
        player.entity_id(),
        None,
        state,
    ) else {
        return false;
    };
    let Some(bootstrap_server) = world
        .server
        .upgrade()
        .filter(|owner| std::ptr::eq(owner.as_ref(), server))
    else {
        return false;
    };
    let Some(bootstrap) = super::cluster_player_lifecycle::prepare_bootstrap(server, player, world)
    else {
        return false;
    };
    let Some(camera) = initial_camera_id(server, player, avatar) else {
        return false;
    };
    let position = avatar.position.map(f64::from_bits);
    let position = Vector3::new(position[0], position[1], position[2]);
    let teleport = player
        .teleport_id_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .wrapping_add(1);
    let pose = CPlayerPosition::new(
        teleport.into(),
        position,
        Vector3::new(0.0, 0.0, 0.0),
        f32::from_bits(avatar.yaw),
        f32::from_bits(avatar.pitch),
        Vec::new(),
    );
    let health = CSetHealth::new(
        state.combat.health_milli as f32 / 1000.0,
        i32::from(state.food.food).into(),
        state.food.saturation_milli as f32 / 1000.0,
    );
    let creative = avatar.game_mode == 1;
    let spectator = avatar.game_mode == 3;
    let flying = spectator || player.cluster_handoff_abilities.flying;
    let flags = u8::from(creative || spectator)
        | u8::from(flying) << 1
        | u8::from(creative || spectator) << 2
        | u8::from(creative) << 3;
    let abilities = CPlayerAbilities::new(
        flags as i8,
        player.cluster_handoff_abilities.fly_speed,
        player.cluster_handoff_abilities.walk_speed,
    );
    let gamemode = CGameEvent::new(GameEvent::ChangeGameMode, f32::from(avatar.game_mode));
    let camera = CSetCamera::new(camera.into());
    let anchor_ids = [super::cluster_lobby::lobby_anchor_id().into()];
    let remove_anchor = CRemoveEntities::new(&anchor_ids);
    let Ok(metadata) =
        super::cluster_living_metadata::java_player_presentation(state, client.version.load())
    else {
        return false;
    };
    let metadata = CSetEntityMetadata::new(player.entity_id().into(), metadata.into_boxed_slice());
    let (
        Ok(pose),
        Ok(health),
        Ok(abilities),
        Ok(gamemode),
        Ok(camera),
        Ok(remove_anchor),
        Ok(metadata),
    ) = (
        client.serialize_packet(&pose),
        client.serialize_packet(&health),
        client.serialize_packet(&abilities),
        client.serialize_packet(&gamemode),
        client.serialize_packet(&camera),
        client.serialize_packet(&remove_anchor),
        client.serialize_packet(&metadata),
    )
    else {
        return false;
    };
    if !server.cluster_controller.note_epoch_teleport(
        lease.gid,
        avatar.spatial_epoch,
        teleport.into(),
        position,
    ) {
        return false;
    }
    client.try_enqueue_packet(pose);
    for packet in inventory {
        client.try_enqueue_packet(packet);
    }
    client.try_enqueue_packet(health);
    client.try_enqueue_packet(abilities);
    client.try_enqueue_packet(gamemode);
    client.try_enqueue_packet(camera);
    client.try_enqueue_packet(remove_anchor);
    client.try_enqueue_packet(metadata);
    for packet in attributes {
        client.try_enqueue_packet(packet);
    }
    for packet in sleep_packets {
        client.try_enqueue_packet(packet);
    }
    if !publish_experience(
        player.client.as_ref(),
        player.entity_id(),
        avatar.experience,
        avatar.movement_tick,
    ) {
        return false;
    }
    let mut before = state.clone();
    before.effects.clear();
    before.hidden_effects.clear();
    if !super::cluster_living_metadata::publish_player_effect_delta(
        player.client.as_ref(),
        player.entity_id(),
        &before,
        state,
        avatar.movement_tick,
    ) {
        return false;
    }
    super::cluster_player_lifecycle::publish_bootstrap(&bootstrap_server, player, bootstrap);
    completion.complete();
    true
}

fn initial_camera_id(
    server: &super::Server,
    player: &crate::entity::player::Player,
    avatar: &pumpkin_cluster::item_action::PlayerAvatarState,
) -> Option<i32> {
    let Some(contact) = avatar.presentation.camera_target.as_ref() else {
        return Some(player.entity_id());
    };
    server
        .cluster_combat_client_targets
        .load()
        .iter()
        .find(|(_, entry)| entry.contact.identity == contact.identity)
        .map(|(id, _)| *id)
}

fn publish_bedrock_hud(player: &crate::entity::player::Player, state: &ActorLivingState) -> bool {
    let ClientPlatform::Bedrock(client) = player.client.as_ref() else {
        return true;
    };
    let Some(avatar) = state.avatar.as_ref() else {
        return false;
    };
    let health_id = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
    let Some(base) = state
        .combat
        .effective_attributes
        .get(&health_id)
        .copied()
        .map(f64::from_bits)
    else {
        return false;
    };
    let maximum = state.attribute(health_id, base);
    if !maximum.is_finite() || maximum < 0.0 || maximum > f64::from(f32::MAX) {
        return false;
    }
    let maximum = maximum as f32;
    let attribute = |name: &str, current_value: f32, max_value: f32| {
        pumpkin_protocol::bedrock::client::update_attributes::AttributeData {
            min_value: 0.0,
            max_value,
            current_value,
            default_min_value: 0.0,
            default_max_value: max_value,
            default_value: max_value,
            name: name.to_owned(),
            modifiers: Vec::new(),
        }
    };
    let packet = pumpkin_protocol::bedrock::client::update_attributes::CUpdateAttributes {
        target_runtime_id: (player.entity_id() as u64).into(),
        attribute_list: vec![
            attribute(
                "minecraft:health",
                state.combat.health_milli as f32 / 1000.0,
                maximum,
            ),
            attribute("minecraft:player.hunger", f32::from(state.food.food), 20.0),
            attribute(
                "minecraft:player.saturation",
                state.food.saturation_milli as f32 / 1000.0,
                20.0,
            ),
            attribute(
                "minecraft:player.exhaustion",
                state.food.exhaustion_milli as f32 / 1000.0,
                4.0,
            ),
        ],
        tick: u64::from(avatar.movement_tick.0).into(),
    };
    let Ok(bytes) = client.serialize_packet(&packet) else {
        return false;
    };
    client.try_enqueue_packet(bytes);
    true
}

fn target_incarnation(
    server: &super::Server,
    actor: GlobalPlayerId,
    epoch: SpatialEpoch,
) -> Option<u64> {
    let incarnation = server
        .cluster_actor_statistics
        .load()
        .get(&actor)?
        .incarnation;
    if let SpatialEpoch::Admission { lease, .. } = epoch
        && (lease.gid != actor || lease.incarnation != incarnation)
    {
        return None;
    }
    Some(incarnation)
}
