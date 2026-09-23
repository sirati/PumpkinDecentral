use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pumpkin_cluster::player_admin::{PlayerAdministrativeIntent, PlayerAdministrativeMutation};
use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command, literal};
use crate::command::argument_types::coordinates::rotation::RotationArgumentType;
use crate::command::argument_types::coordinates::vec3::Vec3ArgumentType;
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::argument_types::entity_anchor::{
    EntityAnchor, EntityAnchorArgumentType, EntityAnchorExt,
};
use crate::command::argument_types::entity_selector::EntitySelector;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::entity::EntityBase;
use crate::world::World;

const DESCRIPTION: &str = "Teleports entities, including players.";
const PERMISSION: &str = "minecraft:command.teleport";

const ERROR_INVALID_POSITION: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_TELEPORT_INVALIDPOSITION,
    translation::java::COMMANDS_TELEPORT_INVALIDPOSITION,
);

fn yaw_pitch_facing_position(
    looking_from: &Vector3<f64>,
    looking_towards: &Vector3<f64>,
) -> (f32, f32) {
    let direction_vector = (looking_towards.sub(looking_from)).normalize();

    let yaw_radians = -direction_vector.x.atan2(direction_vector.z);
    let pitch_radians = (-direction_vector.y).asin();

    let yaw_degrees = yaw_radians.to_degrees();
    let pitch_degrees = pitch_radians.to_degrees();

    (yaw_degrees as f32, pitch_degrees as f32)
}

fn success_key_and_arg(
    targets: &[Arc<dyn EntityBase>],
    single_key: &'static str,
    multiple_key: &'static str,
) -> (&'static str, TextComponent) {
    if targets.len() == 1 {
        (single_key, targets[0].get_display_name())
    } else {
        (multiple_key, TextComponent::text(targets.len().to_string()))
    }
}

struct TeleportDestination {
    pos: Vector3<f64>,
    yaw: f32,
    pitch: f32,
    world: Arc<World>,
    display_name: TextComponent,
}

fn remote_destination(context: &CommandContext, argument: &str) -> Option<TeleportDestination> {
    let selector = context.get_argument::<EntitySelector>(argument).ok()?;
    let name = selector.player_name.as_deref()?;
    if crate::server::cluster_presence::remote_player_in_lobby(context.server(), name) {
        return None;
    }
    let (gid, entry) = crate::server::cluster_presence::remote_presence_entries(context.server())
        .into_iter()
        .find(|(_, entry)| entry.name.eq_ignore_ascii_case(name))?;
    let sample = crate::server::cluster_ghost::remote_player_position(gid)?;
    let world = context
        .server()
        .worlds
        .load()
        .iter()
        .find(|world| crate::server::cluster_world_delta::world_id(world) == sample.world)?
        .clone();
    Some(TeleportDestination {
        pos: Vector3::new(sample.pos[0], sample.pos[1], sample.pos[2]),
        yaw: sample.yaw,
        pitch: sample.pitch,
        world,
        display_name: TextComponent::text(entry.name),
    })
}

fn destination_or_lobby_error(
    context: &CommandContext,
    argument: &str,
) -> Option<CommandExecutorResult> {
    let selector = context.get_argument::<EntitySelector>(argument).ok()?;
    let name = selector.player_name.as_deref()?;
    if !crate::server::cluster_presence::remote_player_in_lobby(context.server(), name) {
        return None;
    }
    context.source.send_error(TextComponent::text(format!(
        "{name} is currently in the lobby, and not in the game world"
    )));
    Some(Ok(0))
}

fn teleport_named_player(
    context: &CommandContext,
    argument: &str,
    pos: Vector3<f64>,
    rotation: Option<(f32, f32)>,
    world: &Arc<World>,
    destination: Option<TextComponent>,
) -> Option<CommandExecutorResult> {
    if !context.server().advanced_config.cluster.enabled {
        return None;
    }
    let selector = context.get_argument::<EntitySelector>(argument).ok()?;
    let name = selector.player_name.as_deref()?;
    let target = crate::server::cluster_presence::all_presence_entries(context.server())
        .into_iter()
        .find(|(_, entry)| entry.name.eq_ignore_ascii_case(name))
        .map(|(gid, entry)| (gid, entry.name, entry.in_lobby))
        .or_else(|| {
            let player = context.server().get_player_by_name(name)?;
            Some((
                player.cluster_gid()?,
                player.gameprofile.name.clone(),
                player.is_in_cluster_lobby(),
            ))
        })?;
    let (gid, target_name, in_lobby) = target;
    if in_lobby {
        context.source.send_error(TextComponent::text(format!(
            "{} is currently in the lobby, and not in the game world",
            target_name
        )));
        return Some(Ok(0));
    }
    let (yaw, pitch) = rotation.unwrap_or_else(|| {
        context
            .server()
            .get_player_by_name(&target_name)
            .map(|player| player.rotation())
            .or_else(|| {
                crate::server::cluster_ghost::remote_player_position(gid)
                    .map(|sample| (sample.yaw, sample.pitch))
            })
            .unwrap_or((0.0, 0.0))
    });
    let origin = (1_u64 << 63)
        | context
            .server()
            .cluster_lobby_session_generation
            .fetch_add(1, Ordering::Relaxed);
    let intent = PlayerAdministrativeIntent {
        target: gid,
        mutation: PlayerAdministrativeMutation::Teleport {
            world: crate::server::cluster_world_delta::world_id(world),
            position: [pos.x, pos.y, pos.z],
            yaw,
            pitch,
        },
        origin: Some(origin),
    };
    let mut publications =
        crate::server::cluster_player_admin::subscribe_player_administrative(context.server());
    let mut failures =
        crate::server::cluster_player_admin::subscribe_player_administrative_failures(
            context.server(),
        );
    let submitted = if let Some(issuer) = context.source.output.as_pre_join() {
        crate::server::cluster_player_admin::submit_lobby_player_administrative(
            context.server(),
            issuer.gid,
            issuer.profile.id,
            intent,
        )
    } else {
        crate::server::cluster_player_admin::submit_player_administrative(context.server(), intent)
    };
    if submitted.is_err() {
        context
            .source
            .send_error(TextComponent::text("Cluster teleport unavailable"));
        return Some(Ok(0));
    }
    let feedback = if let Some(destination) = destination {
        TextComponent::translate_cross(
            translation::java::COMMANDS_TELEPORT_SUCCESS_ENTITY_SINGLE,
            translation::bedrock::COMMANDS_TP_SUCCESSVICTIM,
            [TextComponent::text(target_name), destination],
        )
    } else {
        TextComponent::translate_cross(
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
            translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
            [
                TextComponent::text(target_name),
                TextComponent::text(pos.x.to_string()),
                TextComponent::text(pos.y.to_string()),
                TextComponent::text(pos.z.to_string()),
            ],
        )
    };
    let sender = context.source.output.clone();
    let server = context.server().clone();
    server.spawn_task(async move {
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                tokio::select! {
                    publication = publications.recv() => match publication {
                        Ok(publication) if publication.target == gid && publication.origin == Some(origin) => {
                            return publication.accepted;
                        }
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return false,
                    },
                    failure = failures.recv() => match failure {
                        Ok(failure) if failure.target == gid && failure.origin == Some(origin) => {
                            return false;
                        }
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return false,
                    }
                }
            }
        }).await;
        match result {
            Ok(true) => sender.send_message(feedback),
            Ok(false) => sender.send_message(TextComponent::text("Teleport failed")),
            Err(_) => {
                tracing::error!(target = gid.pack_u32(), origin, "cluster teleport did not settle within 30 seconds");
                sender.send_message(TextComponent::text("Teleport timed out"));
            }
        }
    });
    Some(Ok(1))
}

struct SelfToPosExecutor;

impl CommandExecutor for SelfToPosExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let entity = context.source.entity_or_err()?;

        let pos = Vec3ArgumentType::get_coordinates(context, "location")?.resolve(&context.source);
        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let yaw = entity.get_entity().yaw.load();
        let pitch = entity.get_entity().pitch.load();
        let world = context.source.world();
        entity.teleport(pos, Some(yaw), Some(pitch), world.clone());

        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
                translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
                [
                    entity.get_display_name(),
                    TextComponent::text(pos.x.to_string()),
                    TextComponent::text(pos.y.to_string()),
                    TextComponent::text(pos.z.to_string()),
                ],
            ),
            true,
        );

        Ok(1)
    }
}

struct SelfToEntityExecutor;

impl CommandExecutor for SelfToEntityExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let entity = context.source.entity_or_err()?;

        let destination = match EntityArgumentType::get_entity(context, "destination") {
            Ok(destination) => {
                let entity = destination.get_entity();
                TeleportDestination {
                    pos: entity.pos.load(),
                    yaw: entity.yaw.load(),
                    pitch: entity.pitch.load(),
                    world: entity.world.load_full(),
                    display_name: entity.get_display_name(),
                }
            }
            Err(error) => match remote_destination(context, "destination") {
                Some(destination) => destination,
                None => {
                    if let Some(result) = destination_or_lobby_error(context, "destination") {
                        return result;
                    }
                    return Err(error);
                }
            },
        };
        let pos = destination.pos;

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        entity.teleport(
            pos,
            Some(destination.yaw),
            Some(destination.pitch),
            destination.world,
        );

        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_TELEPORT_SUCCESS_ENTITY_SINGLE,
                translation::bedrock::COMMANDS_TP_SUCCESSVICTIM,
                [entity.get_display_name(), destination.display_name],
            ),
            true,
        );

        Ok(1)
    }
}

struct EntitiesToEntityExecutor;

impl CommandExecutor for EntitiesToEntityExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let destination = match EntityArgumentType::get_entity(context, "destination") {
            Ok(destination) => {
                let entity = destination.get_entity();
                TeleportDestination {
                    pos: entity.pos.load(),
                    yaw: entity.yaw.load(),
                    pitch: entity.pitch.load(),
                    world: entity.world.load_full(),
                    display_name: entity.get_display_name(),
                }
            }
            Err(error) => match remote_destination(context, "destination") {
                Some(destination) => destination,
                None => {
                    if let Some(result) = destination_or_lobby_error(context, "destination") {
                        return result;
                    }
                    return Err(error);
                }
            },
        };
        let pos = destination.pos;

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        if let Some(result) = teleport_named_player(
            context,
            "targets",
            pos,
            Some((destination.yaw, destination.pitch)),
            &destination.world,
            Some(destination.display_name.clone()),
        ) {
            return result;
        }

        let targets = EntityArgumentType::get_entities(context, "targets")?;

        for target in &targets {
            target.teleport(
                pos,
                Some(destination.yaw),
                Some(destination.pitch),
                destination.world.clone(),
            );
        }

        let (key, target_arg) = success_key_and_arg(
            &targets,
            translation::java::COMMANDS_TELEPORT_SUCCESS_ENTITY_SINGLE,
            translation::java::COMMANDS_TELEPORT_SUCCESS_ENTITY_MULTIPLE,
        );
        context.source.send_feedback(
            TextComponent::translate_cross(
                key,
                translation::bedrock::COMMANDS_TP_SUCCESSVICTIM,
                [target_arg, destination.display_name],
            ),
            true,
        );

        Ok(targets.len() as i32)
    }
}

struct EntitiesToPosExecutor;

impl CommandExecutor for EntitiesToPosExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let pos = Vec3ArgumentType::get_coordinates(context, "location")?.resolve(&context.source);

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let world = context.source.world();
        if let Some(result) = teleport_named_player(context, "targets", pos, None, world, None) {
            return result;
        }
        let targets = EntityArgumentType::get_entities(context, "targets")?;
        for target in &targets {
            let yaw = target.get_entity().yaw.load();
            let pitch = target.get_entity().pitch.load();
            target.teleport(pos, Some(yaw), Some(pitch), world.clone());
        }

        let (key, target_arg) = success_key_and_arg(
            &targets,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_MULTIPLE,
        );
        context.source.send_feedback(
            TextComponent::translate_cross(
                key,
                translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
                [
                    target_arg,
                    TextComponent::text(pos.x.to_string()),
                    TextComponent::text(pos.y.to_string()),
                    TextComponent::text(pos.z.to_string()),
                ],
            ),
            true,
        );

        Ok(targets.len() as i32)
    }
}

struct EntitiesToPosWithRotationExecutor;

impl CommandExecutor for EntitiesToPosWithRotationExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let pos = Vec3ArgumentType::get_coordinates(context, "location")?.resolve(&context.source);

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let rot = RotationArgumentType::get(context, "rotation")?.rotation(&context.source);
        let yaw = rot.y;
        let pitch = rot.x;
        let world = context.source.world();

        if let Some(result) =
            teleport_named_player(context, "targets", pos, Some((yaw, pitch)), world, None)
        {
            return result;
        }
        let targets = EntityArgumentType::get_entities(context, "targets")?;

        for target in &targets {
            target.teleport(pos, Some(yaw), Some(pitch), world.clone());
        }

        let (key, target_arg) = success_key_and_arg(
            &targets,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_MULTIPLE,
        );
        context.source.send_feedback(
            TextComponent::translate_cross(
                key,
                translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
                [
                    target_arg,
                    TextComponent::text(pos.x.to_string()),
                    TextComponent::text(pos.y.to_string()),
                    TextComponent::text(pos.z.to_string()),
                ],
            ),
            true,
        );

        Ok(targets.len() as i32)
    }
}

struct EntitiesToPosFacingPosExecutor;

impl CommandExecutor for EntitiesToPosFacingPosExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let pos = Vec3ArgumentType::get_coordinates(context, "location")?.resolve(&context.source);

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let facing_pos =
            Vec3ArgumentType::get_coordinates(context, "facingLocation")?.resolve(&context.source);
        let world = context.source.world();
        let (named_yaw, named_pitch) = yaw_pitch_facing_position(&pos, &facing_pos);

        if let Some(result) = teleport_named_player(
            context,
            "targets",
            pos,
            Some((named_yaw, named_pitch)),
            world,
            None,
        ) {
            return result;
        }
        let targets = EntityArgumentType::get_entities(context, "targets")?;

        for target in &targets {
            let eye_offset = match context.source.entity_anchor {
                EntityAnchor::Feet => 0.0,
                EntityAnchor::Eyes => target.get_entity().get_eye_height(),
            };
            let looking_from = Vector3::new(pos.x, pos.y + eye_offset, pos.z);
            let (yaw, pitch) = yaw_pitch_facing_position(&looking_from, &facing_pos);
            target.teleport(pos, Some(yaw), Some(pitch), world.clone());
        }

        let (key, target_arg) = success_key_and_arg(
            &targets,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_MULTIPLE,
        );
        context.source.send_feedback(
            TextComponent::translate_cross(
                key,
                translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
                [
                    target_arg,
                    TextComponent::text(pos.x.to_string()),
                    TextComponent::text(pos.y.to_string()),
                    TextComponent::text(pos.z.to_string()),
                ],
            ),
            true,
        );

        Ok(targets.len() as i32)
    }
}

struct EntitiesToPosFacingEntityExecutor {
    has_anchor: bool,
}

impl CommandExecutor for EntitiesToPosFacingEntityExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let pos = Vec3ArgumentType::get_coordinates(context, "location")?.resolve(&context.source);

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let facing_entity = EntityArgumentType::get_entity(context, "facingEntity")?;
        let anchor = if self.has_anchor {
            EntityAnchorArgumentType::get(context, "facingAnchor")?
        } else {
            EntityAnchor::Feet
        };

        let facing_pos = anchor.position_at_entity(facing_entity.get_entity());
        let world = context.source.world();

        let (yaw, pitch) = yaw_pitch_facing_position(&pos, &facing_pos);
        if let Some(result) =
            teleport_named_player(context, "targets", pos, Some((yaw, pitch)), world, None)
        {
            return result;
        }
        let targets = EntityArgumentType::get_entities(context, "targets")?;

        for target in &targets {
            let eye_offset = match context.source.entity_anchor {
                EntityAnchor::Feet => 0.0,
                EntityAnchor::Eyes => target.get_entity().get_eye_height(),
            };
            let looking_from = Vector3::new(pos.x, pos.y + eye_offset, pos.z);
            let (yaw, pitch) = yaw_pitch_facing_position(&looking_from, &facing_pos);
            target.teleport(pos, Some(yaw), Some(pitch), world.clone());
        }

        let (key, target_arg) = success_key_and_arg(
            &targets,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_MULTIPLE,
        );
        context.source.send_feedback(
            TextComponent::translate_cross(
                key,
                translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
                [
                    target_arg,
                    TextComponent::text(pos.x.to_string()),
                    TextComponent::text(pos.y.to_string()),
                    TextComponent::text(pos.z.to_string()),
                ],
            ),
            true,
        );

        Ok(targets.len() as i32)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    let cmd = command("teleport", DESCRIPTION)
        .requires(PERMISSION)
        .then(argument("location", Vec3ArgumentType::Default).executes(SelfToPosExecutor))
        .then(argument("destination", EntityArgumentType::Entity).executes(SelfToEntityExecutor))
        .then(
            argument("targets", EntityArgumentType::Entities)
                .then(
                    argument("location", Vec3ArgumentType::Default)
                        .executes(EntitiesToPosExecutor)
                        .then(
                            argument("rotation", RotationArgumentType)
                                .executes(EntitiesToPosWithRotationExecutor),
                        )
                        .then(
                            literal("facing")
                                .then(
                                    literal("entity").then(
                                        argument("facingEntity", EntityArgumentType::Entity)
                                            .executes(EntitiesToPosFacingEntityExecutor {
                                                has_anchor: false,
                                            })
                                            .then(
                                                argument("facingAnchor", EntityAnchorArgumentType)
                                                    .executes(EntitiesToPosFacingEntityExecutor {
                                                        has_anchor: true,
                                                    }),
                                            ),
                                    ),
                                )
                                .then(
                                    argument("facingLocation", Vec3ArgumentType::Default)
                                        .executes(EntitiesToPosFacingPosExecutor),
                                ),
                        ),
                )
                .then(
                    argument("destination", EntityArgumentType::Entity)
                        .executes(EntitiesToEntityExecutor),
                ),
        );

    dispatcher.register_with_aliases(cmd, &["tp"]);
}
