use std::sync::Arc;

use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::vector3::{Axis, Vector3};
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

fn resolve_position(
    context: &CommandContext,
    name: &str,
) -> Result<Vector3<f64>, crate::command::errors::command_syntax_error::CommandSyntaxError> {
    let coordinates = Vec3ArgumentType::get_coordinates(context, name)?;
    if matches!(
        &context.source.output,
        crate::command::CommandSender::PreJoin(_)
    ) && [Axis::X, Axis::Y, Axis::Z]
        .into_iter()
        .any(|axis| coordinates.is_relative(axis))
    {
        context.source.entity_or_err()?;
    }
    Ok(coordinates.resolve(&context.source))
}

fn resolve_rotation(
    context: &CommandContext,
) -> Result<
    pumpkin_util::math::vector2::Vector2<f32>,
    crate::command::errors::command_syntax_error::CommandSyntaxError,
> {
    let coordinates = RotationArgumentType::get(context, "rotation")?;
    if matches!(
        &context.source.output,
        crate::command::CommandSender::PreJoin(_)
    ) && [Axis::X, Axis::Y]
        .into_iter()
        .any(|axis| coordinates.is_relative(axis))
    {
        context.source.entity_or_err()?;
    }
    Ok(coordinates.rotation(&context.source))
}

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

fn submit_player_teleport(
    context: &CommandContext,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    uuid: [u8; 16],
    destination: pumpkin_cluster::player_command_control::TeleportDestination,
) -> CommandExecutorResult {
    let directory = crate::server::cluster_presence::all_presence_entries(context.server());
    let target_name = directory
        .iter()
        .find(|(_, entry)| entry.uuid == uuid)
        .map_or_else(
            || uuid::Uuid::from_bytes(uuid).to_string(),
            |(_, entry)| entry.name.clone(),
        );
    let feedback = match &destination {
        pumpkin_cluster::player_command_control::TeleportDestination::Absolute {
            position, ..
        } => TextComponent::translate_cross(
            translation::java::COMMANDS_TELEPORT_SUCCESS_LOCATION_SINGLE,
            translation::bedrock::COMMANDS_TP_SUCCESS_COORDINATES,
            [
                TextComponent::text(target_name),
                TextComponent::text(position[0].to_string()),
                TextComponent::text(position[1].to_string()),
                TextComponent::text(position[2].to_string()),
            ],
        ),
        pumpkin_cluster::player_command_control::TeleportDestination::Player {
            uuid: destination_uuid,
            ..
        } => {
            let destination_name = directory
                .iter()
                .find(|(_, entry)| entry.uuid == *destination_uuid)
                .map_or_else(
                    || uuid::Uuid::from_bytes(*destination_uuid).to_string(),
                    |(_, entry)| entry.name.clone(),
                );
            TextComponent::translate_cross(
                translation::java::COMMANDS_TELEPORT_SUCCESS_ENTITY_SINGLE,
                translation::bedrock::COMMANDS_TP_SUCCESSVICTIM,
                [
                    TextComponent::text(target_name),
                    TextComponent::text(destination_name),
                ],
            )
        }
    };
    match crate::server::cluster_player_commands::submit_with_feedback(
        context.server(),
        &context.source,
        gid,
        uuid,
        pumpkin_cluster::player_command_control::PlayerCommandMutation::Teleport(destination),
        Some(feedback),
    ) {
        Ok(_) => Ok(1),
        Err(error) => {
            context.source.send_error(TextComponent::text(format!(
                "Cluster teleport unavailable: {error:?}"
            )));
            Ok(0)
        }
    }
}

fn named_identity(
    context: &CommandContext,
    argument: &str,
) -> Option<(pumpkin_cluster::identity::GlobalPlayerId, [u8; 16])> {
    let selector = context.get_argument::<EntitySelector>(argument).ok()?;
    let directory = crate::server::cluster_presence::all_presence_entries(context.server());
    let (gid, entry) = if let Some(name) = selector.player_name.as_deref() {
        directory
            .into_iter()
            .find(|(_, entry)| entry.name.eq_ignore_ascii_case(name))?
    } else {
        let entity = EntityArgumentType::get_entity(context, argument).ok()?;
        let uuid = entity.get_entity().entity_uuid;
        directory
            .into_iter()
            .find(|(_, entry)| entry.uuid == *uuid.as_bytes())?
    };
    (!entry.in_lobby).then_some((gid, entry.uuid))
}

fn player_target_identities(
    context: &CommandContext,
    argument: &str,
) -> Option<Vec<(pumpkin_cluster::identity::GlobalPlayerId, [u8; 16])>> {
    if let Some(target) = named_identity(context, argument) {
        return Some(vec![target]);
    }
    let entities = EntityArgumentType::get_entities(context, argument).ok()?;
    let directory = crate::server::cluster_presence::all_presence_entries(context.server());
    entities
        .iter()
        .map(|entity| {
            let uuid = entity.get_entity().entity_uuid;
            directory
                .iter()
                .find(|(_, entry)| !entry.in_lobby && entry.uuid == *uuid.as_bytes())
                .map(|(gid, entry)| (*gid, entry.uuid))
        })
        .collect()
}

fn teleport_named_player(
    context: &CommandContext,
    argument: &str,
    pos: Vector3<f64>,
    rotation: Option<(f32, f32)>,
    world: &Arc<World>,
    _destination: Option<TextComponent>,
) -> Option<CommandExecutorResult> {
    if !context.server().advanced_config.cluster.enabled {
        return None;
    }
    if let Some(result) = destination_or_lobby_error(context, argument) {
        return Some(result);
    }
    let targets = player_target_identities(context, argument)?;
    let destination = pumpkin_cluster::player_command_control::TeleportDestination::Absolute {
        world: crate::server::cluster_world_delta::world_id(world),
        position: [pos.x, pos.y, pos.z],
        rotation: rotation.map(|(yaw, pitch)| [yaw, pitch]),
    };
    Some(targets.into_iter().try_fold(0, |count, (gid, uuid)| {
        submit_player_teleport(context, gid, uuid, destination.clone()).map(|value| count + value)
    }))
}

fn teleport_named_destination(
    context: &CommandContext,
    self_target: bool,
) -> Option<CommandExecutorResult> {
    if !context.server().advanced_config.cluster.enabled {
        return None;
    }
    if let Some(result) = destination_or_lobby_error(context, "destination") {
        return Some(result);
    }
    let (destination_gid, destination_uuid) = named_identity(context, "destination")?;
    let targets = if self_target {
        let player = context.source.output.as_player()?;
        vec![(player.cluster_gid()?, *player.gameprofile.id.as_bytes())]
    } else {
        player_target_identities(context, "targets")?
    };
    let destination = pumpkin_cluster::player_command_control::TeleportDestination::Player {
        gid: destination_gid,
        uuid: destination_uuid,
    };
    Some(targets.into_iter().try_fold(0, |count, (gid, uuid)| {
        submit_player_teleport(context, gid, uuid, destination.clone()).map(|value| count + value)
    }))
}

struct SelfToPosExecutor;

impl CommandExecutor for SelfToPosExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let entity = context.source.entity_or_err()?;

        let pos = resolve_position(context, "location")?;
        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        if context.server().advanced_config.cluster.enabled
            && let Some(player) = context.source.output.as_player()
            && let Some(gid) = player.cluster_gid()
        {
            return submit_player_teleport(
                context,
                gid,
                *player.gameprofile.id.as_bytes(),
                pumpkin_cluster::player_command_control::TeleportDestination::Absolute {
                    world: crate::server::cluster_world_delta::world_id(context.source.world()),
                    position: [pos.x, pos.y, pos.z],
                    rotation: None,
                },
            );
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
        if let Some(result) = teleport_named_destination(context, true) {
            return result;
        }

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
            Err(error) => {
                if let Some(result) = destination_or_lobby_error(context, "destination") {
                    return result;
                }
                return Err(error);
            }
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
        if let Some(result) = teleport_named_destination(context, false) {
            return result;
        }
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
            Err(error) => {
                if let Some(result) = destination_or_lobby_error(context, "destination") {
                    return result;
                }
                return Err(error);
            }
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
        let pos = resolve_position(context, "location")?;

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
        let pos = resolve_position(context, "location")?;

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let rot = resolve_rotation(context)?;
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
        let pos = resolve_position(context, "location")?;

        if !World::is_valid(BlockPos(pos.floor_to_i32())) {
            return Err(ERROR_INVALID_POSITION.create_without_context());
        }

        let facing_pos = resolve_position(context, "facingLocation")?;
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
        let pos = resolve_position(context, "location")?;

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
