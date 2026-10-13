use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::difficulty::Difficulty;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;
use uuid::Uuid;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::coordinates::vec3::Vec3ArgumentType;
use crate::command::argument_types::nbt::NbtCompoundArgumentType;
use crate::command::argument_types::resource::{ENTITY_TYPE_ARGUMENT, ResourceArgument};
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::entity::r#type::from_type;
use crate::world::World;

const DESCRIPTION: &str = "Spawns an entity at position.";
const PERMISSION: &str = "minecraft:command.summon";

const ERROR_FAILED_PEACEFUL: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SUMMON_FAILED_PEACEFUL,
    translation::bedrock::COMMANDS_SUMMON_FAILED,
);
const ERROR_DUPLICATE_UUID: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SUMMON_FAILED_UUID,
    translation::bedrock::COMMANDS_SUMMON_FAILED,
);
const ERROR_FAILED: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SUMMON_FAILED,
    translation::bedrock::COMMANDS_SUMMON_FAILED,
);
const INVALID_POSITION: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SUMMON_INVALIDPOSITION,
    translation::bedrock::COMMANDS_SUMMON_OUTOFWORLD,
);

struct SummonExecutor {
    has_pos: bool,
    has_nbt: bool,
}

impl CommandExecutor for SummonExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        self.execute_outcome(context).map(|execution| match execution {
            pumpkin_command::deferred::CommandExecution::Complete(value) => value,
            pumpkin_command::deferred::CommandExecution::Deferred(_) => 0,
        })
    }

    fn execute_outcome(&self, context: &CommandContext) -> Result<pumpkin_command::deferred::CommandExecution, pumpkin_command::errors::command_syntax_error::CommandSyntaxError> {
        let entity_type = ResourceArgument::get_summonable_entity_type(context, "entity")?;
        if context.source.server.as_ref().is_some_and(|server| server.advanced_config.cluster.enabled)
            && matches!(context.source.output, crate::command::CommandSender::Lobby(_) | crate::command::CommandSender::PreJoin(_)) {
            if self.has_pos {
                let coordinates = Vec3ArgumentType::get_coordinates(context, "pos")?;
                if [pumpkin_util::math::vector3::Axis::X, pumpkin_util::math::vector3::Axis::Y,
                    pumpkin_util::math::vector3::Axis::Z].into_iter().enumerate().any(|(index, axis)|
                        coordinates.is_relative(axis) && !context.source.position_known[index]) {
                    return Err(INVALID_POSITION.create_without_context());
                }
            } else if !context.source.position_known.iter().all(|known| *known) {
                return Err(INVALID_POSITION.create_without_context());
            }
        }
        let pos = if self.has_pos {
            Vec3ArgumentType::get_coordinates(context, "pos")?.resolve(&context.source)
        } else {
            context.source.position
        };

        let block_pos = BlockPos::floored_v(pos);
        if !World::is_valid(block_pos) {
            return Err(INVALID_POSITION.create_without_context());
        }

        let world = context.source.world();
        if !context.source.server.as_ref().is_some_and(|server| server.advanced_config.cluster.enabled)
            && world.level_info.load().difficulty == Difficulty::Peaceful && !entity_type.category.is_friendly {
            return Err(ERROR_FAILED_PEACEFUL.create_without_context());
        }

        let nbt = if self.has_nbt {
            Some(NbtCompoundArgumentType::get(context, "nbt")?)
        } else {
            None
        };

        if let Some(server) = context.source.server.as_ref().filter(|server| server.advanced_config.cluster.enabled)
            && (entity_type.mob || crate::server::cluster_vehicle_birth::vehicle_kind(entity_type).is_some()) {
            let empty = pumpkin_nbt::compound::NbtCompound::new();
            let root = nbt.unwrap_or(&empty);
            if root.has("UUID") {
                return Err(ERROR_FAILED.create_without_context());
            }
            let motion = root.get_list("Motion");
            let velocity = std::array::from_fn::<_, 3, _>(|axis| motion.and_then(|values| values.get(axis))
                .and_then(pumpkin_nbt::tag::NbtTag::extract_double).unwrap_or(0.0));
            if velocity.iter().any(|value| !value.is_finite() || value.abs() > 1_000_000.0) {
                return Err(ERROR_FAILED.create_without_context());
            }
            let rotation = root.get_list("Rotation");
            let yaw = rotation.and_then(|values| values.first()).and_then(pumpkin_nbt::tag::NbtTag::extract_float).unwrap_or(0.0);
            let pitch = rotation.and_then(|values| values.get(1)).and_then(pumpkin_nbt::tag::NbtTag::extract_float).unwrap_or(0.0);
            let spatial = pumpkin_cluster::entity_body::EntitySpatial {
                world: crate::server::cluster_world_delta::world_id(world), position: [pos.x, pos.y, pos.z].map(f64::to_bits),
                velocity_micro: velocity.map(|value| (value * 1_000_000.0).round() as i64),
                yaw: yaw.to_bits(), pitch: pitch.to_bits(), on_ground: root.get_bool("OnGround").unwrap_or(false),
            };
            let initial = if entity_type.mob {
                crate::server::cluster_owned_mob_initial::parse_initialization(entity_type, &spatial, root)
            } else {
                crate::server::cluster_vehicle_birth::parse_initialization(entity_type, root)
            }.map_err(|_| ERROR_FAILED.create_without_context())?;
            let request = pumpkin_cluster::mob_initialization::EntityAdmissionRequest { kind: entity_type.id, spatial, initial };
            let token = crate::server::cluster_player_commands::submit_summon_deferred(server, context.source.clone(), request)
                .map_err(|_| ERROR_FAILED.create_without_context())?;
            return Ok(pumpkin_command::deferred::CommandExecution::Deferred(token));
        }

        if context.source.server.as_ref().is_some_and(|server| server.advanced_config.cluster.enabled) {
            return Err(ERROR_FAILED.create_without_context());
        }

        let uuid = nbt
            .and_then(|nbt| nbt.get_uuid("UUID"))
            .unwrap_or_else(Uuid::new_v4);

        let entity = from_type(entity_type, pos, world, uuid);
        if let Some(nbt) = nbt {
            entity.read_nbt_non_mut(nbt);
            entity.get_entity().set_pos(pos);
        }

        if world
            .entities
            .load()
            .iter()
            .any(|e| e.get_entity().entity_uuid == entity.get_entity().entity_uuid)
        {
            return Err(ERROR_DUPLICATE_UUID.create_without_context());
        }

        let name = entity.get_display_name();
        world.spawn_entity(entity);

        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_SUMMON_SUCCESS,
                translation::bedrock::COMMANDS_SUMMON_SUCCESS,
                [name],
            ),
            true,
        );

        Ok(pumpkin_command::deferred::CommandExecution::Complete(1))
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    dispatcher.register(
        command("summon", DESCRIPTION).requires(PERMISSION).then(
            argument("entity", ENTITY_TYPE_ARGUMENT.clone())
                .executes(SummonExecutor {
                    has_pos: false,
                    has_nbt: false,
                })
                .then(
                    argument("pos", Vec3ArgumentType::Default)
                        .executes(SummonExecutor {
                            has_pos: true,
                            has_nbt: false,
                        })
                        .then(
                            argument("nbt", NbtCompoundArgumentType).executes(SummonExecutor {
                                has_pos: true,
                                has_nbt: true,
                            }),
                        ),
                ),
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::commands::fetchprofile;
    use crate::command::context::command_source::CommandSource;
    use std::sync::Arc;

    #[test]
    fn parse_summon_cat() {
        let mut dispatcher = CommandDispatcher::new();
        let registry = PermissionRegistry::default();

        // Register fetchprofile first to ensure its "entity" argument doesn't collide
        fetchprofile::register(&mut dispatcher, &registry);
        register(&mut dispatcher, &registry);

        let source = Arc::new(CommandSource::dummy());
        let result = dispatcher.parse_input("summon cat", &source);
        assert!(result.errors.is_empty());
        let context = result.context.build("summon cat");
        let entity_type = ResourceArgument::get_summonable_entity_type(&context, "entity")
            .expect("Should resolve summonable cat entity type");
        assert_eq!(entity_type.resource_name, "cat");
    }

    #[test]
    fn parse_summon_cat_with_pos_and_nbt() {
        let mut dispatcher = CommandDispatcher::new();
        let registry = PermissionRegistry::default();

        fetchprofile::register(&mut dispatcher, &registry);
        register(&mut dispatcher, &registry);

        let source = Arc::new(CommandSource::dummy());
        let result = dispatcher.parse_input("summon cat ~ ~ ~ {NoAI:1b}", &source);
        assert!(result.errors.is_empty());
        let context = result.context.build("summon cat ~ ~ ~ {NoAI:1b}");
        let entity_type = ResourceArgument::get_summonable_entity_type(&context, "entity")
            .expect("Should resolve summonable cat entity type");
        assert_eq!(entity_type.resource_name, "cat");
        let nbt =
            NbtCompoundArgumentType::get(&context, "nbt").expect("Should resolve NBT argument");
        assert_eq!(nbt.get_byte("NoAI"), Some(1));
    }
}
