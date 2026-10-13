use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

const DESCRIPTION: &str = "Kills all target entities.";

const PERMISSION: &str = "minecraft:command.kill";

const ARG_TARGETS: &str = "targets";

struct TargetsExecutor;

impl CommandExecutor for TargetsExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = EntityArgumentType::get_entities(context, ARG_TARGETS)?;

        if let Some(server) = context.source.server.as_ref().filter(|server| server.advanced_config.cluster.enabled) {
            let mut queued = 0;
            for target in &targets {
                if queue_canonical_kill(context, server, target.as_ref()) { queued += 1; }
            }
            return Ok(queued);
        }
        let target_count = targets.len();
        for target in &targets {
            target.kill(target.as_ref());
        }

        let msg = if target_count == 1 {
            TextComponent::translate_cross(
                translation::java::COMMANDS_KILL_SUCCESS_SINGLE,
                translation::bedrock::COMMANDS_KILL_SUCCESSFUL,
                [targets[0].get_display_name()],
            )
        } else {
            TextComponent::translate_cross(
                translation::java::COMMANDS_KILL_SUCCESS_MULTIPLE,
                translation::bedrock::COMMANDS_KILL_SUCCESSFUL,
                [TextComponent::text(target_count.to_string())],
            )
        };

        context.source.send_feedback(msg, true);

        Ok(target_count as i32)
    }
}

struct SelfExecutor;

impl CommandExecutor for SelfExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let target = context.source.entity_or_err()?;
        if let Some(server) = context.source.server.as_ref().filter(|server| server.advanced_config.cluster.enabled) {
            return Ok(i32::from(queue_canonical_kill(context, server, target.as_ref())));
        }
        target.kill(&*target);

        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_KILL_SUCCESS_SINGLE,
                translation::bedrock::COMMANDS_KILL_SUCCESSFUL,
                [target.get_display_name()],
            ),
            true,
        );

        Ok(1)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    dispatcher.register(
        command("kill", DESCRIPTION)
            .requires(PERMISSION)
            .then(argument(ARG_TARGETS, EntityArgumentType::Entities).executes(TargetsExecutor))
            .executes(SelfExecutor),
    );
}

fn queue_canonical_kill(context: &CommandContext, server: &crate::server::Server,
    target: &dyn crate::entity::EntityBase) -> bool {
    let Some(target) = crate::server::cluster_player_commands::entity_target(server, target) else {
        context.source.send_error(TextComponent::text("Kill target has no current canonical identity"));
        return false;
    };
    match crate::server::cluster_player_commands::submit_target(server, &context.source, target,
        pumpkin_cluster::player_command_control::PlayerCommandMutation::Kill,
        Some(TextComponent::text("Kill applied"))) {
        Ok(_) => true,
        Err(error) => {
            context.source.send_error(TextComponent::text(format!("Kill could not be queued: {error:?}")));
            false
        }
    }
}
