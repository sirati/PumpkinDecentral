use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};

const PERMISSION: &str = "pumpkin.command.clustertest";

struct ClusterTestExecutor;

impl CommandExecutor for ClusterTestExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let test_id = StringArgumentType::get(context, "test-id")?;
        if !matches!(
            test_id,
            "arena"
                | "controller-divergence"
                | "beacon"
                | "departure-admin-lobby"
                | "departure-admin-disconnect"
        ) {
            context
                .source
                .send_feedback(TextComponent::text("unknown cluster test"), false);
            return Ok(0);
        }
        let server = context.server();
        let submitted = crate::server::cluster_test::submit(server, test_id);
        if !submitted {
            context.source.send_feedback(
                TextComponent::text("cluster test request queue full"),
                false,
            );
            return Ok(0);
        }
        context.source.send_feedback(
            TextComponent::text(format!("cluster test {test_id} submitted")),
            false,
        );
        Ok(1)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        "Prepares deterministic cluster test fixtures.",
        PermissionDefault::Op(PermissionLvl::Four),
    ));
    dispatcher.register(
        command("clustertest", "Prepares a cluster test fixture.")
            .requires(PERMISSION)
            .then(
                argument("test-id", StringArgumentType::SingleWord).executes(ClusterTestExecutor),
            ),
    );
}
