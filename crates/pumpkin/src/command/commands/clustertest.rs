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
        if let Some(mode) = crate::server::cluster_test_health_policy::parse(test_id) {
            if crate::server::cluster_test_health_policy::submit(context.server(), &context.source, mode) {
                return Ok(1);
            }
            context.source.send_feedback(TextComponent::text("cluster test health policy arm unavailable"), false);
            return Ok(0);
        }

        if let Some(scenario) =
            crate::server::cluster_test_preflight::PreflightScenario::parse(test_id)
        {
            let submitted = crate::server::cluster_test_preflight::submit_arm(
                context.server(),
                &context.source,
                scenario,
            );
            let state = if submitted {
                "submitted"
            } else {
                "submission failed"
            };
            context.source.send_feedback(
                TextComponent::text(format!("cluster test {test_id} {state}")),
                false,
            );
            return Ok(if submitted { 1 } else { 0 });
        }
        if matches!(
            test_id,
            "packet-preflight-lobby-depart" | "handoff-callback-release"
        ) && !crate::server::cluster_test_preflight::authorize_operator(
            context.server(),
            &context.source,
        ) {
            context.source.send_feedback(
                TextComponent::text(
                    "cluster test departure requires the registered independent operator",
                ),
                false,
            );
            return Ok(0);
        }
        if let Some(command) =
            crate::server::cluster_test_scenarios::ActionScenarioCommand::parse(test_id)
        {
            let result = crate::server::cluster_test_scenarios::submit_action_scenario(
                context.server(),
                &context.source,
                command,
            );
            let success = result.is_ok();
            let feedback = match result {
                Ok(()) => format!("cluster test {test_id} submitted"),
                Err(reason) => format!("cluster test {test_id} submission failed: {reason:?}"),
            };
            context
                .source
                .send_feedback(TextComponent::text(feedback), false);
            return Ok(if success { 1 } else { 0 });
        }
        if let Some(command) =
            crate::server::cluster_test_scenarios::InventoryScenarioCommand::parse(test_id)
        {
            let result = crate::server::cluster_test_scenarios::submit_inventory(
                context.server(),
                &context.source,
                command,
            );
            let success = result.is_ok();
            let feedback = match result {
                Ok(()) => format!("cluster test {test_id} submitted"),
                Err(reason) => format!("cluster test {test_id} submission failed: {reason:?}"),
            };
            context
                .source
                .send_feedback(TextComponent::text(feedback), false);
            return Ok(if success { 1 } else { 0 });
        }
        if !matches!(
            test_id,
            "arena"
                | "controller-divergence"
                | "beacon"
                | "departure-admin-lobby"
                | "departure-admin-disconnect"
                | "item-place"
                | "item-stale"
                | "handoff-revision-mismatch"
                | "packet-preflight-lobby-depart"
                | "handoff-callback-release"
                | "door-trapdoor"
                | "button-lever"
                | "end-frame-anchor"
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
