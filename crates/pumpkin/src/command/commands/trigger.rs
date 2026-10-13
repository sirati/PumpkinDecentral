use crate::command::argument_builder::{ArgumentBuilder, argument, command, literal};
use crate::command::argument_types::core::integer::IntegerArgumentType;
use crate::command::argument_types::objective::ObjectiveArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::command_syntax_error::CommandSyntaxError;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use pumpkin_command::deferred::CommandExecution;
use pumpkin_data::translation;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};

const DESCRIPTION: &str = "Modifies a trigger scoreboard objective.";
const PERMISSION: &str = "minecraft:command.trigger";

const ARG_OBJECTIVE: &str = "objective";
const ARG_VALUE: &str = "value";

const INVALID_TRIGGER_ERROR: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_TRIGGER_FAILED_INVALID,
    translation::java::COMMANDS_TRIGGER_FAILED_INVALID,
);

const UNPRIMED_TRIGGER_ERROR: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_TRIGGER_FAILED_UNPRIMED,
    translation::java::COMMANDS_TRIGGER_FAILED_UNPRIMED,
);

struct SimpleTriggerExecutor;
struct AddTriggerExecutor;
struct SetTriggerExecutor;

fn submit_trigger(
    context: &CommandContext,
    value: i32,
    add: bool,
) -> Result<CommandExecution, CommandSyntaxError> {
    let player = context.source.player_or_err()?;
    let objective = ObjectiveArgumentType::get(context, ARG_OBJECTIVE)?.to_string();
    let board = context.world().scoreboard.snapshot();
    if board
        .get_objective(&objective)
        .is_none_or(|objective| objective.criterion != "trigger")
    {
        return Err(INVALID_TRIGGER_ERROR.create_without_context());
    }
    if board
        .get_score(&player.gameprofile.name, &objective)
        .is_none_or(|score| score.locked)
    {
        return Err(UNPRIMED_TRIGGER_ERROR.create_without_context());
    }
    let token = context
        .world()
        .scoreboard
        .submit_scores(
            crate::server::cluster_scoreboard::ScoreMutation {
                targets: vec![player.gameprofile.name.clone()],
                objective,
                action: crate::server::cluster_scoreboard::ScoreAction::Trigger { value, add },
            },
            Some(context.source.clone()),
        )
        .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;
    Ok(CommandExecution::Deferred(token))
}
impl CommandExecutor for SimpleTriggerExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        self.execute_outcome(context).map(|_| 0)
    }
    fn execute_outcome(
        &self,
        context: &CommandContext,
    ) -> Result<CommandExecution, CommandSyntaxError> {
        submit_trigger(context, 1, true)
    }
}
impl CommandExecutor for AddTriggerExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        self.execute_outcome(context).map(|_| 0)
    }
    fn execute_outcome(
        &self,
        context: &CommandContext,
    ) -> Result<CommandExecution, CommandSyntaxError> {
        submit_trigger(context, IntegerArgumentType::get(context, ARG_VALUE)?, true)
    }
}
impl CommandExecutor for SetTriggerExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        self.execute_outcome(context).map(|_| 0)
    }
    fn execute_outcome(
        &self,
        context: &CommandContext,
    ) -> Result<CommandExecution, CommandSyntaxError> {
        submit_trigger(
            context,
            IntegerArgumentType::get(context, ARG_VALUE)?,
            false,
        )
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Allow,
    ));

    dispatcher.register(
        command("trigger", DESCRIPTION).requires(PERMISSION).then(
            argument(ARG_OBJECTIVE, ObjectiveArgumentType)
                .executes(SimpleTriggerExecutor)
                .then(literal("add").then(
                    argument(ARG_VALUE, IntegerArgumentType::any()).executes(AddTriggerExecutor),
                ))
                .then(literal("set").then(
                    argument(ARG_VALUE, IntegerArgumentType::any()).executes(SetTriggerExecutor),
                )),
        ),
    );
}
