use crate::command::argument_builder::{ArgumentBuilder, argument, command, literal};
use crate::command::argument_types::core::integer::IntegerArgumentType;
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::argument_types::objective::ObjectiveArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::command_syntax_error::CommandSyntaxError;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::server::cluster_scoreboard::{ScoreAction, ScoreMutation, ScoreOperation};
use crate::world::scoreboard::{ScoreboardObjective, ScoreboardScore};
use pumpkin_command::deferred::CommandExecution;
use pumpkin_data::translation;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::RenderType;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

const DESCRIPTION: &str = "Manages scoreboard objectives and players.";
const PERMISSION: &str = "minecraft:command.scoreboard";

const ARG_OBJECTIVE: &str = "objective";
const ARG_CRITERION: &str = "criterion";
const ARG_DISPLAY_NAME: &str = "display_name";
const ARG_TARGETS: &str = "targets";

const DUPLICATE_OBJECTIVE_ERROR: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SCOREBOARD_OBJECTIVES_ADD_DUPLICATE,
    translation::java::COMMANDS_SCOREBOARD_OBJECTIVES_ADD_DUPLICATE,
);

const INVALID_ENABLE_ERROR: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_INVALID,
    translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_INVALID,
);

const FAILED_ENABLE_ERROR: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_FAILED,
    translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_FAILED,
);

struct ObjectivesAddExecutor {
    has_display_name: bool,
}

impl CommandExecutor for ObjectivesAddExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let objective_name = StringArgumentType::get(context, ARG_OBJECTIVE)?;
        let criterion = StringArgumentType::get(context, ARG_CRITERION)?;

        let display_name = if self.has_display_name {
            TextComponent::text(StringArgumentType::get(context, ARG_DISPLAY_NAME)?.to_string())
        } else {
            TextComponent::text(objective_name.to_string())
        };

        let world = context.world().clone();
        let objective_name_owned = objective_name.to_string();
        let criterion_owned = criterion.to_string();
        let display_name_clone = display_name;

        let mut scoreboard = world
            .scoreboard
            .edit()
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;

        if scoreboard
            .get_objectives()
            .contains_key(&objective_name_owned)
        {
            return Err(DUPLICATE_OBJECTIVE_ERROR.create_without_context());
        }

        let new_objective = ScoreboardObjective::new(
            &objective_name_owned,
            display_name_clone.clone(),
            RenderType::Integer,
            None,
            &criterion_owned,
        );

        scoreboard.add_objective(&world, new_objective);

        scoreboard
            .commit()
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;

        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_SCOREBOARD_OBJECTIVES_ADD_SUCCESS,
                translation::java::COMMANDS_SCOREBOARD_OBJECTIVES_ADD_SUCCESS,
                [display_name_clone],
            ),
            true,
        );

        Ok(1)
    }
}

struct PlayersEnableExecutor;

impl CommandExecutor for PlayersEnableExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let mut targets = EntityArgumentType::get_optional_entities(context, ARG_TARGETS)?
            .into_iter()
            .map(|entity| entity.get_scoreboard_name())
            .collect::<Vec<_>>();
        if targets.is_empty() {
            let requested = context
                .get_argument::<crate::command::argument_types::entity_selector::EntitySelector>(
                    ARG_TARGETS,
                )
                .ok()
                .and_then(|selector| selector.player_name.as_ref());
            if let Some(name) = requested {
                let directory = crate::server::cluster_presence::admin_directory_snapshot(
                    context.source.server(),
                );
                if let Some(name) = directory
                    .locate_by_name(name)
                    .and_then(|gid| directory.player_name(&gid).cloned())
                {
                    targets.push(name);
                }
            }
        }
        if targets.is_empty() {
            return Err(
                crate::command::argument_types::entity::NO_ENTITIES_ERROR_TYPE
                    .create_without_context(),
            );
        }
        let objective_name = ObjectiveArgumentType::get(context, ARG_OBJECTIVE)?;

        let world = context.world().clone();
        let objective_name_owned = objective_name.to_string();
        let targets_len = targets.len();
        let first_target_name = targets.first().cloned().unwrap_or_default();

        let mut scoreboard = world
            .scoreboard
            .edit()
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;

        let Some(objective) = scoreboard.get_objectives().get(&objective_name_owned) else {
            return Err(INVALID_ENABLE_ERROR.create_without_context());
        };

        if objective.criterion != "trigger" {
            return Err(INVALID_ENABLE_ERROR.create_without_context());
        }

        let objective_display_name = objective.display_name.clone();

        let mut enabled_count = 0;
        for player_name in &targets {
            let current_score = scoreboard
                .get_scores()
                .get(&objective_name_owned)
                .and_then(|m| m.get(player_name));

            let is_already_enabled = current_score.is_some_and(|s| !s.locked);

            if !is_already_enabled {
                let value = current_score.map_or(0, |s| s.value.0);
                let display_name = current_score.and_then(|s| s.display_name.clone());
                let number_format = current_score.and_then(|s| s.number_format.clone());

                let updated_score = ScoreboardScore {
                    entity_name: player_name.clone(),
                    objective_name: objective_name_owned.clone(),
                    value: VarInt(value),
                    display_name,
                    number_format,
                    locked: false,
                };

                scoreboard.update_score(&world, updated_score);
                enabled_count += 1;
            }
        }

        if enabled_count == 0 {
            return Err(FAILED_ENABLE_ERROR.create_without_context());
        }

        let msg = if targets_len == 1 {
            TextComponent::translate_cross(
                translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_SUCCESS_SINGLE,
                translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_SUCCESS_SINGLE,
                [
                    objective_display_name,
                    TextComponent::text(first_target_name),
                ],
            )
        } else {
            TextComponent::translate_cross(
                translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_SUCCESS_MULTIPLE,
                translation::java::COMMANDS_SCOREBOARD_PLAYERS_ENABLE_SUCCESS_MULTIPLE,
                [
                    objective_display_name,
                    TextComponent::text(targets_len.to_string()),
                ],
            )
        };

        scoreboard
            .commit()
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;

        context.source.send_feedback(msg, true);

        Ok(1)
    }
}

struct ObjectivesRemoveExecutor;

impl CommandExecutor for ObjectivesRemoveExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let objective_name = ObjectiveArgumentType::get(context, ARG_OBJECTIVE)?;

        let world = context.world().clone();
        let objective_name_owned = objective_name.to_string();

        let mut scoreboard = world
            .scoreboard
            .edit()
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;

        let Some(objective) = scoreboard.get_objectives().get(&objective_name_owned) else {
            return Err(INVALID_ENABLE_ERROR.create_without_context());
        };

        let display_name = objective.display_name.clone();

        scoreboard.remove_objective(&world, &objective_name_owned);

        scoreboard
            .commit()
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;

        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_SCOREBOARD_OBJECTIVES_REMOVE_SUCCESS,
                translation::bedrock::COMMANDS_SCOREBOARD_OBJECTIVES_REMOVE_SUCCESS,
                [display_name],
            ),
            true,
        );

        Ok(1)
    }
}

#[derive(Clone, Copy)]
struct ScoreHolderArgument;

impl crate::command::argument_types::argument_type::ArgumentType<crate::command::CommandSource>
    for ScoreHolderArgument
{
    type Item = String;
    fn parse(
        &self,
        reader: &mut crate::command::string_reader::StringReader,
    ) -> Result<String, pumpkin_command::errors::command_syntax_error::CommandSyntaxError> {
        let start = reader.cursor();
        if reader.peek() == Some('@') {
            EntityArgumentType::Entities.parse(reader)?;
        } else {
            reader.read_until_space();
        }
        let name = reader.string()[start..reader.cursor()].to_owned();
        if name.is_empty() || name.len() > 4096 {
            return Err(
                crate::command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
                    .create_without_context(TextComponent::text("Invalid scoreboard score holder")),
            );
        }
        Ok(name)
    }
    fn parse_with_source(
        &self,
        reader: &mut crate::command::string_reader::StringReader,
        source: &crate::command::CommandSource,
    ) -> Result<String, pumpkin_command::errors::command_syntax_error::CommandSyntaxError> {
        if reader.peek() != Some('@') {
            return self.parse(reader);
        }
        let start = reader.cursor();
        EntityArgumentType::Entities.parse_with_source(reader, source)?;
        Ok(reader.string()[start..reader.cursor()].to_owned())
    }
    fn client_side_parser(
        &self,
    ) -> crate::command::argument_types::argument_type::JavaClientArgumentType {
        crate::command::argument_types::argument_type::JavaClientArgumentType::ScoreHolder {
            flags: 1,
        }
    }
}

#[derive(Clone, Copy)]
struct ScoreOperationArgument;
impl crate::command::argument_types::argument_type::ArgumentType<crate::command::CommandSource>
    for ScoreOperationArgument
{
    type Item = String;
    fn parse(
        &self,
        reader: &mut crate::command::string_reader::StringReader,
    ) -> Result<String, pumpkin_command::errors::command_syntax_error::CommandSyntaxError> {
        let start = reader.cursor();
        reader.read_until_space();
        let token = &reader.string()[start..reader.cursor()];
        score_operation(token)?;
        Ok(token.to_owned())
    }
    fn client_side_parser(
        &self,
    ) -> crate::command::argument_types::argument_type::JavaClientArgumentType {
        crate::command::argument_types::argument_type::JavaClientArgumentType::Operation
    }
}

fn score_operation(
    token: &str,
) -> Result<ScoreOperation, pumpkin_command::errors::command_syntax_error::CommandSyntaxError> {
    match token {
        "=" => Ok(ScoreOperation::Assign),
        "+=" => Ok(ScoreOperation::Add),
        "-=" => Ok(ScoreOperation::Subtract),
        "*=" => Ok(ScoreOperation::Multiply),
        "/=" => Ok(ScoreOperation::Divide),
        "%=" => Ok(ScoreOperation::Modulo),
        "<" => Ok(ScoreOperation::Minimum),
        ">" => Ok(ScoreOperation::Maximum),
        "><" => Ok(ScoreOperation::Swap),
        _ => Err(
            crate::command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
                .create_without_context(TextComponent::text("Invalid scoreboard operation")),
        ),
    }
}

enum ScoreValueMode {
    Set,
    Add,
    Remove,
}

struct PlayersValueExecutor {
    mode: ScoreValueMode,
}

fn score_holders(
    context: &CommandContext,
    argument: &str,
) -> Result<Vec<String>, pumpkin_command::errors::command_syntax_error::CommandSyntaxError> {
    use crate::command::argument_types::argument_type::ArgumentType;
    let name = StringArgumentType::get(context, argument)?;
    if name == "*" {
        let board = context.world().scoreboard.snapshot();
        let names = board
            .get_scores()
            .values()
            .flat_map(|scores| scores.keys().cloned())
            .collect::<std::collections::BTreeSet<_>>();
        if names.is_empty() {
            return Err(
                crate::command::argument_types::entity::NO_ENTITIES_ERROR_TYPE
                    .create_without_context(),
            );
        }
        return Ok(names.into_iter().collect());
    }
    if name.starts_with('@') {
        let mut reader = crate::command::string_reader::StringReader::new(name);
        let selector =
            EntityArgumentType::Entities.parse_with_source(&mut reader, context.source.as_ref())?;
        let entities = selector.find_entities(context.source.as_ref())?;
        if entities.is_empty() {
            return Err(
                crate::command::argument_types::entity::NO_ENTITIES_ERROR_TYPE
                    .create_without_context(),
            );
        }
        return Ok(entities
            .into_iter()
            .map(|entity| entity.get_scoreboard_name())
            .collect());
    }
    Ok(vec![name.to_string()])
}

impl CommandExecutor for PlayersValueExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        self.execute_outcome(context).map(|_| 0)
    }
    fn execute_outcome(
        &self,
        context: &CommandContext,
    ) -> Result<CommandExecution, CommandSyntaxError> {
        let targets = score_holders(context, ARG_TARGETS)?;
        let objective = ObjectiveArgumentType::get(context, ARG_OBJECTIVE)?.to_string();
        let value = IntegerArgumentType::get(context, "score")?;
        let action = match self.mode {
            ScoreValueMode::Set => ScoreAction::Set(value),
            ScoreValueMode::Add => ScoreAction::Delta(value),
            ScoreValueMode::Remove => ScoreAction::Delta(value.wrapping_neg()),
        };
        let token = context
            .world()
            .scoreboard
            .submit_scores(
                ScoreMutation {
                    targets,
                    objective,
                    action,
                },
                Some(context.source.clone()),
            )
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;
        Ok(CommandExecution::Deferred(token))
    }
}

struct PlayersOperationExecutor;
impl CommandExecutor for PlayersOperationExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        self.execute_outcome(context).map(|_| 0)
    }
    fn execute_outcome(
        &self,
        context: &CommandContext,
    ) -> Result<CommandExecution, CommandSyntaxError> {
        let targets = score_holders(context, ARG_TARGETS)?;
        let objective = ObjectiveArgumentType::get(context, ARG_OBJECTIVE)?.to_string();
        let sources = score_holders(context, "source")?;
        let source_objective = ObjectiveArgumentType::get(context, "sourceObjective")?.to_string();
        let operation = score_operation(StringArgumentType::get(context, "operation")?)?;
        let token = context
            .world()
            .scoreboard
            .submit_scores(
                ScoreMutation {
                    targets,
                    objective,
                    action: ScoreAction::Operation {
                        operation,
                        sources,
                        objective: source_objective,
                    },
                },
                Some(context.source.clone()),
            )
            .map_err(crate::world::scoreboard::ScoreboardBusy::command_error)?;
        Ok(CommandExecution::Deferred(token))
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    dispatcher.register(
        command("scoreboard", DESCRIPTION)
            .requires(PERMISSION)
            .then(
                literal("objectives")
                    .then(
                        literal("add").then(
                            argument(ARG_OBJECTIVE, StringArgumentType::SingleWord).then(
                                argument(ARG_CRITERION, StringArgumentType::SingleWord)
                                    .executes(ObjectivesAddExecutor {
                                        has_display_name: false,
                                    })
                                    .then(
                                        argument(
                                            ARG_DISPLAY_NAME,
                                            StringArgumentType::GreedyPhrase,
                                        )
                                        .executes(
                                            ObjectivesAddExecutor {
                                                has_display_name: true,
                                            },
                                        ),
                                    ),
                            ),
                        ),
                    )
                    .then(
                        literal("remove").then(
                            argument(ARG_OBJECTIVE, ObjectiveArgumentType)
                                .executes(ObjectivesRemoveExecutor),
                        ),
                    ),
            )
            .then(
                literal("players")
                    .then(
                        literal("enable").then(
                            argument(ARG_TARGETS, EntityArgumentType::Players).then(
                                argument(ARG_OBJECTIVE, ObjectiveArgumentType)
                                    .executes(PlayersEnableExecutor),
                            ),
                        ),
                    )
                    .then(
                        literal("set").then(argument(ARG_TARGETS, ScoreHolderArgument).then(
                            argument(ARG_OBJECTIVE, ObjectiveArgumentType).then(
                                argument("score", IntegerArgumentType::any()).executes(
                                    PlayersValueExecutor {
                                        mode: ScoreValueMode::Set,
                                    },
                                ),
                            ),
                        )),
                    )
                    .then(
                        literal("add").then(argument(ARG_TARGETS, ScoreHolderArgument).then(
                            argument(ARG_OBJECTIVE, ObjectiveArgumentType).then(
                                argument("score", IntegerArgumentType::with_min(0)).executes(
                                    PlayersValueExecutor {
                                        mode: ScoreValueMode::Add,
                                    },
                                ),
                            ),
                        )),
                    )
                    .then(
                        literal("remove").then(argument(ARG_TARGETS, ScoreHolderArgument).then(
                            argument(ARG_OBJECTIVE, ObjectiveArgumentType).then(
                                argument("score", IntegerArgumentType::with_min(0)).executes(
                                    PlayersValueExecutor {
                                        mode: ScoreValueMode::Remove,
                                    },
                                ),
                            ),
                        )),
                    )
                    .then(
                        literal("operation").then(
                            argument(ARG_TARGETS, ScoreHolderArgument).then(
                                argument(ARG_OBJECTIVE, ObjectiveArgumentType).then(
                                    argument("operation", ScoreOperationArgument).then(
                                        argument("source", ScoreHolderArgument).then(
                                            argument("sourceObjective", ObjectiveArgumentType)
                                                .executes(PlayersOperationExecutor),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
            ),
    );
}

#[cfg(test)]
mod semantic_scoreboard_argument_tests {
    use super::*;
    use crate::command::argument_types::argument_type::ArgumentType;
    use crate::command::string_reader::StringReader;

    #[test]
    fn scoreboard_symbols_and_non_player_score_holders_consume_exact_tokens() {
        for token in ["=", "+=", "-=", "*=", "/=", "%=", "<", ">", "><"] {
            let input = format!("{token} Bob");
            let mut reader = StringReader::new(input);
            assert!(ScoreOperationArgument.parse(&mut reader).is_ok());
            assert_eq!(reader.remaining_part(), " Bob");
        }
        for token in ["*", "#non_player_score_holder_with_a_long_name", "Alice"] {
            let input = format!("{token} kills");
            let mut reader = StringReader::new(input);
            assert_eq!(ScoreHolderArgument.parse(&mut reader).unwrap(), token);
            assert_eq!(reader.remaining_part(), " kills");
        }
        assert!(
            ScoreOperationArgument
                .parse(&mut StringReader::new("+=bad"))
                .is_err()
        );
        let mut reader = StringReader::new("@a[name=Alice] kills");
        assert_eq!(
            ScoreHolderArgument.parse(&mut reader).unwrap(),
            "@a[name=Alice]"
        );
        assert_eq!(reader.remaining_part(), " kills");
    }
}
