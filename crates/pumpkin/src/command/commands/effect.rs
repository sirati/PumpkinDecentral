use std::cell::RefCell;

use pumpkin_cluster::combat::chunk_of_pos;
use pumpkin_cluster::entity_action::{
    EntityMutationSeqClock, capture_add_effect, capture_remove_effect, stage,
};
use pumpkin_cluster::identity::{ActionActor, ServerId};
use pumpkin_cluster::protocol::{EntityMutationTarget, StatusEffectState};
use pumpkin_data::potion::Effect;
use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command, literal};
use crate::command::argument_types::core::bool::BoolArgumentType;
use crate::command::argument_types::core::integer::IntegerArgumentType;
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::argument_types::resource::{MOB_EFFECT_ARGUMENT, ResourceArgument};
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::entity::EntityBase;

thread_local! {
    static ENTITY_MUTATION_SEQUENCES: RefCell<EntityMutationSeqClock> = RefCell::new(EntityMutationSeqClock::new());
}

const DESCRIPTION: &str = "Adds or removes the status effects of players and other entities.";
const PERMISSION: &str = "minecraft:command.effect";

const ERROR_NOT_PLAYER: CommandErrorType<0> = CommandErrorType::new(
    translation::java::PERMISSIONS_REQUIRES_PLAYER,
    translation::java::PERMISSIONS_REQUIRES_PLAYER,
);

const ERROR_GIVE_FAILED: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_EFFECT_GIVE_FAILED,
    translation::java::COMMANDS_EFFECT_GIVE_FAILED,
);

const ERROR_CLEAR_EVERYTHING_FAILED: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_EFFECT_CLEAR_EVERYTHING_FAILED,
    translation::java::COMMANDS_EFFECT_CLEAR_EVERYTHING_FAILED,
);

const ERROR_CLEAR_SPECIFIC_FAILED: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_EFFECT_CLEAR_SPECIFIC_FAILED,
    translation::java::COMMANDS_EFFECT_CLEAR_SPECIFIC_FAILED,
);

#[derive(Clone, Copy)]
enum Duration {
    Default,
    Specified,
    Infinite,
}

struct GiveExecutor {
    duration: Duration,
    has_amplifier: bool,
    has_hide_particles: bool,
}

fn effect_state(effect: Effect) -> StatusEffectState {
    let mut flags = 0_u8;
    if effect.ambient {
        flags |= 1;
    }
    if effect.show_particles {
        flags |= 2;
    }
    if effect.show_icon {
        flags |= 4;
    }
    if effect.blend {
        flags |= 8;
    }
    StatusEffectState {
        effect: u16::from(effect.effect_type.id),
        duration_ticks: effect.duration,
        amplifier: effect.amplifier,
        flags,
    }
}

fn mutation_actor(context: &CommandContext) -> ActionActor {
    context
        .source
        .as_player()
        .and_then(|player| player.cluster_gid())
        .map(ActionActor::Player)
        .unwrap_or_else(|| {
            ActionActor::Server(ServerId(
                context.source.server().advanced_config.cluster.server_id,
            ))
        })
}

fn mutation_target(target: &crate::entity::player::Player) -> Option<EntityMutationTarget> {
    let gid = target.cluster_gid()?;
    let position = target.position();
    Some(EntityMutationTarget::Player {
        gid,
        chunk: chunk_of_pos(position.x, position.z),
    })
}

fn issue_mutation_sequence(actor: ActionActor) -> Option<pumpkin_cluster::identity::ActionSeq> {
    ENTITY_MUTATION_SEQUENCES
        .try_with(|sequences| sequences.try_borrow_mut().ok().map(|mut sequences| sequences.issue(actor)))
        .ok()
        .flatten()
}

fn stage_add_effect(
    context: &CommandContext,
    target: &crate::entity::player::Player,
    effect: Effect,
) -> bool {
    let server = context.source.server();
    if !server.advanced_config.cluster.enabled {
        target.add_effect(effect);
        return true;
    }
    let Some(tick) = crate::server::cluster::disciplined_tick_now() else {
        return false;
    };
    let Some(target_ref) = mutation_target(target) else {
        return false;
    };
    let actor = mutation_actor(context);
    let Some(seq) = issue_mutation_sequence(actor) else {
        return false;
    };
    let update = capture_add_effect(
        actor,
        seq,
        tick,
        target_ref,
        target.living_entity.get_effect(effect.effect_type).map(effect_state),
        effect_state(effect),
    );
    if !crate::server::cluster_entity_apply::stage_owned_entity_mutation(update) {
        return false;
    }
    stage(update);
    true
}

fn stage_remove_effect(
    context: &CommandContext,
    target: &crate::entity::player::Player,
    effect: &'static pumpkin_data::effect::StatusEffect,
) -> bool {
    let server = context.source.server();
    if !server.advanced_config.cluster.enabled {
        return target.remove_effect(effect);
    }
    let Some(before) = target.living_entity.get_effect(effect).map(effect_state) else {
        return false;
    };
    let Some(tick) = crate::server::cluster::disciplined_tick_now() else {
        return false;
    };
    let Some(target_ref) = mutation_target(target) else {
        return false;
    };
    let actor = mutation_actor(context);
    let Some(seq) = issue_mutation_sequence(actor) else {
        return false;
    };
    let update = capture_remove_effect(actor, seq, tick, target_ref, before);
    if !crate::server::cluster_entity_apply::stage_owned_entity_mutation(update) {
        return false;
    }
    stage(update);
    true
}

impl CommandExecutor for GiveExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = EntityArgumentType::get_players(context, "targets")?;
        let effect = ResourceArgument::get_mob_effect(context, "effect")?;

        let duration_ticks = match self.duration {
            Duration::Default => 30 * 20,
            Duration::Specified => IntegerArgumentType::get(context, "seconds")? * 20,
            Duration::Infinite => -1,
        };

        let amplifier = if self.has_amplifier {
            IntegerArgumentType::get(context, "amplifier")? as u8
        } else {
            0
        };

        let hide_particles = if self.has_hide_particles {
            BoolArgumentType::get(context, "hideParticles")?
        } else {
            false
        };

        let mut successes = 0;

        for target in &targets {
            let should_skip = target
                .living_entity
                .get_effect(effect)
                .is_some_and(|existing| existing.amplifier >= amplifier);

            if !should_skip {
                if stage_add_effect(context, target, Effect {
                    effect_type: effect,
                    duration: duration_ticks,
                    amplifier,
                    ambient: false,
                    show_particles: !hide_particles,
                    show_icon: true,
                    blend: false,
                }) {
                    successes += 1;
                }
            }
        }

        let translation_name = TextComponent::translate_cross(
            effect.translation_key.to_string(),
            effect.translation_key.to_string(),
            [],
        );

        if successes == 0 {
            return Err(ERROR_GIVE_FAILED.create_without_context());
        }

        if targets.len() == 1 {
            context.source.send_feedback(
                TextComponent::translate_cross(
                    translation::java::COMMANDS_EFFECT_GIVE_SUCCESS_SINGLE,
                    translation::java::COMMANDS_EFFECT_GIVE_SUCCESS_SINGLE,
                    [translation_name, targets[0].as_ref().get_display_name()],
                ),
                true,
            );
        } else {
            context.source.send_feedback(
                TextComponent::translate_cross(
                    translation::java::COMMANDS_EFFECT_GIVE_SUCCESS_MULTIPLE,
                    translation::java::COMMANDS_EFFECT_GIVE_SUCCESS_MULTIPLE,
                    [
                        translation_name,
                        TextComponent::text(targets.len().to_string()),
                    ],
                ),
                true,
            );
        }

        Ok(successes)
    }
}

enum ClearMode {
    SelfAll,
    TargetsAll,
    TargetsSpecific,
}

struct ClearExecutor(ClearMode);

impl CommandExecutor for ClearExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = match self.0 {
            ClearMode::SelfAll => {
                let player = context
                    .source
                    .output
                    .as_player()
                    .ok_or_else(|| ERROR_NOT_PLAYER.create_without_context())?;
                vec![player]
            }
            ClearMode::TargetsAll | ClearMode::TargetsSpecific => {
                EntityArgumentType::get_players(context, "targets")?
            }
        };

        match self.0 {
            ClearMode::SelfAll | ClearMode::TargetsAll => {
                let mut succeeded_clears = 0;
                for target in &targets {
                    if !context.source.server().advanced_config.cluster.enabled {
                        if !target.living_entity.active_effects.is_empty() {
                            target.remove_all_effects();
                            succeeded_clears += 1;
                        }
                        continue;
                    }
                    let mut cleared = false;
                    for effect in target.living_entity.active_effects.types() {
                        if stage_remove_effect(context, target, effect) {
                            cleared = true;
                        }
                    }
                    if cleared {
                        succeeded_clears += 1;
                    }
                }

                if succeeded_clears == 0 {
                    return Err(ERROR_CLEAR_EVERYTHING_FAILED.create_without_context());
                }

                if targets.len() == 1 {
                    context.source.send_feedback(
                        TextComponent::translate_cross(
                            translation::java::COMMANDS_EFFECT_CLEAR_EVERYTHING_SUCCESS_SINGLE,
                            translation::java::COMMANDS_EFFECT_CLEAR_EVERYTHING_SUCCESS_SINGLE,
                            [targets[0].as_ref().get_display_name()],
                        ),
                        true,
                    );
                } else {
                    context.source.send_feedback(
                        TextComponent::translate_cross(
                            translation::java::COMMANDS_EFFECT_CLEAR_EVERYTHING_SUCCESS_MULTIPLE,
                            translation::java::COMMANDS_EFFECT_CLEAR_EVERYTHING_SUCCESS_MULTIPLE,
                            [TextComponent::text(targets.len().to_string())],
                        ),
                        true,
                    );
                }
                Ok(succeeded_clears)
            }
            ClearMode::TargetsSpecific => {
                let effect = ResourceArgument::get_mob_effect(context, "effect")?;
                let mut succeeded_clears = 0;
                for target in &targets {
                    if target.living_entity.has_effect(effect)
                        && stage_remove_effect(context, target, effect)
                    {
                        succeeded_clears += 1;
                    }
                }

                if succeeded_clears == 0 {
                    return Err(ERROR_CLEAR_SPECIFIC_FAILED.create_without_context());
                }

                if targets.len() == 1 {
                    context.source.send_feedback(
                        TextComponent::translate_cross(
                            translation::java::COMMANDS_EFFECT_CLEAR_SPECIFIC_SUCCESS_SINGLE,
                            translation::java::COMMANDS_EFFECT_CLEAR_SPECIFIC_SUCCESS_SINGLE,
                            [
                                TextComponent::translate_cross(
                                    effect.translation_key,
                                    effect.translation_key,
                                    [],
                                ),
                                targets[0].as_ref().get_display_name(),
                            ],
                        ),
                        true,
                    );
                } else {
                    context.source.send_feedback(
                        TextComponent::translate_cross(
                            translation::java::COMMANDS_EFFECT_CLEAR_SPECIFIC_SUCCESS_MULTIPLE,
                            translation::java::COMMANDS_EFFECT_CLEAR_SPECIFIC_SUCCESS_MULTIPLE,
                            [
                                TextComponent::translate_cross(
                                    effect.translation_key,
                                    effect.translation_key,
                                    [],
                                ),
                                TextComponent::text(targets.len().to_string()),
                            ],
                        ),
                        true,
                    );
                }
                Ok(succeeded_clears)
            }
        }
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    let seconds_node = argument("seconds", IntegerArgumentType::new(0, 1_000_000))
        .executes(GiveExecutor {
            duration: Duration::Specified,
            has_amplifier: false,
            has_hide_particles: false,
        })
        .then(
            argument("amplifier", IntegerArgumentType::new(0, 255))
                .executes(GiveExecutor {
                    duration: Duration::Specified,
                    has_amplifier: true,
                    has_hide_particles: false,
                })
                .then(
                    argument("hideParticles", BoolArgumentType).executes(GiveExecutor {
                        duration: Duration::Specified,
                        has_amplifier: true,
                        has_hide_particles: true,
                    }),
                ),
        );

    let infinite_node = literal("infinite")
        .executes(GiveExecutor {
            duration: Duration::Infinite,
            has_amplifier: false,
            has_hide_particles: false,
        })
        .then(
            argument("amplifier", IntegerArgumentType::new(0, 255))
                .executes(GiveExecutor {
                    duration: Duration::Infinite,
                    has_amplifier: true,
                    has_hide_particles: false,
                })
                .then(
                    argument("hideParticles", BoolArgumentType).executes(GiveExecutor {
                        duration: Duration::Infinite,
                        has_amplifier: true,
                        has_hide_particles: true,
                    }),
                ),
        );

    let give_node = literal("give").then(
        argument("targets", EntityArgumentType::Players).then(
            argument("effect", MOB_EFFECT_ARGUMENT.clone())
                .executes(GiveExecutor {
                    duration: Duration::Default,
                    has_amplifier: false,
                    has_hide_particles: false,
                })
                .then(seconds_node)
                .then(infinite_node),
        ),
    );

    let clear_node = literal("clear")
        .executes(ClearExecutor(ClearMode::SelfAll))
        .then(
            argument("targets", EntityArgumentType::Players)
                .executes(ClearExecutor(ClearMode::TargetsAll))
                .then(
                    argument("effect", MOB_EFFECT_ARGUMENT.clone())
                        .executes(ClearExecutor(ClearMode::TargetsSpecific)),
                ),
        );

    dispatcher.register(
        command("effect", DESCRIPTION)
            .requires(PERMISSION)
            .then(clear_node)
            .then(give_node),
    );
}
