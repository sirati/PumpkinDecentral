use crate::item_contact::ContactIdentity;
use crate::mob_continuation::{
    CrossbowStage, MobBrain, MobFollowMode, MobGoalProgress, MobGoalSelector, MobRangedWeapon,
    MobReproduction,
};
use crate::protocol::BlockPos;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MobIntent {
    EatGrass {
        position: BlockPos,
        expected: u16,
        replacement: u16,
        modify_world: bool,
    },
    Attack(ContactIdentity),
    Breed(ContactIdentity),
    LookAt([u64; 3]),
    Jump,
    StartGrazing,
    BeginUse,
    EndUse,
    Shoot {
        target: ContactIdentity,
        power: u32,
    },
    Door {
        position: BlockPos,
        open: bool,
        breaking: bool,
    },
}

pub trait MobGoalEnvironment {
    fn position(&self) -> [f64; 3];
    fn eye_position(&self) -> Option<[f64; 3]>;
    fn target_eye_position(&self, target: &ContactIdentity) -> Option<[f64; 3]>;
    fn block(&self, position: BlockPos) -> Option<u16>;
    fn edible_for_sheep(&self, state: u16) -> bool;
    fn grass_block(&self, state: u16) -> bool;
    fn air_state(&self) -> u16;
    fn dirt_state(&self) -> u16;
    fn mob_griefing(&self) -> bool;
    fn swimming(&self) -> bool;
    fn target_position(&self, target: &ContactIdentity) -> Option<[f64; 3]>;
    fn nearest(&self, kind: Option<u16>, range: f64) -> Option<ContactIdentity>;
    fn attackable(&self, target: &ContactIdentity) -> bool { self.target_position(target).is_some() }
    fn nearest_attackable(&self, kind: Option<u16>, range: f64) -> Option<ContactIdentity> {
        self.nearest(kind, range).filter(|target| self.attackable(target))
    }
    fn visible(&self, target: &ContactIdentity) -> bool;
    fn breeding_partner(&self) -> Option<ContactIdentity>;
    fn follow_target(&self, mode: MobFollowMode, range: f64) -> Option<ContactIdentity>;
    fn tempted_by(&self, target: &ContactIdentity, items: &[u16]) -> bool;
    fn nearest_tempting(&self, items: &[u16], range: f64) -> Option<ContactIdentity>;
    fn in_attack_range(&self, target: &ContactIdentity) -> bool;
    fn follow_range(&self) -> f64;
    fn in_danger(&self) -> bool;
    fn escape_destination(&self, random: &mut MobBrain) -> Option<[f64; 3]>;
    fn wander_destination(&self, random: &mut MobBrain) -> Option<[f64; 3]>;
}

pub fn tick_goals(
    brain: &mut MobBrain,
    reproduction: &MobReproduction,
    environment: &impl MobGoalEnvironment,
) -> Vec<MobIntent> {
    brain.age = brain.age.wrapping_add(1);
    let full = brain.age <= 1 || (brain.age.wrapping_add(u32::from(brain.selector_phase)) & 1) == 0;
    let mut output = tick_selector(
        brain,
        reproduction,
        environment,
        MobGoalSelector::Targets,
        full,
    );
    output.extend(tick_selector(
        brain,
        reproduction,
        environment,
        MobGoalSelector::Behavior,
        full,
    ));
    output
}

fn tick_selector(
    brain: &mut MobBrain,
    reproduction: &MobReproduction,
    environment: &impl MobGoalEnvironment,
    selector: MobGoalSelector,
    full: bool,
) -> Vec<MobIntent> {
    let mut output = Vec::new();
    if !full {
        for index in 0..brain.goals.len() {
            let mut goal = brain.goals[index].clone();
            if goal.selector == selector && goal.running && goal.every_tick {
                advance(&mut goal.progress, brain, environment, &mut output);
            }
            brain.goals[index] = goal;
        }
        return output;
    }
    for index in 0..brain.goals.len() {
        let mut goal = brain.goals[index].clone();
        if goal.selector != selector {
            continue;
        }
        if goal.running
            && (goal.controls & brain.disabled_controls != 0
                || !evaluate(&mut goal.progress, true, brain, reproduction, environment))
        {
            stop_goal(&mut goal.progress, brain, &mut output);
            goal.running = false;
        }
        brain.goals[index] = goal;
    }
    for index in 0..brain.goals.len() {
        let mut goal = brain.goals[index].clone();
        if goal.selector != selector {
            continue;
        }
        if goal.running || goal.controls & brain.disabled_controls != 0 {
            continue;
        }
        if brain.goals.iter().any(|other| {
            other.selector == selector
                && other.running
                && other.controls & goal.controls != 0
                && (!other.stoppable || other.priority <= goal.priority)
        }) {
            continue;
        }
        if !evaluate(&mut goal.progress, false, brain, reproduction, environment) {
            continue;
        }
        for other_index in 0..brain.goals.len() {
            if brain.goals[other_index].selector == selector
                && brain.goals[other_index].running
                && brain.goals[other_index].controls & goal.controls != 0
            {
                let mut other = brain.goals[other_index].clone();
                stop_goal(&mut other.progress, brain, &mut output);
                other.running = false;
                brain.goals[other_index] = other;
            }
        }
        goal.running = true;
        if matches!(goal.progress, MobGoalProgress::Graze { .. }) {
            stop_navigation(brain);
            output.push(MobIntent::StartGrazing);
        }
        brain.goals[index] = goal;
    }
    for index in 0..brain.goals.len() {
        let mut goal = brain.goals[index].clone();
        if goal.selector == selector && goal.running {
            advance(&mut goal.progress, brain, environment, &mut output);
        }
        brain.goals[index] = goal;
    }
    output
}

fn stop_goal(goal: &mut MobGoalProgress, brain: &mut MobBrain, output: &mut Vec<MobIntent>) {
    match goal {
        MobGoalProgress::EscapeDanger { panicking, .. } => {
            *panicking = false;
            stop_navigation(brain);
        }
        MobGoalProgress::Graze { remaining } => *remaining = 0,
        MobGoalProgress::Tempt {
            cooldown, target, ..
        } => {
            *cooldown = 50;
            *target = None;
            stop_navigation(brain);
        }
        MobGoalProgress::Ranged {
            drawing,
            draw_ticks,
            ..
        } => {
            if *drawing {
                output.push(MobIntent::EndUse);
            }
            *drawing = false;
            *draw_ticks = 0;
        }
        MobGoalProgress::Crossbow {
            stage,
            charge_ticks,
            ..
        } => {
            if *stage == CrossbowStage::Charging {
                output.push(MobIntent::EndUse);
            }
            *stage = CrossbowStage::Uncharged;
            *charge_ticks = 0;
        }
        MobGoalProgress::Target { target, .. } => {
            if brain.target == *target {
                brain.target = None;
            }
            *target = None;
        }
        MobGoalProgress::Melee { .. }
        | MobGoalProgress::Wander { .. }
        | MobGoalProgress::Breed { .. }
        | MobGoalProgress::Follow { .. } => stop_navigation(brain),
        _ => {}
    }
}

fn evaluate(
    goal: &mut MobGoalProgress,
    running: bool,
    brain: &mut MobBrain,
    reproduction: &MobReproduction,
    environment: &impl MobGoalEnvironment,
) -> bool {
    match goal {
        MobGoalProgress::Swim => environment.swimming(),
        MobGoalProgress::EscapeDanger {
            destination,
            panicking,
            ..
        } => {
            if running {
                return usize::from(brain.navigation.cursor) < brain.navigation.path.len();
            }
            if !environment.in_danger() {
                return false;
            }
            *destination = environment
                .escape_destination(brain)
                .map(|position| position.map(f64::to_bits));
            *panicking = destination.is_some();
            *panicking
        }
        MobGoalProgress::Graze { remaining } => {
            if running {
                return *remaining > 0;
            }
            let interval = if reproduction.growth_age < 0 {
                50
            } else {
                1000
            };
            if brain.random_bounded(interval) != Some(0) {
                return false;
            }
            let [x, y, z] = environment.position();
            let position = BlockPos {
                x: x.floor() as i32,
                y: y.floor() as i32,
                z: z.floor() as i32,
            };
            let edible = environment
                .block(position)
                .is_some_and(|state| environment.edible_for_sheep(state));
            let below = BlockPos {
                y: position.y.saturating_sub(1),
                ..position
            };
            if edible
                || environment
                    .block(below)
                    .is_some_and(|state| environment.grass_block(state))
            {
                *remaining = 40;
                return true;
            }
            false
        }
        MobGoalProgress::LookAround {
            remaining,
            direction,
        } => {
            if running {
                return *remaining > 0;
            }
            if brain.random_bounded(50) != Some(0) {
                return false;
            }
            *remaining = 20 + brain.random_bounded(20).unwrap_or(0);
            let angle = brain.next_random() as f64 / u64::MAX as f64 * std::f64::consts::TAU;
            *direction = [angle.cos().to_bits(), angle.sin().to_bits()];
            true
        }
        MobGoalProgress::LookAt {
            target,
            remaining,
            range,
            chance,
            kind,
            ..
        } => {
            if running {
                return *remaining > 0
                    && target
                        .as_ref()
                        .and_then(|target| environment.target_position(target))
                        .is_some_and(|position| {
                            distance_squared(environment.position(), position)
                                <= f64::from(f32::from_bits(*range)).powi(2)
                        });
            }
            if brain.next_random() as f64 / u64::MAX as f64 >= f64::from(f32::from_bits(*chance)) {
                return false;
            }
            *target = environment.nearest(Some(*kind), f64::from(f32::from_bits(*range)));
            *remaining = (40 + brain.random_bounded(40).unwrap_or(0)).div_ceil(2);
            target.is_some()
        }
        MobGoalProgress::Breed {
            mate, remaining, ..
        } => {
            if reproduction.growth_age < 0
                || reproduction.love_ticks == 0
                || reproduction.breeding_cooldown != 0
            {
                return false;
            }
            if !running {
                *mate = environment.breeding_partner();
                *remaining = 0;
            }
            *remaining < 60
                && mate
                    .as_ref()
                    .and_then(|mate| environment.target_position(mate))
                    .is_some()
        }
        MobGoalProgress::Melee { target, .. } => {
            *target = brain.target.clone();
            target
                .as_ref()
                .and_then(|target| environment.target_position(target))
                .is_some()
        }
        MobGoalProgress::Tempt {
            target,
            cooldown,
            items,
            ..
        } => {
            if *cooldown > 0 {
                *cooldown -= 1;
                return false;
            }
            if !running {
                *target = environment.nearest_tempting(items, 10.0);
            }
            target.as_ref().is_some_and(|target| {
                environment.tempted_by(target, items)
                    && environment.target_position(target).is_some()
            })
        }
        MobGoalProgress::Follow {
            mode,
            target,
            start_distance,
            stop_distance,
            ..
        } => {
            if !running {
                *target = environment.follow_target(
                    *mode,
                    if *mode == MobFollowMode::Parent {
                        8.0
                    } else {
                        f64::from(f32::from_bits(*start_distance))
                    },
                );
            }
            target
                .as_ref()
                .and_then(|target| environment.target_position(target))
                .is_some_and(|position| {
                    let distance = distance_squared(environment.position(), position);
                    let minimum = f64::from(f32::from_bits(if running {
                        *stop_distance
                    } else {
                        *start_distance
                    }));
                    distance >= minimum * minimum
                        && (*mode != MobFollowMode::Parent
                            || reproduction.growth_age < 0 && distance <= 256.0)
                })
        }
        MobGoalProgress::Wander {
            interval,
            forced,
            target,
            ..
        } => {
            if running {
                return !brain.navigation.path.is_empty()
                    && usize::from(brain.navigation.cursor) < brain.navigation.path.len();
            }
            if !*forced && brain.random_bounded(interval.div_ceil(2)) != Some(0) {
                return false;
            }
            *target = environment
                .wander_destination(brain)
                .map(|position| position.map(f64::to_bits));
            *forced = false;
            target.is_some()
        }
        MobGoalProgress::Target {
            target,
            kind,
            chance,
            visibility,
            ..
        } => {
            if !running && brain.random_bounded((*chance).max(1)) != Some(0) {
                return false;
            }
            if !running {
                *target = environment.nearest_attackable(*kind, environment.follow_range());
            }
            if target.as_ref().is_some_and(|target| {
                environment.attackable(target) && environment.target_position(target).is_some()
                    && (!*visibility || environment.visible(target))
            }) {
                brain.target = target.clone();
                return true;
            }
            false
        }
        MobGoalProgress::Door { found, passed, .. } => *found && !*passed,
        MobGoalProgress::MoveTo { reached, .. } => !*reached,
        MobGoalProgress::Ranged { target, .. } | MobGoalProgress::Crossbow { target, .. } => {
            *target = brain.target.clone();
            target
                .as_ref()
                .and_then(|target| environment.target_position(target))
                .is_some()
        }
    }
}

fn advance(
    goal: &mut MobGoalProgress,
    brain: &mut MobBrain,
    environment: &impl MobGoalEnvironment,
    output: &mut Vec<MobIntent>,
) {
    match goal {
        MobGoalProgress::EscapeDanger {
            destination, speed, ..
        } => {
            if let Some(position) = destination.map(|position| position.map(f64::from_bits)) {
                set_navigation(brain, position, *speed, environment.position());
            }
        }
        MobGoalProgress::Swim => {
            if brain.random_bounded(5) != Some(0) {
                output.push(MobIntent::Jump);
            }
        }
        MobGoalProgress::Graze { remaining } => {
            *remaining = remaining.saturating_sub(1);
            if *remaining != 4 {
                return;
            }
            let [x, y, z] = environment.position();
            let position = BlockPos {
                x: x.floor() as i32,
                y: y.floor() as i32,
                z: z.floor() as i32,
            };
            let below = BlockPos {
                y: position.y.saturating_sub(1),
                ..position
            };
            if let Some(expected) = environment
                .block(position)
                .filter(|state| environment.edible_for_sheep(*state))
            {
                output.push(MobIntent::EatGrass {
                    position,
                    expected,
                    replacement: environment.air_state(),
                    modify_world: environment.mob_griefing(),
                });
            } else if let Some(expected) = environment
                .block(below)
                .filter(|state| environment.grass_block(*state))
            {
                output.push(MobIntent::EatGrass {
                    position: below,
                    expected,
                    replacement: environment.dirt_state(),
                    modify_world: environment.mob_griefing(),
                });
            }
        }
        MobGoalProgress::LookAround {
            remaining,
            direction,
        } => {
            *remaining = remaining.saturating_sub(1);
            let Some([x, y, z]) = environment.eye_position() else { return; };
            output.push(MobIntent::LookAt([
                (x + f64::from_bits(direction[0])).to_bits(),
                y.to_bits(),
                (z + f64::from_bits(direction[1])).to_bits(),
            ]));
        }
        MobGoalProgress::LookAt {
            target, remaining, ..
        } => {
            *remaining = remaining.saturating_sub(1);
            if let Some(position) = target
                .as_ref()
                .and_then(|target| environment.target_eye_position(target))
            {
                output.push(MobIntent::LookAt(position.map(f64::to_bits)));
            }
        }
        MobGoalProgress::Melee {
            target,
            attack_cooldown,
            path_cooldown,
            speed,
            ..
        } => {
            *attack_cooldown = attack_cooldown.saturating_sub(1);
            *path_cooldown = path_cooldown.saturating_sub(1);
            let Some(target) = target.as_ref() else {
                return;
            };
            let Some(position) = environment.target_position(target) else {
                return;
            };
            emit_target_look(output, environment, target);
            set_navigation(brain, position, *speed, environment.position());
            if *attack_cooldown == 0
                && environment.attackable(target)
                && environment.visible(target)
                && environment.in_attack_range(target)
            {
                *attack_cooldown = 20;
                output.push(MobIntent::Attack(target.clone()));
            }
        }
        MobGoalProgress::Breed {
            mate,
            remaining,
            speed,
        } => {
            *remaining += 1;
            let Some(mate) = mate.as_ref() else {
                return;
            };
            let Some(position) = environment.target_position(mate) else {
                return;
            };
            emit_target_look(output, environment, mate);
            set_navigation(brain, position, *speed, environment.position());
            if *remaining >= 30 && distance_squared(environment.position(), position) < 9.0 {
                output.push(MobIntent::Breed(mate.clone()));
            }
        }
        MobGoalProgress::Tempt { target, speed, .. }
        | MobGoalProgress::Follow { target, speed, .. } => {
            if let Some(position) = target
                .as_ref()
                .and_then(|target| environment.target_position(target))
            {
                if let Some(target) = target.as_ref() { emit_target_look(output, environment, target); }
                set_navigation(brain, position, *speed, environment.position());
            }
        }
        MobGoalProgress::Wander { target, speed, .. } => {
            if let Some(position) = target.map(|position| position.map(f64::from_bits)) {
                set_navigation(brain, position, *speed, environment.position());
            }
        }
        MobGoalProgress::Door {
            position,
            close_after,
            breaking,
            remaining,
            ..
        } => {
            if *remaining == 0 {
                output.push(MobIntent::Door {
                    position: *position,
                    open: !*close_after,
                    breaking: *breaking,
                });
            }
            *remaining = remaining.saturating_sub(1);
        }
        MobGoalProgress::MoveTo {
            destination,
            speed,
            trying,
            reached,
            ..
        } => {
            set_navigation(
                brain,
                [
                    f64::from(destination.x) + 0.5,
                    f64::from(destination.y),
                    f64::from(destination.z) + 0.5,
                ],
                *speed,
                environment.position(),
            );
            *trying = trying.saturating_add(1);
            *reached = distance_squared(
                environment.position(),
                [
                    f64::from(destination.x) + 0.5,
                    f64::from(destination.y),
                    f64::from(destination.z) + 0.5,
                ],
            ) < 1.0;
        }
        MobGoalProgress::Ranged {
            weapon,
            target,
            attack_time,
            seen_time,
            draw_ticks,
            drawing,
            speed,
            interval,
            range,
            strafing_time,
            clockwise,
            backwards,
        } => {
            let Some(target) = target.as_ref() else {
                return;
            };
            let Some(position) = environment.target_position(target) else {
                return;
            };
            let visible = environment.visible(target);
            if let MobRangedWeapon::Projectile {
                minimum_interval,
                maximum_interval,
            } = *weapon
            {
                *seen_time = if visible {
                    seen_time.saturating_add(1)
                } else {
                    0
                };
                let distance = distance_squared(environment.position(), position);
                let radius = f64::from_bits(*range);
                if distance <= radius * radius && *seen_time >= 5 {
                    stop_navigation(brain);
                } else {
                    set_navigation(brain, position, *speed, environment.position());
                }
                emit_target_look(output, environment, target);
                *attack_time = attack_time.saturating_sub(1);
                let ratio = distance.sqrt() as f32 / radius as f32;
                let cooldown = (ratio.mul_add(
                    maximum_interval.saturating_sub(minimum_interval) as f32,
                    minimum_interval as f32,
                ))
                .floor()
                .min(i32::MAX as f32) as i32;
                if *attack_time == 0 && visible {
                    output.push(MobIntent::Shoot {
                        target: target.clone(),
                        power: ratio.clamp(0.1, 1.0).to_bits(),
                    });
                    *attack_time = cooldown;
                } else if *attack_time < 0 {
                    *attack_time = cooldown;
                }
                return;
            }
            if visible != (*seen_time > 0) {
                *seen_time = 0;
            }
            *seen_time = seen_time.saturating_add(if visible { 1 } else { -1 });
            let distance = distance_squared(environment.position(), position);
            let maximum = f64::from_bits(*range).powi(2);
            if distance <= maximum && *seen_time >= 20 {
                stop_navigation(brain);
                *strafing_time = strafing_time.saturating_add(1);
            } else {
                set_navigation(brain, position, *speed, environment.position());
                *strafing_time = -1;
            }
            if *strafing_time >= 20 {
                if brain.random_bounded(10).is_some_and(|value| value < 3) {
                    *clockwise = !*clockwise;
                }
                if brain.random_bounded(10).is_some_and(|value| value < 3) {
                    *backwards = !*backwards;
                }
                *strafing_time = 0;
            }
            if *strafing_time >= 0 {
                if distance > maximum * 0.75 {
                    *backwards = false;
                } else if distance < maximum * 0.25 {
                    *backwards = true;
                }
                brain.movement.strafe(
                    if *backwards { -0.5 } else { 0.5 },
                    if *clockwise { 0.5 } else { -0.5 },
                );
            }
            emit_target_look(output, environment, target);
            if *drawing {
                if !visible && *seen_time < -60 {
                    *drawing = false;
                    *draw_ticks = 0;
                    output.push(MobIntent::EndUse);
                } else if visible {
                    *draw_ticks = draw_ticks.saturating_add(1);
                    if *draw_ticks >= 20 {
                        *drawing = false;
                        *draw_ticks = 0;
                        *attack_time = (*interval).min(i32::MAX as u32) as i32;
                        output.push(MobIntent::EndUse);
                        output.push(MobIntent::Shoot {
                            target: target.clone(),
                            power: 1.0_f32.to_bits(),
                        });
                    }
                }
            } else {
                *attack_time = attack_time.saturating_sub(1);
                if *attack_time <= 0 && *seen_time >= -60 {
                    *drawing = true;
                    *draw_ticks = 0;
                    output.push(MobIntent::BeginUse);
                }
            }
        }
        MobGoalProgress::Crossbow {
            target,
            stage,
            seen_time,
            attack_delay,
            path_delay,
            charge_ticks,
            speed,
            range,
        } => {
            let Some(target) = target.as_ref() else {
                return;
            };
            let Some(position) = environment.target_position(target) else {
                return;
            };
            let visible = environment.visible(target);
            if visible != (*seen_time > 0) {
                *seen_time = 0;
            }
            *seen_time = seen_time.saturating_add(if visible { 1 } else { -1 });
            let needs_move = (distance_squared(environment.position(), position)
                > f64::from_bits(*range).powi(2)
                || *seen_time < 5)
                && *attack_delay == 0;
            if needs_move {
                *path_delay = path_delay.saturating_sub(1);
                if *path_delay == 0 {
                    let move_speed = if *stage == CrossbowStage::Charging {
                        (f64::from_bits(*speed) * 0.5).to_bits()
                    } else {
                        *speed
                    };
                    set_navigation(brain, position, move_speed, environment.position());
                    *path_delay = 20 + brain.random_bounded(20).unwrap_or(0);
                }
            } else {
                *path_delay = 0;
                stop_navigation(brain);
            }
            emit_target_look(output, environment, target);
            match *stage {
                CrossbowStage::Uncharged if !needs_move => {
                    *stage = CrossbowStage::Charging;
                    *charge_ticks = 0;
                    output.push(MobIntent::BeginUse);
                }
                CrossbowStage::Charging => {
                    *charge_ticks = charge_ticks.saturating_add(1);
                    if *charge_ticks >= 25 {
                        *stage = CrossbowStage::Charged;
                        *attack_delay = 20 + brain.random_bounded(20).unwrap_or(0);
                        output.push(MobIntent::EndUse);
                    }
                }
                CrossbowStage::Charged => {
                    *attack_delay = attack_delay.saturating_sub(1);
                    if *attack_delay == 0 {
                        *stage = CrossbowStage::Ready;
                    }
                }
                CrossbowStage::Ready if visible => {
                    output.push(MobIntent::Shoot {
                        target: target.clone(),
                        power: 1.0_f32.to_bits(),
                    });
                    *stage = CrossbowStage::Uncharged;
                }
                _ => {}
            }
        }
        MobGoalProgress::Target { .. } => {}
    }
}

fn stop_navigation(brain: &mut MobBrain) {
    brain.navigation.stop();
}

fn set_navigation(brain: &mut MobBrain, position: [f64; 3], speed: u64, current: [f64; 3]) {
    if !brain.navigation.path.is_empty() && !brain.navigation.needs_path(position) {
        brain.navigation.speed = speed;
        brain.navigation.goal = Some(crate::mob_continuation::MobNavigationGoal {
            destination: position.map(f64::to_bits), current_progress: current.map(f64::to_bits), speed });
        return;
    }
    let destination = BlockPos {
        x: position[0].floor() as i32,
        y: position[1].floor() as i32,
        z: position[2].floor() as i32,
    };
    brain.navigation.path.clear();
    brain.navigation.path_reached = false;
    brain.navigation.path_distance = f32::MAX.to_bits();
    brain.navigation.cursor = 0;
    brain.navigation.target = Some(destination);
    brain.navigation.speed = speed;
    brain.navigation.goal = Some(crate::mob_continuation::MobNavigationGoal {
        destination: position.map(f64::to_bits),
        current_progress: current.map(f64::to_bits),
        speed,
    });
    brain.navigation.last_stuck_check = brain.navigation.tick_count;
    brain.navigation.last_stuck_position = current.map(f64::to_bits);
    brain.navigation.timeout_cached_node = BlockPos { x: 0, y: 0, z: 0 };
    brain.navigation.timeout_timer = 0;
    brain.navigation.timeout_limit = 0.0_f64.to_bits();
    brain.navigation.stuck = false;
}

fn distance_squared(left: [f64; 3], right: [f64; 3]) -> f64 {
    left.into_iter()
        .zip(right)
        .map(|(a, b)| (a - b).powi(2))
        .sum()
}

fn emit_target_look(output: &mut Vec<MobIntent>, environment: &impl MobGoalEnvironment, target: &ContactIdentity) {
    if let Some(position) = environment.target_eye_position(target) {
        output.push(MobIntent::LookAt(position.map(f64::to_bits)));
    }
}
