use serde::{Deserialize, Serialize};

use crate::item_contact::ContactIdentity;
use crate::protocol::BlockPos;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobContinuation {
    pub brain: MobBrain,
    pub species: MobSpecies,
    pub reproduction: MobReproduction,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobReproduction {
    pub growth_age: i32,
    pub age_locked: bool,
    pub forced_age: i32,
    pub forced_age_timer: u32,
    pub age_lock_particle_timer: u32,
    pub breeding_cooldown: u32,
    pub love_ticks: u32,
    pub breeder: Option<uuid::Uuid>,
}

impl MobReproduction {
    pub fn valid(&self) -> bool {
        self.breeder.is_none_or(|uuid| !uuid.is_nil())
    }

    pub fn advance(&self, elapsed: u32) -> Self {
        let mut next = self.clone();
        next.breeding_cooldown = next.breeding_cooldown.saturating_sub(elapsed);
        next.love_ticks = next.love_ticks.saturating_sub(elapsed);
        next.forced_age_timer = next.forced_age_timer.saturating_sub(elapsed);
        next.age_lock_particle_timer = next.age_lock_particle_timer.saturating_sub(elapsed);
        if next.growth_age < 0 && !next.age_locked {
            next.growth_age = i64::from(next.growth_age)
                .saturating_add(i64::from(elapsed))
                .min(0) as i32;
        } else if next.growth_age > 0 {
            next.growth_age = i64::from(next.growth_age)
                .saturating_sub(i64::from(elapsed))
                .max(0) as i32;
        }
        next
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobBrain {
    pub fall_distance: u32,
    pub velocity_revision: [u64; 3],
    pub look: MobLookControl,
    pub movement: MobMoveControl,
    pub jump_requested: bool,
    pub random: [u64; 2],
    pub random_gaussian: Option<u64>,
    pub processed_tick: Option<TickStamp>,
    pub age: u32,
    pub idle_ticks: u32,
    pub target: Option<ContactIdentity>,
    pub restriction: Option<(BlockPos, u32)>,
    pub persistence_required: bool,
    pub ai_disabled: bool,
    pub left_handed: bool,
    pub can_pick_up_loot: bool,
    pub attacking: bool,
    pub disabled_controls: u8,
    pub selector_phase: u8,
    pub goals: Vec<MobGoal>,
    pub navigation: MobNavigation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobMoveOperation {
    Wait,
    MoveTo,
    Strafe,
    Jumping,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobMoveControl {
    pub wanted: [u64; 3],
    pub speed_modifier: u64,
    pub strafe: [u32; 2],
    pub operation: MobMoveOperation,
    pub input: [u64; 3],
}

impl MobMoveControl {
    pub fn valid(&self) -> bool {
        self.wanted
            .into_iter()
            .chain(self.input)
            .chain([self.speed_modifier])
            .all(|value| f64::from_bits(value).is_finite())
            && self
                .strafe
                .into_iter()
                .all(|value| f32::from_bits(value).is_finite())
    }

    pub fn move_to(&mut self, position: [f64; 3], speed: f64) {
        self.wanted = position.map(f64::to_bits);
        self.speed_modifier = speed.to_bits();
        if self.operation != MobMoveOperation::Jumping {
            self.operation = MobMoveOperation::MoveTo;
        }
    }

    pub fn strafe(&mut self, forwards: f32, sideways: f32) {
        self.strafe = [forwards.to_bits(), sideways.to_bits()];
        self.speed_modifier = 0.25_f64.to_bits();
        self.operation = MobMoveOperation::Strafe;
    }

    pub fn advance(
        &mut self,
        position: [f64; 3],
        yaw: f32,
        on_ground: bool,
        movement_speed: f64,
        step_height: f64,
        width: f64,
    ) -> (f32, bool) {
        match self.operation {
            MobMoveOperation::Strafe => {
                self.input = [
                    f64::from(f32::from_bits(self.strafe[1])),
                    0.0,
                    f64::from(f32::from_bits(self.strafe[0])),
                ]
                .map(f64::to_bits);
                self.operation = MobMoveOperation::Wait;
                (yaw, false)
            }
            MobMoveOperation::MoveTo => {
                self.operation = MobMoveOperation::Wait;
                let wanted = self.wanted.map(f64::from_bits);
                let dx = wanted[0] - position[0];
                let dy = wanted[1] - position[1];
                let dz = wanted[2] - position[2];
                if dx * dx + dy * dy + dz * dz < 2.5000003e-7 {
                    self.input = [0.0_f64.to_bits(); 3];
                    return (yaw, false);
                }
                let yaw = change_angle(yaw, (dz.atan2(dx).to_degrees() as f32) - 90.0, 90.0);
                self.input = [
                    0.0,
                    0.0,
                    f64::from_bits(self.speed_modifier) * movement_speed,
                ]
                .map(f64::to_bits);
                let jump = dy > step_height && dx * dx + dz * dz < width.max(1.0);
                if jump {
                    self.operation = MobMoveOperation::Jumping;
                }
                (yaw, jump)
            }
            MobMoveOperation::Jumping => {
                self.input = [
                    0.0,
                    0.0,
                    f64::from_bits(self.speed_modifier) * movement_speed,
                ]
                .map(f64::to_bits);
                if on_ground {
                    self.operation = MobMoveOperation::Wait;
                }
                (yaw, false)
            }
            MobMoveOperation::Wait => (yaw, false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobLookControl {
    pub head_yaw: u32,
    pub max_yaw_change: u32,
    pub max_pitch_change: u32,
    pub remaining: u32,
    pub target: [u64; 3],
}

impl MobLookControl {
    pub fn valid(&self) -> bool {
        [self.head_yaw, self.max_yaw_change, self.max_pitch_change]
            .into_iter()
            .all(|value| f32::from_bits(value).is_finite())
            && self
                .target
                .into_iter()
                .all(|value| f64::from_bits(value).is_finite())
    }

    pub fn look_at(&mut self, target: [f64; 3], max_yaw_change: f32, max_pitch_change: f32) {
        self.target = target.map(f64::to_bits);
        self.max_yaw_change = max_yaw_change.to_bits();
        self.max_pitch_change = max_pitch_change.to_bits();
        self.remaining = 2;
    }

    pub fn advance(
        &mut self,
        eye: [f64; 3],
        body_yaw: f32,
        navigating: bool,
        max_head_rotation: f32,
    ) -> f32 {
        let mut pitch = 0.0;
        let mut head_yaw = f32::from_bits(self.head_yaw);
        if self.remaining > 0 {
            self.remaining -= 1;
            let [x, y, z] = self.target.map(f64::from_bits);
            let dx = x - eye[0];
            let dy = y - eye[1];
            let dz = z - eye[2];
            if dx.abs() > 1.0e-5 || dz.abs() > 1.0e-5 {
                let target_yaw = (dz.atan2(dx) as f32).to_degrees() - 90.0;
                head_yaw = change_angle(head_yaw, target_yaw, f32::from_bits(self.max_yaw_change));
            }
            let horizontal = dx.hypot(dz);
            if dy.abs() > 1.0e-5 || horizontal.abs() > 1.0e-5 {
                pitch = change_angle(
                    0.0,
                    -(dy.atan2(horizontal) as f32).to_degrees(),
                    f32::from_bits(self.max_pitch_change),
                );
            }
        } else {
            head_yaw = change_angle(head_yaw, body_yaw, 10.0);
        }
        if navigating {
            head_yaw = change_angle(body_yaw, head_yaw, max_head_rotation);
        }
        self.head_yaw = head_yaw.to_bits();
        pitch
    }
}

fn change_angle(current: f32, target: f32, limit: f32) -> f32 {
    let difference = (target - current + 180.0).rem_euclid(360.0) - 180.0;
    current + difference.clamp(-limit.abs(), limit.abs())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobGoal {
    pub selector: MobGoalSelector,
    pub every_tick: bool,
    pub stoppable: bool,
    pub priority: u8,
    pub controls: u8,
    pub running: bool,
    pub progress: MobGoalProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobGoalSelector {
    Targets,
    Behavior,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobGoalProgress {
    Swim,
    EscapeDanger {
        speed: u64,
        destination: Option<[u64; 3]>,
        panicking: bool,
    },
    Wander {
        interval: u32,
        speed: u64,
        target: Option<[u64; 3]>,
        forced: bool,
    },
    LookAround {
        remaining: u32,
        direction: [u64; 2],
    },
    LookAt {
        target: Option<ContactIdentity>,
        remaining: u32,
        range: u32,
        chance: u32,
        kind: u16,
        forward: bool,
    },
    Graze {
        remaining: u32,
    },
    Melee {
        target: Option<ContactIdentity>,
        attack_cooldown: u32,
        path_cooldown: u32,
        last_target: [u64; 3],
        last_can_use: u32,
        speed: u64,
        following: bool,
    },
    Breed {
        mate: Option<ContactIdentity>,
        remaining: u32,
        speed: u64,
    },
    Tempt {
        target: Option<ContactIdentity>,
        cooldown: u32,
        watched: [u64; 3],
        yaw: u32,
        pitch: u32,
        speed: u64,
        items: Vec<u16>,
        can_scare: bool,
    },
    Follow {
        mode: MobFollowMode,
        target: Option<ContactIdentity>,
        remaining: u32,
        speed: u64,
        start_distance: u32,
        stop_distance: u32,
    },
    Door {
        position: BlockPos,
        found: bool,
        passed: bool,
        direction: [u32; 2],
        remaining: u32,
        close_after: bool,
        breaking: bool,
    },
    Ranged {
        weapon: MobRangedWeapon,
        target: Option<ContactIdentity>,
        attack_time: i32,
        seen_time: i32,
        draw_ticks: u32,
        drawing: bool,
        speed: u64,
        interval: u32,
        range: u64,
        strafing_time: i32,
        clockwise: bool,
        backwards: bool,
    },
    Crossbow {
        target: Option<ContactIdentity>,
        stage: CrossbowStage,
        seen_time: i32,
        attack_delay: u32,
        path_delay: u32,
        charge_ticks: u32,
        speed: u64,
        range: u64,
    },
    Target {
        target: Option<ContactIdentity>,
        timestamp: u32,
        unseen_ticks: u32,
        chance: u32,
        kind: Option<u16>,
        visibility: bool,
        navigation: bool,
    },
    MoveTo {
        destination: BlockPos,
        cooldown: u32,
        trying: i32,
        safe_wait: u32,
        reached: bool,
        speed: u64,
        range: u32,
        max_y: i32,
        min_y: i32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobFollowMode {
    Parent,
    Owner,
    DifferentSpecies,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobRangedWeapon {
    Bow,
    Projectile {
        minimum_interval: u32,
        maximum_interval: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrossbowStage {
    Uncharged,
    Charging,
    Charged,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NavigationKind {
    Walk,
    Fly,
    Swim,
    Amphibious,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobNavigation {
    pub age: u64,
    pub goal: Option<MobNavigationGoal>,
    pub timeout_cached_node: BlockPos,
    pub timeout_timer: u64,
    pub last_timeout_check: u64,
    pub timeout_limit: u64,
    pub maximum_waypoint_distance: u32,
    pub delayed_recomputation: bool,
    pub last_recomputation: u64,
    pub reach_range: i32,
    pub maximum_visited_multiplier: u32,
    pub stuck: bool,
    pub required_path_length: u32,
    pub last_node_index: u16,
    pub total_ticks: u64,
    pub path_start_position: Option<[u64; 3]>,
    pub can_path_below_surface: bool,
    pub kind: NavigationKind,
    pub path: Vec<MobPathNode>,
    pub path_reached: bool,
    pub path_distance: u32,
    pub cursor: u16,
    pub target: Option<BlockPos>,
    pub speed: u64,
    pub tick_count: u32,
    pub last_stuck_check: u32,
    pub last_stuck_position: [u64; 3],
    pub node_ticks: u32,
    pub repath_cooldown: u32,
    pub can_float: bool,
    pub can_open_doors: bool,
    pub can_pass_doors: bool,
    pub can_walk_over_fences: bool,
    pub avoid_sun: bool,
    pub path_type_malus: Vec<(u8, u32)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobPathNode {
    pub position: BlockPos,
    pub hash: i32,
    pub heap_index: i32,
    pub costs: [u32; 5],
    pub predecessor: Option<BlockPos>,
    pub closed: bool,
    pub path_type: u8,
}

impl MobPathNode {
    pub fn valid(&self) -> bool {
        self.path_type < 27
            && self
                .costs
                .into_iter()
                .all(|bits| f32::from_bits(bits).is_finite())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobNavigationGoal {
    pub destination: [u64; 3],
    pub current_progress: [u64; 3],
    pub speed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobSpecies {
    Generic,
    Horse {
        kind: HorseSpeciesKind,
        color: u8,
        markings: u8,
        flags: u8,
        chested: bool,
        temper: u32,
        owner: Option<uuid::Uuid>,
    },
    Llama {
        trader: bool,
        variant: u8,
        strength: u8,
        carpet_color: Option<u8>,
        flags: u8,
        chested: bool,
        temper: u32,
        owner: Option<uuid::Uuid>,
    },
    Sheep {
        color: u8,
        sheared: bool,
    },
    Creeper {
        fuse: u32,
        previous_fuse: u32,
        maximum_fuse: u32,
        radius: u32,
        ignited: bool,
        powered: bool,
    },
    Slime {
        size: u32,
        jump_delay: u32,
        squish: u32,
        previous_squish: u32,
        target_squish: u32,
    },
    Enderman {
        carried_block: Option<u16>,
        teleport_cooldown: u32,
        aggressive: bool,
        stared_at: bool,
    },
    Zombie {
        baby: bool,
        conversion: Option<u32>,
        breaking_doors: bool,
    },
    Skeleton {
        ranged_cooldown: u32,
        converting: Option<u32>,
    },
    Bat {
        resting: bool,
        target: Option<BlockPos>,
    },
    Phantom {
        size: u32,
        anchor: BlockPos,
        phase: u8,
        target: [u64; 3],
        sweep_ticks: u32,
    },
    Blaze {
        attack_step: u32,
        attack_time: u32,
        unseen_ticks: u32,
        charged: bool,
        height_offset: u32,
        height_ticks: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HorseSpeciesKind {
    Horse,
    Donkey,
    Mule,
    Skeleton,
    Zombie,
}

impl MobContinuation {
    pub fn valid(&self) -> bool {
        self.brain.valid() && self.species.valid() && self.reproduction.valid()
    }
}

impl MobBrain {
    pub fn at_birth(
        random: [u64; 2],
        position: [f64; 3],
        yaw: f32,
        kind: NavigationKind,
    ) -> Option<Self> {
        let brain = Self {
            fall_distance: 0.0_f32.to_bits(),
            velocity_revision: [0; 3],
            look: MobLookControl {
                head_yaw: yaw.to_bits(),
                max_yaw_change: 0.0_f32.to_bits(),
                max_pitch_change: 0.0_f32.to_bits(),
                remaining: 0,
                target: position.map(f64::to_bits),
            },
            movement: MobMoveControl {
                wanted: position.map(f64::to_bits),
                speed_modifier: 0.0_f64.to_bits(),
                strafe: [0.0_f32.to_bits(); 2],
                operation: MobMoveOperation::Wait,
                input: [0.0_f64.to_bits(); 3],
            },
            jump_requested: false,
            random,
            random_gaussian: None,
            processed_tick: None,
            age: 0,
            idle_ticks: 0,
            target: None,
            restriction: None,
            persistence_required: false,
            ai_disabled: false,
            left_handed: false,
            can_pick_up_loot: false,
            attacking: false,
            disabled_controls: 0,
            selector_phase: 0,
            goals: Vec::new(),
            navigation: MobNavigation {
                age: 0,
                goal: None,
                timeout_cached_node: BlockPos { x: 0, y: 0, z: 0 },
                timeout_timer: 0,
                last_timeout_check: 0,
                timeout_limit: 0.0_f64.to_bits(),
                maximum_waypoint_distance: 0.5_f32.to_bits(),
                delayed_recomputation: false,
                last_recomputation: 0,
                reach_range: 1,
                maximum_visited_multiplier: 1.0_f32.to_bits(),
                stuck: false,
                required_path_length: 16.0_f32.to_bits(),
                last_node_index: 0,
                total_ticks: 0,
                path_start_position: None,
                can_path_below_surface: false,
                kind,
                path: Vec::new(),
                path_reached: false,
                path_distance: f32::MAX.to_bits(),
                cursor: 0,
                target: None,
                speed: 0.0_f64.to_bits(),
                tick_count: 0,
                last_stuck_check: 0,
                last_stuck_position: position.map(f64::to_bits),
                node_ticks: 0,
                repath_cooldown: 0,
                can_float: false,
                can_open_doors: false,
                can_pass_doors: true,
                can_walk_over_fences: false,
                avoid_sun: false,
                path_type_malus: Vec::new(),
            },
        };
        brain.valid().then_some(brain)
    }

    pub fn next_random(&mut self) -> u64 {
        let [low, high] = self.random;
        let value = low.wrapping_add(high).rotate_left(17).wrapping_add(low);
        let mixed = high ^ low;
        self.random = [
            low.rotate_left(49) ^ mixed ^ (mixed << 21),
            mixed.rotate_left(28),
        ];
        value
    }

    pub fn random_bounded(&mut self, bound: u32) -> Option<u32> {
        (bound != 0).then(|| ((u128::from(self.next_random()) * u128::from(bound)) >> 64) as u32)
    }

    pub fn valid(&self) -> bool {
        self.random != [0; 2]
            && f32::from_bits(self.fall_distance).is_finite()
            && f32::from_bits(self.fall_distance) >= 0.0
            && self.look.valid()
            && self.movement.valid()
            && self
                .random_gaussian
                .is_none_or(|bits| f64::from_bits(bits).is_finite())
            && self
                .target
                .as_ref()
                .is_none_or(crate::actor_relation::identity_valid)
            && self.goals.len() <= 64
            && self.disabled_controls < 16
            && self.selector_phase < 2
            && self.goals.iter().all(MobGoal::valid)
            && self.navigation.valid()
    }
}

impl MobSpecies {
    pub fn valid(&self) -> bool {
        match *self {
            MobSpecies::Horse {
                kind,
                color,
                markings,
                flags,
                chested,
                temper,
                owner,
            } => {
                color < 7
                    && markings < 5
                    && flags & 129 == 0
                    && temper <= 100
                    && (matches!(kind, HorseSpeciesKind::Horse) || (color == 0 && markings == 0))
                    && (!chested
                        || matches!(kind, HorseSpeciesKind::Donkey | HorseSpeciesKind::Mule))
                    && owner.is_none_or(|owner| !owner.is_nil())
            }
            MobSpecies::Llama {
                variant,
                strength,
                carpet_color,
                flags,
                temper,
                owner,
                ..
            } => {
                variant < 4
                    && (1..=5).contains(&strength)
                    && carpet_color.is_none_or(|color| color < 16)
                    && flags & 133 == 0
                    && temper <= 100
                    && owner.is_none_or(|owner| !owner.is_nil())
            }
            MobSpecies::Sheep { color, .. } => color < 16,
            MobSpecies::Slime {
                size,
                squish,
                previous_squish,
                target_squish,
                ..
            } => {
                size > 0
                    && size <= 127
                    && [squish, previous_squish, target_squish]
                        .into_iter()
                        .all(|value| f32::from_bits(value).is_finite())
            }
            MobSpecies::Phantom { phase, target, .. } => {
                phase <= 1
                    && target
                        .into_iter()
                        .all(|value| f64::from_bits(value).is_finite())
            }
            MobSpecies::Blaze { height_offset, .. } => f32::from_bits(height_offset).is_finite(),
            _ => true,
        }
    }
}

impl MobGoal {
    pub fn valid(&self) -> bool {
        if self.controls & !15 != 0 {
            return false;
        }
        let finite = |value| f64::from_bits(value).is_finite();
        let target_valid = |target: &Option<ContactIdentity>| {
            target
                .as_ref()
                .is_none_or(crate::actor_relation::identity_valid)
        };
        match &self.progress {
            MobGoalProgress::Swim | MobGoalProgress::Graze { .. } => true,
            MobGoalProgress::EscapeDanger {
                speed, destination, ..
            } => {
                finite(*speed)
                    && destination.is_none_or(|position| position.into_iter().all(finite))
            }
            MobGoalProgress::MoveTo { speed, .. } => finite(*speed),
            MobGoalProgress::Wander { speed, target, .. } => {
                finite(*speed) && target.is_none_or(|values| values.into_iter().all(finite))
            }
            MobGoalProgress::LookAround { direction, .. } => direction.iter().copied().all(finite),
            MobGoalProgress::LookAt {
                target,
                range,
                chance,
                ..
            } => {
                target_valid(target)
                    && f32::from_bits(*range).is_finite()
                    && (0.0..=1.0).contains(&f32::from_bits(*chance))
            }
            MobGoalProgress::Melee {
                target,
                speed,
                last_target,
                ..
            } => target_valid(target) && finite(*speed) && last_target.iter().copied().all(finite),
            MobGoalProgress::Breed { mate, speed, .. } => target_valid(mate) && finite(*speed),
            MobGoalProgress::Tempt {
                target,
                speed,
                watched,
                yaw,
                pitch,
                items,
                ..
            } => {
                target_valid(target)
                    && finite(*speed)
                    && watched.iter().copied().all(finite)
                    && f32::from_bits(*yaw).is_finite()
                    && f32::from_bits(*pitch).is_finite()
                    && items.len() <= 256
            }
            MobGoalProgress::Follow {
                target,
                speed,
                start_distance,
                stop_distance,
                ..
            } => {
                target_valid(target)
                    && finite(*speed)
                    && [*start_distance, *stop_distance]
                        .into_iter()
                        .all(|value| f32::from_bits(value).is_finite())
            }
            MobGoalProgress::Door { direction, .. } => direction
                .iter()
                .all(|value| f32::from_bits(*value).is_finite()),
            MobGoalProgress::Ranged {
                target,
                speed,
                range,
                weapon,
                interval,
                ..
            } => {
                target_valid(target)
                    && finite(*speed)
                    && finite(*range)
                    && f64::from_bits(*range) > 0.0
                    && *interval <= i32::MAX as u32
                    && match weapon {
                        MobRangedWeapon::Bow => true,
                        MobRangedWeapon::Projectile {
                            minimum_interval,
                            maximum_interval,
                        } => {
                            *minimum_interval > 0
                                && minimum_interval <= maximum_interval
                                && *maximum_interval <= i32::MAX as u32
                        }
                    }
            }
            MobGoalProgress::Crossbow {
                target,
                speed,
                range,
                ..
            } => target_valid(target) && finite(*speed) && finite(*range),
            MobGoalProgress::Target { target, .. } => target_valid(target),
        }
    }
}

impl MobNavigation {
    pub fn needs_path(&self, destination: [f64; 3]) -> bool {
        if self.path.is_empty() { return true; }
        if self.repath_cooldown != 0 { return false; }
        let Some(target) = self.target else { return true; };
        let distance = [target.x, target.y, target.z].into_iter().zip(destination)
            .map(|(a, b)| (f64::from(a) - b.floor()).powi(2)).sum::<f64>();
        let remaining = self.path[usize::from(self.cursor)..].windows(2).map(|nodes| {
            let a = nodes[0].position;
            let b = nodes[1].position;
            [a.x, a.y, a.z].into_iter().zip([b.x, b.y, b.z])
                .map(|(a, b)| (f64::from(a) - f64::from(b)).powi(2)).sum::<f64>().sqrt()
        }).sum::<f64>().clamp(4.0, 16.0);
        distance > (remaining * 0.5).powi(2)
    }

    pub fn ground_waypoint(&mut self, position: [f64; 3], surface_y: f64,
        width: f32, movement_speed: f64, step_height: f64, on_ground: bool,
        yaw: f32) -> Option<(f32, f64, bool)> {
        let Some(goal) = self.goal.as_ref() else { return None; };
        if goal.current_progress == goal.destination { self.stop(); return None; }
        self.total_ticks = self.total_ticks.saturating_add(1);
        self.detect_stuck([position[0], surface_y, position[2]], movement_speed);
        let Some(goal) = self.goal.as_ref() else { return None; };
        let speed = f64::from_bits(goal.speed);
        let index = usize::from(self.cursor);
        let Some(node) = self.path.get(index) else { self.stop(); return None; };
        let target = node.position;
        let path_type = node.path_type;
        if self.cursor == self.last_node_index {
            self.node_ticks = self.node_ticks.saturating_add(1);
        } else { self.node_ticks = 0; self.last_node_index = self.cursor; }
        if self.node_ticks > 100 { self.stop(); return None; }
        if self.total_ticks.is_multiple_of(100) {
            if self.path_start_position.is_some_and(|start| start.into_iter().zip(position)
                .map(|(a, b)| (f64::from_bits(a) - b).powi(2)).sum::<f64>() < 4.0) {
                self.stop(); return None;
            }
            self.path_start_position = Some(position.map(f64::to_bits));
        }
        let current = [f64::from(target.x) + 0.5, f64::from(target.y), f64::from(target.z) + 0.5];
        let dx = current[0] - position[0];
        let dy = current[1] - position[1];
        let dz = current[2] - position[2];
        let horizontal_squared = dx * dx + dz * dz;
        let distance = if width > 0.75 { width * 0.5 } else { 0.75 - width * 0.5 };
        self.maximum_waypoint_distance = distance.to_bits();
        let near = horizontal_squared.sqrt() < f64::from(distance);
        let corner = !matches!(path_type, 3 | 13 | 14) && self.path.get(index + 1).is_some_and(|next| {
            let at = [position[0], surface_y, position[2]];
            let a = std::array::from_fn::<_, 3, _>(|axis| current[axis] - at[axis]);
            let next = next.position;
            let next = [f64::from(next.x) + 0.5, f64::from(next.y), f64::from(next.z) + 0.5];
            let b = std::array::from_fn::<_, 3, _>(|axis| next[axis] - at[axis]);
            let a_squared = a.into_iter().map(|value| value * value).sum::<f64>();
            let b_squared = b.into_iter().map(|value| value * value).sum::<f64>();
            a_squared <= 4.0 && (b_squared < a_squared || a_squared < 0.5)
                && a_squared.sqrt() >= 1.0e-5 && b_squared.sqrt() >= 1.0e-5
                && a.into_iter().zip(b).map(|(a, b)| a * b).sum::<f64>() < 0.0
        });
        if (!on_ground && near && dy < -0.5) || (near && dy.abs() < 1.0) || corner {
            self.cursor = self.cursor.saturating_add(1);
            return None;
        }
        let target_yaw = (dz.atan2(dx).to_degrees() as f32) - 90.0;
        Some((change_angle(yaw, target_yaw, 90.0), speed * movement_speed,
            (dy > step_height || f64::from(target.y) > position[1])
                && horizontal_squared < f64::from(width).max(1.0).powi(2)))
    }

    pub fn stop(&mut self) {
        self.goal = None;
        self.path.clear();
        self.path_reached = false;
        self.path_distance = f32::MAX.to_bits();
        self.cursor = 0;
        self.target = None;
        self.node_ticks = 0;
        self.total_ticks = 0;
        self.path_start_position = None;
    }

    pub fn advance_clock(&mut self) {
        self.age = self.age.saturating_add(1);
        self.tick_count = self.tick_count.wrapping_add(1);
        self.repath_cooldown = self.repath_cooldown.saturating_sub(1);
    }

    pub fn detect_stuck(&mut self, position: [f64; 3], movement_speed: f64) {
        if self.tick_count.wrapping_sub(self.last_stuck_check) > 100 {
            let speed = movement_speed as f32;
            let speed = if speed >= 1.0 { speed } else { speed * speed };
            let threshold = f64::from(speed * 100.0 * 0.25).powi(2);
            let distance = position
                .into_iter()
                .zip(self.last_stuck_position.map(f64::from_bits))
                .map(|(current, old)| (current - old).powi(2))
                .sum::<f64>();
            self.stuck = distance < threshold;
            if self.stuck {
                self.stop();
            }
            self.last_stuck_check = self.tick_count;
            self.last_stuck_position = position.map(f64::to_bits);
        }
        let Some(node) = self
            .path
            .get(usize::from(self.cursor))
            .map(|node| node.position)
        else {
            return;
        };
        if node == self.timeout_cached_node {
            self.timeout_timer = self
                .timeout_timer
                .saturating_add(self.age.saturating_sub(self.last_timeout_check));
        } else {
            self.timeout_cached_node = node;
            let center = [
                f64::from(node.x) + 0.5,
                f64::from(node.y),
                f64::from(node.z) + 0.5,
            ];
            let distance = position
                .into_iter()
                .zip(center)
                .map(|(current, target)| (current - target).powi(2))
                .sum::<f64>()
                .sqrt();
            self.timeout_limit = if movement_speed > 0.0 {
                distance / movement_speed * 20.0
            } else {
                0.0
            }
            .to_bits();
        }
        let limit = f64::from_bits(self.timeout_limit);
        if limit > 0.0 && self.timeout_timer as f64 > limit * 3.0 {
            self.timeout_cached_node = BlockPos { x: 0, y: 0, z: 0 };
            self.timeout_timer = 0;
            self.timeout_limit = 0.0_f64.to_bits();
            self.stuck = false;
            self.stop();
        }
        self.last_timeout_check = self.age;
    }

    pub fn valid(&self) -> bool {
        self.path.len() <= 4096
            && self.path.iter().all(MobPathNode::valid)
            && f32::from_bits(self.path_distance).is_finite()
            && self.goal.as_ref().is_none_or(|goal| {
                goal.destination
                    .iter()
                    .chain(&goal.current_progress)
                    .all(|value| f64::from_bits(*value).is_finite())
                    && f64::from_bits(goal.speed).is_finite()
            })
            && f64::from_bits(self.timeout_limit).is_finite()
            && [
                self.maximum_waypoint_distance,
                self.maximum_visited_multiplier,
                self.required_path_length,
            ]
            .into_iter()
            .all(|value| f32::from_bits(value).is_finite() && f32::from_bits(value) >= 0.0)
            && self.path_start_position.is_none_or(|position| {
                position
                    .into_iter()
                    .all(|value| f64::from_bits(value).is_finite())
            })
            && self.reach_range >= 0
            && usize::from(self.cursor) <= self.path.len()
            && f64::from_bits(self.speed).is_finite()
            && self
                .last_stuck_position
                .into_iter()
                .all(|value| f64::from_bits(value).is_finite())
            && self.path_type_malus.len() <= 64
            && self
                .path_type_malus
                .iter()
                .enumerate()
                .all(|(index, (kind, value))| {
                    f32::from_bits(*value).is_finite()
                        && self.path_type_malus[..index]
                            .iter()
                            .all(|(other, _)| other != kind)
                })
    }
}

pub fn continuation_action(
    body: &crate::entity_body::EntityBody,
    chunk: crate::protocol::ChunkKey,
    issuer: crate::realm_completion::PeerGeneration,
    tick: TickStamp,
    id: crate::canonical_action::ActionId,
    brain: MobBrain,
) -> Result<crate::canonical_action::Action, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::{
        Action, ActionClass, ActionKind, ChunkPart, ClientOrigin, Effect, EntityMoveAuthority,
        Stakeholder, Target, UndoProcedure,
    };
    if issuer != body.owner
        || issuer.generation == 0
        || !body.belongs_to(body.id, chunk)
        || id.actor != crate::actor::ActorId::Entity(body.id)
        || id.incarnation != u64::from(body.id.generation)
        || brain.processed_tick != Some(tick)
        || !brain.valid()
        || !matches!(&body.payload, crate::entity_body::EntityPayload::Living(living) if living.mob.is_some())
    {
        return Err(crate::canonical_action::ApplyFailure::InvalidAction);
    }
    let action = Action {
        edit_authority: None,
        id,
        tick,
        class: ActionClass::Infallible,
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(chunk)],
        dependencies: Vec::new(),
        conditions: Vec::new(),
        effects: vec![Effect::UpdateMobContinuation {
            target: Target::Chunk {
                key: chunk,
                part: ChunkPart::LivingProperty {
                    actor: id.actor,
                    property: crate::living_property::LivingProperty::MobContinuation,
                },
                inventory: None,
            },
            authority: EntityMoveAuthority {
                birth: body.birth.clone(),
                body_owner: body.owner,
                issuer,
            },
            brain,
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        presentation: Vec::new(),
        kind: ActionKind::Actor,
        client_origin: ClientOrigin::None,
    };
    action.validate()?;
    Ok(action)
}
