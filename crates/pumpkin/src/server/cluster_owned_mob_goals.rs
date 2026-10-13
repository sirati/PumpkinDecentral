use pumpkin_cluster::mob_continuation::{MobFollowMode, MobGoal, MobGoalProgress, MobGoalSelector};
use pumpkin_data::entity::EntityType;
use pumpkin_data::item::Item;

fn goal(priority: u8, controls: u8, every_tick: bool, progress: MobGoalProgress) -> MobGoal {
    MobGoal {
        selector: MobGoalSelector::Behavior,
        every_tick,
        stoppable: true,
        priority,
        controls,
        running: false,
        progress,
    }
}

pub(crate) fn passive(kind: &EntityType) -> Option<Vec<MobGoal>> {
    let (panic_speed, breed_speed, tempt_speed, follow_speed, foods, grazes) =
        match kind.resource_name {
            "sheep" => (1.25, 1.0, 1.1, 1.1, vec![Item::WHEAT.id], true),
            "horse" | "donkey" | "mule" | "skeleton_horse" | "zombie_horse" => (1.2, 1.0, 1.25, 1.0, vec![Item::GOLDEN_APPLE.id,
                Item::ENCHANTED_GOLDEN_APPLE.id, Item::GOLDEN_CARROT.id], false),
            "cow" | "mooshroom" => (2.0, 1.0, 1.25, 1.25, vec![Item::WHEAT.id], false),
            "pig" => (
                1.25,
                1.0,
                1.2,
                1.1,
                vec![
                    Item::CARROT.id,
                    Item::POTATO.id,
                    Item::BEETROOT.id,
                    Item::CARROT_ON_A_STICK.id,
                ],
                false,
            ),
            "chicken" => (
                1.4,
                1.0,
                1.0,
                1.1,
                vec![
                    Item::WHEAT_SEEDS.id,
                    Item::MELON_SEEDS.id,
                    Item::PUMPKIN_SEEDS.id,
                    Item::BEETROOT_SEEDS.id,
                    Item::TORCHFLOWER_SEEDS.id,
                    Item::PITCHER_POD.id,
                ],
                false,
            ),
            _ => return None,
        };
    let horse = matches!(kind.resource_name, "horse" | "donkey" | "mule" | "skeleton_horse" | "zombie_horse");
    let undead_horse = matches!(kind.resource_name, "skeleton_horse" | "zombie_horse");
    let mut goals = vec![
        goal(0, 4, true, MobGoalProgress::Swim),
        goal(
            1,
            1,
            false,
            MobGoalProgress::EscapeDanger {
                speed: f64::to_bits(panic_speed),
                destination: None,
                panicking: false,
            },
        ),
        goal(
            2,
            3,
            false,
            MobGoalProgress::Breed {
                mate: None,
                remaining: 0,
                speed: f64::to_bits(breed_speed),
            },
        ),
        goal(
            3,
            3,
            false,
            MobGoalProgress::Tempt {
                target: None,
                cooldown: 0,
                watched: [0.0_f64.to_bits(); 3],
                yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                speed: f64::to_bits(tempt_speed),
                items: foods,
                can_scare: false,
            },
        ),
        goal(
            4,
            0,
            false,
            MobGoalProgress::Follow {
                mode: MobFollowMode::Parent,
                target: None,
                remaining: 0,
                speed: f64::to_bits(follow_speed),
                start_distance: 3.0_f32.to_bits(),
                stop_distance: 3.0_f32.to_bits(),
            },
        ),
    ];
    if undead_horse || kind.resource_name == "mule" {
        goals.retain(|goal| !matches!(goal.progress, MobGoalProgress::Breed { .. }));
    }
    if undead_horse {
        goals.retain(|goal| !matches!(goal.progress, MobGoalProgress::Tempt { .. } | MobGoalProgress::Follow { .. }));
    }
    if grazes {
        goals.push(goal(5, 7, true, MobGoalProgress::Graze { remaining: 0 }));
    }
    let priority = if grazes || horse { 6 } else { 5 };
    goals.push(goal(
        priority,
        1,
        false,
        MobGoalProgress::Wander {
            interval: 120,
            speed: f64::to_bits(if horse { 0.7 } else { 1.0 }),
            target: None,
            forced: false,
        },
    ));
    goals.push(goal(
        priority + 1,
        2,
        false,
        MobGoalProgress::LookAt {
            target: None,
            remaining: 0,
            range: 6.0_f32.to_bits(),
            chance: 0.02_f32.to_bits(),
            kind: EntityType::PLAYER.id,
            forward: false,
        },
    ));
    goals.push(goal(
        priority + 2,
        2,
        true,
        MobGoalProgress::LookAround {
            remaining: 0,
            direction: [0.0_f64.to_bits(); 2],
        },
    ));
    Some(goals)
}

pub(crate) fn initial(kind: &EntityType) -> Option<Vec<MobGoal>> {
    if let Some(goals) = passive(kind) { return Some(goals); }
    let (melee_priority, wander_speed, following, targets): (_, _, _, &[(&EntityType, u8)]) =
        match kind.resource_name {
            "zombie" | "husk" | "drowned" | "zombie_villager" => (2, 1.0, false,
                &[(&EntityType::PLAYER, 2), (&EntityType::VILLAGER, 3), (&EntityType::IRON_GOLEM, 3),
                    (&EntityType::TURTLE, 5)]),
            "spider" | "cave_spider" => (3, 0.8, false,
                &[(&EntityType::PLAYER, 2), (&EntityType::IRON_GOLEM, 3)]),
            "endermite" => (4, 1.0, true, &[(&EntityType::PLAYER, 1)]),
            _ => return None,
        };
    let mut melee = goal(melee_priority, 3, true, MobGoalProgress::Melee {
        target: None, attack_cooldown: 0, path_cooldown: 0,
        last_target: [0.0_f64.to_bits(); 3], last_can_use: 0,
        speed: 1.0_f64.to_bits(), following,
    });
    melee.every_tick = true;
    let mut goals = vec![goal(0, 4, true, MobGoalProgress::Swim), melee,
        goal(7, 1, false, MobGoalProgress::Wander { interval: 120,
            speed: f64::to_bits(wander_speed), target: None, forced: false }),
        goal(8, 2, false, MobGoalProgress::LookAt { target: None, remaining: 0,
            range: 8.0_f32.to_bits(), chance: 0.02_f32.to_bits(), kind: EntityType::PLAYER.id,
            forward: false }),
        goal(8, 2, true, MobGoalProgress::LookAround { remaining: 0,
            direction: [0.0_f64.to_bits(); 2] })];
    for (target, priority) in targets {
        let mut select = goal(*priority, 8, false, MobGoalProgress::Target {
            target: None, timestamp: 0, unseen_ticks: 0, chance: 5,
            kind: Some(target.id), visibility: true, navigation: false });
        select.selector = MobGoalSelector::Targets;
        goals.push(select);
    }
    Some(goals)
}
