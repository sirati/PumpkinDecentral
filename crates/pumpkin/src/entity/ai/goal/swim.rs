use std::sync::atomic::Ordering;

use pumpkin_data::entity::EntityType;
use pumpkin_data::tag::{self, Taggable};

use super::{Controls, Goal};
use crate::entity::mob::Mob;
use rand::RngExt;

pub struct SwimGoal {
    goal_control: Controls,
}

impl Default for SwimGoal {
    fn default() -> Self {
        Self {
            goal_control: Controls::JUMP,
        }
    }
}

impl SwimGoal {
    fn uses_float_goal(entity_type: &EntityType) -> bool {
        !entity_type.has_tag(&tag::EntityType::MINECRAFT_AQUATIC)
    }

    fn is_in_fluid(mob: &dyn Mob) -> bool {
        let living = &mob.get_mob_entity().living_entity;
        let entity = &living.entity;
        let in_water = entity.touching_water.load(Ordering::SeqCst)
            && entity.water_height.load() > living.get_swim_height();
        Self::uses_float_goal(entity.entity_type)
            && (in_water || entity.touching_lava.load(Ordering::SeqCst))
    }
}

impl Goal for SwimGoal {
    fn can_start(&mut self, mob: &dyn Mob) -> bool {
        if !Self::is_in_fluid(mob) {
            return false;
        }

        mob.get_mob_entity()
            .navigator
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_can_float(true);
        true
    }

    fn should_continue(&mut self, mob: &dyn Mob) -> bool {
        Self::is_in_fluid(mob)
    }

    fn tick(&mut self, mob: &dyn Mob) {
        // No jump control yet; the flag it would set is what the movement tick reads anyway.
        if mob.get_random().random::<f32>() < 0.8 {
            mob.get_mob_entity()
                .living_entity
                .jumping
                .store(true, Ordering::SeqCst);
        }
    }

    fn should_run_every_tick(&self) -> bool {
        true
    }

    fn controls(&self) -> Controls {
        self.goal_control
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aquatic_mobs_do_not_use_the_terrestrial_float_goal() {
        assert!(!SwimGoal::uses_float_goal(&EntityType::COD));
        assert!(!SwimGoal::uses_float_goal(&EntityType::SQUID));
        assert!(!SwimGoal::uses_float_goal(&EntityType::NAUTILUS));
        assert!(SwimGoal::uses_float_goal(&EntityType::COW));
    }
}
