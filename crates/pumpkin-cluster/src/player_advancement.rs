use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvancementEvent {
    InventoryChanged {
        items: Vec<u16>,
        enchanted: bool,
        world: crate::protocol::WorldId,
        biome: Option<u8>,
    },
    PlayerKilledEntity {
        entity_type: u16,
    },
    EnterBlock {
        block: u16,
    },
    PlacedBlock {
        block: u16,
    },
    ConsumeItem {
        item: u16,
    },
    SleptInBed,
    FishedItem {
        item: u16,
    },
    EnterDimension {
        dimension: u8,
    },
    PlayerKilled,
    DeflectedDamage,
    LaunchedEyeOfEnder,
    GlowedSign,
    BredAnimal {
        parent_type: u16,
    },
    DealtOverkillDamage,
    SniperDuel,
    TwoBirdsOneArrow,
    Arbalistic,
    Bullseye,
    CuredZombieVillager,
    TradedWithVillager {
        block_y: i32,
    },
}

impl AdvancementEvent {
    pub fn valid(&self) -> bool {
        match self {
            Self::InventoryChanged { items, .. } => {
                items.len() <= 4096 && items.windows(2).all(|pair| pair[0] < pair[1])
            }
            _ => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvancementOutcome {
    pub state: PlayerAdvancementState,
    pub experience: u32,
}

impl AdvancementOutcome {
    pub fn valid(&self) -> bool {
        self.state.valid()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerAdvancementState {
    pub progress: BTreeMap<String, AdvancementProgress>,
    pub unlocked_recipes: std::collections::BTreeSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvancementProgress {
    pub criteria: BTreeMap<String, Option<u64>>,
    pub completed: bool,
}

impl PlayerAdvancementState {
    pub fn valid(&self) -> bool {
        self.unlocked_recipes.len() <= 32768
            && self
                .unlocked_recipes
                .iter()
                .all(|id| !id.is_empty() && id.len() <= 256)
            && self.progress.len() <= 4096
            && self.progress.iter().all(|(id, progress)| {
                !id.is_empty()
                    && id.len() <= 256
                    && progress.criteria.len() <= 128
                    && progress
                        .criteria
                        .keys()
                        .all(|name| !name.is_empty() && name.len() <= 256)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvancementDefinition {
    pub id: String,
    pub criteria: Vec<String>,
    pub requirements: Vec<Vec<String>>,
    pub recipes: Vec<String>,
}

impl AdvancementDefinition {
    pub fn valid(&self) -> bool {
        !self.id.is_empty()
            && self.id.len() <= 256
            && self.criteria.len() <= 128
            && self.recipes.len() <= 1024
            && self
                .recipes
                .iter()
                .all(|id| !id.is_empty() && id.len() <= 256)
            && self
                .criteria
                .iter()
                .all(|name| !name.is_empty() && name.len() <= 256)
            && self.requirements.len() <= 128
            && self.requirements.iter().all(|group| {
                !group.is_empty()
                    && group.len() <= 128
                    && group.iter().all(|name| self.criteria.contains(name))
            })
    }
}

pub fn change(
    state: &PlayerAdvancementState,
    definition: &AdvancementDefinition,
    criterion: Option<&str>,
    granted: bool,
    achieved_millis: u64,
) -> Option<PlayerAdvancementState> {
    if !state.valid()
        || !definition.valid()
        || criterion.is_some_and(|name| !definition.criteria.iter().any(|entry| entry == name))
    {
        return None;
    }
    let mut next = state.clone();
    let progress = next.progress.entry(definition.id.clone()).or_default();
    let previously_completed = progress.completed;
    for name in &definition.criteria {
        let value = progress.criteria.entry(name.clone()).or_default();
        if criterion.is_none_or(|selected| selected == name) {
            if granted {
                if value.is_none() {
                    *value = Some(achieved_millis);
                }
            } else {
                *value = None;
            }
        }
    }
    progress.completed = !definition.requirements.is_empty()
        && definition.requirements.iter().all(|group| {
            group
                .iter()
                .any(|name| progress.criteria.get(name).is_some_and(Option::is_some))
        });
    if progress.completed && !previously_completed {
        next.unlocked_recipes
            .extend(definition.recipes.iter().cloned());
    }
    next.valid().then_some(next)
}

#[cfg(test)]
mod event_tests {
    use super::*;

    #[test]
    fn inventory_fact_set_must_be_bounded_sorted_and_unique() {
        let event = |items| AdvancementEvent::InventoryChanged {
            items,
            enchanted: false,
            world: crate::protocol::WorldId::new(0, 1),
            biome: None,
        };
        assert!(event(vec![1, 2]).valid());
        assert!(event(vec![]).valid());
        assert!(!event(vec![2, 1]).valid());
        assert!(!event(vec![1, 1]).valid());
        assert!(!event(vec![1; 4097]).valid());
    }

    #[test]
    fn criterion_completion_unlocks_declared_recipes_once_and_preserves_other_progress() {
        let definition = AdvancementDefinition {
            id: "test:earned".into(),
            criteria: vec!["first".into(), "second".into()],
            requirements: vec![vec!["first".into()], vec!["second".into()]],
            recipes: vec!["minecraft:brewing_stand".into()],
        };
        let first = change(
            &PlayerAdvancementState::default(),
            &definition,
            Some("first"),
            true,
            40,
        )
        .unwrap();
        assert!(!first.progress["test:earned"].completed);
        assert!(first.unlocked_recipes.is_empty());
        let second = change(&first, &definition, Some("second"), true, 41).unwrap();
        assert!(second.progress["test:earned"].completed);
        assert!(second.unlocked_recipes.contains("minecraft:brewing_stand"));
        assert_eq!(second.progress["test:earned"].criteria["first"], Some(40));
        let duplicate = change(&second, &definition, Some("second"), true, 99).unwrap();
        assert_eq!(second, duplicate);
    }
}
