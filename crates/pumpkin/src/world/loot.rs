use pumpkin_data::damage::DamageType;
use pumpkin_data::entity::EntityType;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_data::{Block, BlockState};
use pumpkin_util::loot_table::{LootBonusFormula, LootCondition, LootEntry, LootTable};
use pumpkin_util::random::{RandomGenerator, RandomImpl, xoroshiro128::Xoroshiro};

/// Semantic loot has no client stack ID; those IDs belong to publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LootItemFact {
    pub item: u16,
    pub count: u32,
}

/// Splits semantic counts using owned item policy before reserving spawn IDs.
/// Refuses the whole plan if its declared output capacity is insufficient.
pub fn partition_loot_facts(facts: &[LootItemFact], capacity: usize) -> Option<Vec<LootItemFact>> {
    if capacity > 65_536 {
        return None;
    }
    let mut required = 0usize;
    for fact in facts {
        let item = Item::from_id(fact.item)?;
        let limit = u32::from(ItemStack::static_new_java(1, item).get_max_stack_size());
        if limit == 0 || fact.count == 0 {
            return None;
        }
        required = required.checked_add(usize::try_from(fact.count.div_ceil(limit)).ok()?)?;
        if required > capacity {
            return None;
        }
    }
    let mut result = Vec::with_capacity(required);
    for fact in facts {
        let limit = u32::from(
            ItemStack::static_new_java(1, Item::from_id(fact.item)?).get_max_stack_size(),
        );
        let mut remaining = fact.count;
        while remaining > 0 {
            let count = remaining.min(limit);
            result.push(LootItemFact {
                item: fact.item,
                count,
            });
            remaining -= count;
        }
    }
    Some(result)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockLootFacts {
    pub items: Vec<LootItemFact>,
    pub experience: u32,
}

pub fn block_loot_facts(
    block: &Block,
    loot_seed: i64,
    experience_seed: u64,
    experience: bool,
    params: &LootContextParameters,
) -> BlockLootFacts {
    let key = format!("minecraft:blocks/{}", block.name);
    let items = pumpkin_data::loot_table::get_loot_table(&key).map_or_else(Vec::new, |table| {
        generate_loot_facts_with_context(table, loot_seed, params)
    });
    let experience = block_experience_fact(block, experience_seed, experience, params);
    BlockLootFacts { items, experience }
}

/// The XP leg uses the same frozen tool and seed as its enclosing break.
pub fn block_experience_fact(
    block: &Block,
    seed: u64,
    enabled: bool,
    params: &LootContextParameters,
) -> u32 {
    let silk_touch = params.tool.as_ref().is_some_and(|tool| {
        pumpkin_data::Enchantment::from_name("silk_touch")
            .is_some_and(|enchantment| tool.get_enchantment_level(enchantment) > 0)
    });
    if enabled && !silk_touch {
        block.experience.as_ref().map_or(0, |value| {
            let mut random = RandomGenerator::Xoroshiro(Xoroshiro::from_seed(seed));
            value.experience.get(&mut random).max(0) as u32
        })
    } else {
        0
    }
}

#[derive(Default, Clone)]
pub struct LootContextParameters {
    pub explosion_radius: Option<f32>,
    pub block_state: Option<&'static BlockState>,
    pub killed_by_player: Option<bool>,
    pub luck: f32,
    pub this_entity: Option<&'static EntityType>,
    pub killer_entity: Option<&'static EntityType>,
    pub direct_killer_entity: Option<&'static EntityType>,
    pub position: Option<pumpkin_util::math::vector3::Vector3<f64>>,
    pub world_time: u64,
    pub damage_type: Option<DamageType>,
    pub tool: Option<ItemStack>,
    pub is_raining: Option<bool>,
    pub is_thundering: Option<bool>,
    /// Whether the killed entity was on fire at death time.
    /// Computed from `Entity.fire_ticks > 0`.
    pub is_on_fire: Option<bool>,
}

fn check_condition(
    cond: LootCondition,
    has_silk_touch: bool,
    has_shears: bool,
    fortune_level: i32,
    params: &LootContextParameters,
    rng: &mut Xoroshiro,
) -> bool {
    match cond {
        LootCondition::None => true,
        LootCondition::SilkTouch => has_silk_touch,
        LootCondition::NoSilkTouch => !has_silk_touch,
        LootCondition::Shears => has_shears,
        LootCondition::SilkTouchOrShears => has_silk_touch || has_shears,
        LootCondition::NoSilkTouchOrShears => !has_silk_touch && !has_shears,
        LootCondition::KilledByPlayer => params.killed_by_player.unwrap_or(false),
        LootCondition::SurvivesExplosion => params
            .explosion_radius
            .is_none_or(|radius| rng.next_f32() <= 1.0 / radius),
        LootCondition::RandomChance { chance } => rng.next_f32() < chance,
        LootCondition::RandomChanceWithEnchantedBonus {
            unenchanted_chance,
            enchanted_chance_base,
            enchanted_chance_per_level_above_first,
        } => {
            let chance = if fortune_level > 0 {
                enchanted_chance_base
                    + enchanted_chance_per_level_above_first * (fortune_level - 1) as f32
            } else {
                unenchanted_chance
            };
            rng.next_f32() < chance
        }
        LootCondition::TableBonus { chances } => {
            let index = (fortune_level.max(0) as usize).min(chances.len().saturating_sub(1));
            chances
                .get(index)
                .is_some_and(|chance| rng.next_f32() < *chance)
        }
        LootCondition::AllOf(conditions) => conditions
            .iter()
            .all(|c| check_condition(*c, has_silk_touch, has_shears, fortune_level, params, rng)),
    }
}

fn apply_bonus_formula(
    base_count: i32,
    bonus: LootBonusFormula,
    fortune_level: i32,
    rng: &mut Xoroshiro,
) -> i32 {
    match bonus {
        LootBonusFormula::OreDrops => {
            if fortune_level > 0 {
                let bonus = (rng.next_bounded_i32(fortune_level + 2) - 1).max(0);
                base_count * (bonus + 1)
            } else {
                base_count
            }
        }
        LootBonusFormula::UniformBonusCount(bonus_multiplier) => {
            let max_bonus = fortune_level * bonus_multiplier;
            let extra = if max_bonus > 0 {
                rng.next_bounded_i32(max_bonus + 1)
            } else {
                0
            };
            base_count + extra
        }
        LootBonusFormula::BinomialWithBonusCount { extra, probability } => {
            let n = fortune_level + extra;
            let mut bonus_count = 0;
            for _ in 0..n {
                if rng.next_f32() < probability {
                    bonus_count += 1;
                }
            }
            base_count + bonus_count
        }
    }
}

#[must_use]
pub fn generate_loot(table: &LootTable, seed: i64) -> Vec<ItemStack> {
    generate_loot_with_context(table, seed, &LootContextParameters::default())
}

#[must_use]
pub fn generate_loot_with_context(
    table: &LootTable,
    seed: i64,
    params: &LootContextParameters,
) -> Vec<ItemStack> {
    generate_loot_facts_with_context(table, seed, params)
        .into_iter()
        .map(|fact| {
            ItemStack::new(
                fact.count as u8,
                Item::from_id(fact.item).expect("generated loot item exists"),
            )
        })
        .collect()
}

/// Pure table evaluation, shared by the mutable adapter and owner interpreter.
/// It does not allocate global Bedrock stack IDs or inspect World.
pub fn generate_loot_facts_with_context(
    table: &LootTable,
    seed: i64,
    params: &LootContextParameters,
) -> Vec<LootItemFact> {
    let mut rng = Xoroshiro::from_seed(seed as u64);
    let mut items_to_place = Vec::new();

    let has_silk_touch = params.tool.as_ref().is_some_and(|tool| {
        pumpkin_data::Enchantment::from_name("silk_touch")
            .is_some_and(|e| tool.get_enchantment_level(e) > 0)
    });

    let has_shears = params.tool.as_ref().is_some_and(|tool| {
        let name = tool
            .item
            .registry_key
            .strip_prefix("minecraft:")
            .unwrap_or(tool.item.registry_key);
        name == "shears"
    });

    let fortune_level = params.tool.as_ref().map_or(0, |tool| {
        let fortune = pumpkin_data::Enchantment::from_name("fortune")
            .map_or(0, |e| tool.get_enchantment_level(e));
        let looting = pumpkin_data::Enchantment::from_name("looting")
            .map_or(0, |e| tool.get_enchantment_level(e));
        fortune.max(looting)
    });

    for pool in table.pools {
        if !check_condition(
            pool.condition,
            has_silk_touch,
            has_shears,
            fortune_level,
            params,
            &mut rng,
        ) {
            continue;
        }

        let eligible_entries: Vec<&LootEntry> = pool
            .entries
            .iter()
            .filter(|e| {
                check_condition(
                    e.condition,
                    has_silk_touch,
                    has_shears,
                    fortune_level,
                    params,
                    &mut rng,
                )
            })
            .collect();

        if eligible_entries.is_empty() && pool.empty_weight == 0 {
            continue;
        }

        let range = pool.max_rolls - pool.min_rolls;
        let rolls = pool.min_rolls
            + if range > 0 {
                rng.next_bounded_i32(range + 1)
            } else {
                0
            };

        for _ in 0..rolls {
            let entry_weight: i32 = eligible_entries.iter().map(|e| e.weight).sum();
            let total_weight = entry_weight + pool.empty_weight;
            if total_weight == 0 {
                continue;
            }

            let mut pick = rng.next_bounded_i32(total_weight);

            pick -= pool.empty_weight;
            if pick < 0 {
                continue;
            }

            for entry in &eligible_entries {
                pick -= entry.weight;
                if pick < 0 {
                    let count_range = entry.max_count - entry.min_count;
                    let base_count = entry.min_count
                        + if count_range > 0 {
                            rng.next_bounded_i32(count_range + 1)
                        } else {
                            0
                        };

                    let mut final_count = base_count;
                    if let Some(bonus) = entry.bonus_formula {
                        final_count =
                            apply_bonus_formula(final_count, bonus, fortune_level, &mut rng);
                    }

                    if final_count > 0 {
                        let item_key = entry.item.strip_prefix("minecraft:").unwrap_or(entry.item);

                        if let Some(item) = Item::from_registry_key(item_key) {
                            items_to_place.push(LootItemFact {
                                count: final_count as u32,
                                item: item.id,
                            });
                        }
                    }
                    break;
                }
            }
        }
    }

    items_to_place
}

pub use generate_loot as generate_chest_loot;

pub fn fill_chest_inventory(
    inventory: &std::sync::Arc<dyn pumpkin_inventory::Inventory>,
    table: &LootTable,
    seed: i64,
) {
    let mut items_to_place = generate_loot(table, seed);

    if items_to_place.is_empty() {
        return;
    }

    let inv_size = inventory.size();
    let mut rng = Xoroshiro::from_seed(seed as u64);

    let mut available_slots: Vec<usize> = (0..inv_size)
        .filter(|&slot| inventory.get_stack(slot).is_empty())
        .collect();

    for i in (1..available_slots.len()).rev() {
        let j = rng.next_bounded_i32((i + 1) as i32) as usize;
        available_slots.swap(i, j);
    }

    shuffle_and_split_items(&mut items_to_place, available_slots.len(), &mut rng);

    for item in items_to_place {
        let Some(slot) = available_slots.pop() else {
            tracing::warn!("Tried to over-fill a container");
            return;
        };
        inventory.set_stack(slot, item);
    }
}

fn shuffle_and_split_items(
    result: &mut Vec<ItemStack>,
    available_slots: usize,
    rng: &mut Xoroshiro,
) {
    let mut splittable: Vec<ItemStack> = Vec::new();
    let mut i = 0;
    while i < result.len() {
        if result[i].is_empty() {
            result.swap_remove(i);
        } else if result[i].item_count > 1 {
            splittable.push(result.swap_remove(i));
        } else {
            i += 1;
        }
    }

    while available_slots > result.len() + splittable.len() && !splittable.is_empty() {
        let idx = rng.next_bounded_i32(splittable.len() as i32) as usize;
        let mut stack = splittable.swap_remove(idx);

        let count = stack.item_count as i32;
        let split_off = 1 + rng.next_bounded_i32(count / 2);
        stack.item_count = (count - split_off) as u8;
        let mut copy = stack.clone();
        copy.item_count = split_off as u8;

        if stack.item_count > 1 && rng.next_bool() {
            splittable.push(stack);
        } else {
            result.push(stack);
        }
        if copy.item_count > 1 && rng.next_bool() {
            splittable.push(copy);
        } else {
            result.push(copy);
        }
    }

    result.extend(splittable);

    let n = result.len();
    for i in (1..n).rev() {
        let j = rng.next_bounded_i32((i + 1) as i32) as usize;
        result.swap(i, j);
    }
}

#[cfg(test)]
mod owned_loot_tests {
    use super::*;

    #[test]
    fn frozen_silk_touch_break_preserves_ore_and_suppresses_xp() {
        let mut tool = ItemStack::static_new_java(1, &Item::DIAMOND_PICKAXE);
        tool.set_data_component(pumpkin_data::data_component_impl::EnchantmentsImpl {
            enchantment: std::borrow::Cow::Owned(vec![(
                pumpkin_data::Enchantment::from_name("silk_touch").unwrap(),
                1,
            )]),
        });
        let params = LootContextParameters {
            tool: Some(tool),
            block_state: Some(Block::DIAMOND_ORE.default_state),
            ..Default::default()
        };
        let result = block_loot_facts(&Block::DIAMOND_ORE, 17, 29, true, &params);
        assert_eq!(result.experience, 0);
        assert_eq!(
            result.items,
            vec![LootItemFact {
                item: Item::DIAMOND_ORE.id,
                count: 1
            }]
        );
        let normal = LootContextParameters {
            tool: None,
            ..params
        };
        let result = block_loot_facts(&Block::DIAMOND_ORE, 17, 29, true, &normal);
        assert_eq!(
            result.items,
            vec![LootItemFact {
                item: Item::DIAMOND.id,
                count: 1
            }]
        );
        assert!((3..=7).contains(&result.experience));
        assert_eq!(
            block_loot_facts(&Block::DIAMOND_ORE, 17, 29, false, &normal).experience,
            0
        );
    }

    #[test]
    fn block_loot_facts_are_deterministic_and_match_legacy_table_evaluation() {
        for block in [&Block::DIRT, &Block::WHITE_CARPET, &Block::COAL_ORE] {
            let params = LootContextParameters {
                block_state: Some(block.default_state),
                ..Default::default()
            };
            let first = block_loot_facts(block, 17, 29, true, &params);
            assert_eq!(first, block_loot_facts(block, 17, 29, true, &params));
            let key = format!("minecraft:blocks/{}", block.name);
            let legacy = pumpkin_data::loot_table::get_loot_table(&key)
                .map_or_else(Vec::new, |table| {
                    generate_loot_with_context(table, 17, &params)
                });
            assert_eq!(
                first.items,
                legacy
                    .iter()
                    .map(|stack| LootItemFact {
                        item: stack.item.id,
                        count: u32::from(stack.item_count)
                    })
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn owned_partition_conserves_large_counts_and_uses_item_capacity() {
        let facts = [
            LootItemFact {
                item: Item::STONE.id,
                count: 257,
            },
            LootItemFact {
                item: Item::IRON_SWORD.id,
                count: 2,
            },
        ];
        assert!(partition_loot_facts(&facts, 6).is_none());
        let result = partition_loot_facts(&facts, 7).unwrap();
        assert_eq!(result.len(), 7);
        assert_eq!(
            result
                .iter()
                .filter(|fact| fact.item == Item::STONE.id)
                .map(|fact| fact.count)
                .sum::<u32>(),
            257
        );
        assert!(
            result
                .iter()
                .filter(|fact| fact.item == Item::STONE.id)
                .all(|fact| fact.count <= 64)
        );
        assert!(
            result
                .iter()
                .filter(|fact| fact.item == Item::IRON_SWORD.id)
                .all(|fact| fact.count == 1)
        );
        assert!(
            partition_loot_facts(
                &[LootItemFact {
                    item: Item::STONE.id,
                    count: 0
                }],
                8
            )
            .is_none()
        );
    }
}
