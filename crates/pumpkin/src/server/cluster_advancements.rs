use std::collections::BTreeMap;
use std::path::Path;

use pumpkin_cluster::player_advancement::{AdvancementProgress, PlayerAdvancementState};
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_nbt::tag::NbtTag;

const KEY: &str = "PumpkinClusterAdvancements";
const MAX_BYTES: usize = 900_000;

pub fn outcome(
    before: &PlayerAdvancementState,
    event: &pumpkin_cluster::player_advancement::AdvancementEvent,
    achieved_millis: u64,
) -> Option<pumpkin_cluster::player_advancement::AdvancementOutcome> {
    use pumpkin_cluster::player_advancement::AdvancementEvent as Event;
    use pumpkin_data::{Advancement as A, dimension::Dimension, item::Item};
    if !before.valid() || !event.valid() {
        return None;
    }
    let mut state = before.clone();
    let mut experience = 0_u32;
    let mut grant = |advancement: &pumpkin_data::Advancement, criterion: &str| -> Option<()> {
        if !advancement.criteria.contains(&criterion) {
            return Some(());
        }
        let id = advancement.id.to_string();
        if state
            .progress
            .get(&id)
            .and_then(|progress| progress.criteria.get(criterion))
            .is_some_and(Option::is_some)
        {
            return Some(());
        }
        let records_completed = match event {
            Event::PlayerKilledEntity { .. } => {
                advancement == A::ADVENTURE_KILL_A_MOB || advancement == A::ADVENTURE_KILL_ALL_MOBS
            }
            Event::ConsumeItem { .. } => advancement == A::HUSBANDRY_BALANCED_DIET,
            Event::BredAnimal { .. } => true,
            _ => false,
        };
        if before
            .progress
            .get(&id)
            .is_some_and(|progress| progress.completed)
            && !records_completed
        {
            return Some(());
        }
        let previously_completed = state
            .progress
            .get(&id)
            .is_some_and(|progress| progress.completed);
        let definition = definition(&id)?;
        let next = pumpkin_cluster::player_advancement::change(
            &state,
            &definition,
            Some(criterion),
            true,
            achieved_millis,
        )?;
        if !previously_completed
            && next
                .progress
                .get(&id)
                .is_some_and(|progress| progress.completed)
        {
            experience =
                experience.checked_add(u32::try_from(advancement.reward.experience).ok()?)?;
        }
        state = next;
        Some(())
    };
    match event {
        Event::InventoryChanged {
            items,
            enchanted,
            world,
            biome,
        } => {
            if !valid_dimension(world.dimension)
                || items.iter().any(|id| Item::from_id(*id).is_none())
            {
                return None;
            }
            let biome = match biome {
                Some(id) => Some(pumpkin_data::biome::Biome::from_id(*id)?),
                None => None,
            };
            let has = |item: &Item| items.binary_search(&item.id).is_ok();
            for (item, advancement, criterion) in [
                (&Item::CRAFTING_TABLE, A::STORY_ROOT, "crafting_table"),
                (
                    &Item::STONE_PICKAXE,
                    A::STORY_UPGRADE_TOOLS,
                    "stone_pickaxe",
                ),
                (&Item::IRON_INGOT, A::STORY_SMELT_IRON, "iron"),
                (&Item::IRON_PICKAXE, A::STORY_IRON_TOOLS, "iron_pickaxe"),
                (&Item::DIAMOND, A::STORY_MINE_DIAMOND, "diamond"),
                (&Item::LAVA_BUCKET, A::STORY_LAVA_BUCKET, "lava_bucket"),
                (&Item::IRON_HELMET, A::STORY_OBTAIN_ARMOR, "iron_helmet"),
                (
                    &Item::IRON_CHESTPLATE,
                    A::STORY_OBTAIN_ARMOR,
                    "iron_chestplate",
                ),
                (&Item::IRON_LEGGINGS, A::STORY_OBTAIN_ARMOR, "iron_leggings"),
                (&Item::IRON_BOOTS, A::STORY_OBTAIN_ARMOR, "iron_boots"),
                (&Item::DIAMOND_HELMET, A::STORY_SHINY_GEAR, "diamond_helmet"),
                (
                    &Item::DIAMOND_CHESTPLATE,
                    A::STORY_SHINY_GEAR,
                    "diamond_chestplate",
                ),
                (
                    &Item::DIAMOND_LEGGINGS,
                    A::STORY_SHINY_GEAR,
                    "diamond_leggings",
                ),
                (&Item::DIAMOND_BOOTS, A::STORY_SHINY_GEAR, "diamond_boots"),
                (&Item::OBSIDIAN, A::STORY_FORM_OBSIDIAN, "obsidian"),
                (
                    &Item::WITHER_SKELETON_SKULL,
                    A::NETHER_GET_WITHER_SKULL,
                    "wither_skull",
                ),
                (
                    &Item::ANCIENT_DEBRIS,
                    A::NETHER_OBTAIN_ANCIENT_DEBRIS,
                    "ancient_debris",
                ),
                (&Item::BLAZE_ROD, A::NETHER_OBTAIN_BLAZE_ROD, "blaze_rod"),
                (
                    &Item::CRYING_OBSIDIAN,
                    A::NETHER_OBTAIN_CRYING_OBSIDIAN,
                    "crying_obsidian",
                ),
                (
                    &Item::COD_BUCKET,
                    A::HUSBANDRY_TACTICAL_FISHING,
                    "cod_bucket",
                ),
                (
                    &Item::SALMON_BUCKET,
                    A::HUSBANDRY_TACTICAL_FISHING,
                    "salmon_bucket",
                ),
                (
                    &Item::PUFFERFISH_BUCKET,
                    A::HUSBANDRY_TACTICAL_FISHING,
                    "pufferfish_bucket",
                ),
                (
                    &Item::TROPICAL_FISH_BUCKET,
                    A::HUSBANDRY_TACTICAL_FISHING,
                    "tropical_fish_bucket",
                ),
                (
                    &Item::AXOLOTL_BUCKET,
                    A::HUSBANDRY_AXOLOTL_IN_A_BUCKET,
                    "axolotl_bucket",
                ),
                (
                    &Item::TADPOLE_BUCKET,
                    A::HUSBANDRY_TADPOLE_IN_A_BUCKET,
                    "tadpole_bucket",
                ),
                (
                    &Item::NETHERITE_HOE,
                    A::HUSBANDRY_OBTAIN_NETHERITE_HOE,
                    "netherite_hoe",
                ),
                (&Item::BEACON, A::NETHER_CREATE_BEACON, "beacon"),
                (&Item::BEACON, A::NETHER_CREATE_FULL_BEACON, "beacon"),
                (&Item::ELYTRA, A::END_ELYTRA, "elytra"),
                (&Item::DRAGON_EGG, A::END_DRAGON_EGG, "dragon_egg"),
                (&Item::DRAGON_BREATH, A::END_DRAGON_BREATH, "dragon_breath"),
            ] {
                if has(item) {
                    grant(advancement, criterion)?;
                }
            }
            if [
                &Item::COBBLESTONE,
                &Item::STONE,
                &Item::ANDESITE,
                &Item::DIORITE,
                &Item::GRANITE,
                &Item::DEEPSLATE,
                &Item::COBBLED_DEEPSLATE,
                &Item::BLACKSTONE,
            ]
            .iter()
            .any(|item| has(item))
            {
                grant(A::STORY_MINE_STONE, "get_stone")?;
            }
            if [
                &Item::NETHERITE_HELMET,
                &Item::NETHERITE_CHESTPLATE,
                &Item::NETHERITE_LEGGINGS,
                &Item::NETHERITE_BOOTS,
            ]
            .iter()
            .all(|item| has(item))
            {
                grant(A::NETHER_NETHERITE_ARMOR, "netherite_armor")?;
            }
            if *enchanted {
                grant(A::STORY_ENCHANT_ITEM, "enchanted_item")?;
            }
            if [&Item::POTION, &Item::SPLASH_POTION, &Item::LINGERING_POTION]
                .iter()
                .any(|item| has(item))
            {
                grant(A::NETHER_BREW_POTION, "potion")?;
            }
            if [&Item::SHULKER_SHELL, &Item::CHORUS_FRUIT]
                .iter()
                .any(|item| has(item))
            {
                grant(A::END_FIND_END_CITY, "in_city")?;
            }
            if let Some(biome) = biome {
                let criterion = format!("minecraft:{}", biome.registry_id);
                if world.dimension == Dimension::THE_NETHER.id {
                    grant(A::NETHER_EXPLORE_NETHER, &criterion)?;
                }
                if world.dimension == Dimension::OVERWORLD.id
                    || world.dimension == Dimension::OVERWORLD_CAVES.id
                {
                    grant(A::ADVENTURE_ADVENTURING_TIME, &criterion)?;
                }
            }
        }
        Event::PlayerKilledEntity { entity_type } => {
            let entity = pumpkin_data::entity::EntityType::from_raw(*entity_type)?;
            let criterion = format!("minecraft:{}", entity.resource_name);
            grant(A::ADVENTURE_KILL_A_MOB, &criterion)?;
            grant(A::ADVENTURE_KILL_ALL_MOBS, &criterion)?;
            grant(A::ADVENTURE_ROOT, "killed_something")?;
        }
        Event::EnterBlock { block } => {
            pumpkin_data::BlockId::new(*block)?;
        }
        Event::PlacedBlock { block } => {
            let block = pumpkin_data::BlockId::new(*block)?.to_block();
            let name = block.name.strip_prefix("minecraft:").unwrap_or(block.name);
            for criterion in [
                "wheat",
                "pumpkin_stem",
                "melon_stem",
                "beetroots",
                "nether_wart",
                "torchflower",
                "pitcher_pod",
            ] {
                if name == criterion {
                    grant(A::HUSBANDRY_PLANT_SEED, criterion)?;
                }
            }
        }
        Event::ConsumeItem { item } => {
            let item = Item::from_id(*item)?;
            grant(A::HUSBANDRY_ROOT, "consumed_item")?;
            grant(
                A::HUSBANDRY_BALANCED_DIET,
                item.registry_key
                    .strip_prefix("minecraft:")
                    .unwrap_or(item.registry_key),
            )?;
        }
        Event::SleptInBed => grant(A::ADVENTURE_SLEEP_IN_BED, "slept_in_bed")?,
        Event::FishedItem { item } => {
            let item = Item::from_id(*item)?;
            let name = item
                .registry_key
                .strip_prefix("minecraft:")
                .unwrap_or(item.registry_key);
            for criterion in ["cod", "salmon", "pufferfish", "tropical_fish"] {
                if name == criterion {
                    grant(A::HUSBANDRY_FISHY_BUSINESS, criterion)?;
                }
            }
        }
        Event::EnterDimension { dimension } => {
            if !valid_dimension(*dimension) {
                return None;
            }
            if *dimension == Dimension::THE_NETHER.id {
                grant(A::STORY_ENTER_THE_NETHER, "entered_nether")?;
                grant(A::NETHER_ROOT, "entered_nether")?;
            }
            if *dimension == Dimension::THE_END.id {
                grant(A::STORY_ENTER_THE_END, "entered_end")?;
                grant(A::END_ROOT, "entered_end")?;
            }
        }
        Event::PlayerKilled => grant(A::ADVENTURE_ROOT, "killed_by_something")?,
        Event::DeflectedDamage => grant(A::STORY_DEFLECT_ARROW, "deflected_projectile")?,
        Event::LaunchedEyeOfEnder => grant(A::STORY_FOLLOW_ENDER_EYE, "in_stronghold")?,
        Event::GlowedSign => grant(A::HUSBANDRY_MAKE_A_SIGN_GLOW, "make_a_sign_glow")?,
        Event::BredAnimal { parent_type } => {
            let parent = pumpkin_data::entity::EntityType::from_raw(*parent_type)?;
            grant(A::HUSBANDRY_BREED_AN_ANIMAL, "bred")?;
            grant(
                A::HUSBANDRY_BRED_ALL_ANIMALS,
                &format!("minecraft:{}", parent.resource_name),
            )?;
        }
        Event::DealtOverkillDamage => grant(A::ADVENTURE_OVEROVERKILL, "overoverkill")?,
        Event::SniperDuel => grant(A::ADVENTURE_SNIPER_DUEL, "killed_skeleton")?,
        Event::TwoBirdsOneArrow => grant(A::ADVENTURE_TWO_BIRDS_ONE_ARROW, "two_birds")?,
        Event::Arbalistic => grant(A::ADVENTURE_ARBALISTIC, "arbalistic")?,
        Event::Bullseye => grant(A::ADVENTURE_BULLSEYE, "bullseye")?,
        Event::CuredZombieVillager => grant(A::STORY_CURE_ZOMBIE_VILLAGER, "cured_zombie")?,
        Event::TradedWithVillager { block_y } => {
            grant(A::ADVENTURE_TRADE, "traded")?;
            if *block_y >= 319 {
                grant(A::ADVENTURE_TRADE_AT_WORLD_HEIGHT, "trade_at_world_height")?;
            }
        }
    }
    Some(pumpkin_cluster::player_advancement::AdvancementOutcome { state, experience })
}

fn valid_dimension(dimension: u8) -> bool {
    use pumpkin_data::dimension::Dimension;
    [
        Dimension::OVERWORLD.id,
        Dimension::OVERWORLD_CAVES.id,
        Dimension::THE_END.id,
        Dimension::THE_NETHER.id,
    ]
    .contains(&dimension)
}

pub fn definition(id: &str) -> Option<pumpkin_cluster::player_advancement::AdvancementDefinition> {
    let advancement = pumpkin_data::Advancement::from_minecraft_name(id)
        .or_else(|| pumpkin_data::Advancement::from_name(id))?;
    Some(pumpkin_cluster::player_advancement::AdvancementDefinition {
        id: advancement.id.to_string(),
        criteria: advancement
            .criteria
            .iter()
            .map(|name| (*name).to_owned())
            .collect(),
        requirements: advancement
            .requirements
            .iter()
            .map(|group| group.iter().map(|name| (*name).to_owned()).collect())
            .collect(),
        recipes: advancement
            .reward
            .recipes
            .iter()
            .map(ToString::to_string)
            .collect(),
    })
}

pub fn read_nbt(data: &NbtCompound) -> Option<PlayerAdvancementState> {
    let bytes = data.get_byte_array(KEY)?;
    if bytes.len() > MAX_BYTES {
        return None;
    }
    let bytes = bytes.iter().map(|value| *value as u8).collect::<Vec<_>>();
    let state: PlayerAdvancementState = postcard::from_bytes(&bytes).ok()?;
    state.valid().then_some(state)
}

pub fn write_nbt(data: &mut NbtCompound, state: &PlayerAdvancementState) -> bool {
    if !state.valid() {
        return false;
    }
    let Ok(bytes) = postcard::to_allocvec(state) else {
        return false;
    };
    if bytes.len() > MAX_BYTES {
        return false;
    }
    data.put(
        KEY,
        NbtTag::ByteArray(
            bytes
                .into_iter()
                .map(|value| value as i8)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
    );
    let mut book = data.get_compound("recipeBook").cloned().unwrap_or_default();
    book.put_list(
        "recipes",
        state
            .unlocked_recipes
            .iter()
            .cloned()
            .map(|id| NbtTag::String(id.into_boxed_str()))
            .collect(),
    );
    data.put("recipeBook", NbtTag::Compound(book));
    true
}

pub fn load_primary_player(
    path: &Path,
    data: &NbtCompound,
) -> Result<PlayerAdvancementState, String> {
    let mut state = load_primary(path)?;
    if let Some(book) = data.get_compound("recipeBook") {
        if let Some(recipes) = book.get_list("recipes") {
            for recipe in recipes {
                let NbtTag::String(id) = recipe else {
                    return Err("invalid recipe book entry".to_owned());
                };
                state.unlocked_recipes.insert(id.to_string());
            }
        }
    }
    state
        .valid()
        .then_some(state)
        .ok_or_else(|| "invalid recipe book import".to_owned())
}

pub fn load_primary(path: &Path) -> Result<PlayerAdvancementState, String> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(error) => return Err(error.to_string()),
    };
    if metadata.len() > MAX_BYTES as u64 {
        return Err("advancement import exceeds payload budget".to_owned());
    }
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let values: BTreeMap<String, crate::entity::player::advancement::AdvancementProgress> =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let mut state = PlayerAdvancementState::default();
    for (id, mut value) in values {
        let Some(advancement) = pumpkin_data::Advancement::from_minecraft_name(&id) else {
            return Err(format!("unknown advancement {id}"));
        };
        value.update(
            pumpkin_data::advancement_data::AdvancementRequirement::from_const(
                advancement.requirements,
            ),
        );
        let completed = value.is_done();
        let mut criteria = BTreeMap::new();
        for (name, progress) in value.criteria {
            let timestamp = match progress.0 {
                Some(time) => Some(
                    u64::try_from(
                        time.duration_since(std::time::UNIX_EPOCH)
                            .map_err(|error| error.to_string())?
                            .as_millis(),
                    )
                    .map_err(|error| error.to_string())?,
                ),
                None => None,
            };
            criteria.insert(name.to_string(), timestamp);
        }
        if completed {
            state
                .unlocked_recipes
                .extend(advancement.reward.recipes.iter().map(ToString::to_string));
        }
        state.progress.insert(
            advancement.id.to_string(),
            AdvancementProgress {
                criteria,
                completed,
            },
        );
    }
    state
        .valid()
        .then_some(state)
        .ok_or_else(|| "invalid advancement import".to_owned())
}

pub fn recipe_displays(
    recipes: &[pumpkin_protocol::codec::recipe::DynamicRecipe],
) -> BTreeMap<String, std::collections::BTreeSet<i32>> {
    use pumpkin_data::recipes::{
        CookingRecipeType, CraftingRecipeTypes, RECIPES_COOKING, RECIPES_CRAFTING,
        RECIPES_CRAFTING_IDS,
    };
    let mut result = BTreeMap::<String, std::collections::BTreeSet<i32>>::new();
    let mut display = 0_i32;
    for (id, recipe) in RECIPES_CRAFTING_IDS.iter().zip(RECIPES_CRAFTING) {
        if matches!(
            recipe,
            CraftingRecipeTypes::CraftingSpecial | CraftingRecipeTypes::CraftingDecoratedPot { .. }
        ) {
            continue;
        }
        result.entry((*id).to_owned()).or_default().insert(display);
        display += 1;
    }
    for recipe in RECIPES_COOKING {
        let id = match recipe {
            CookingRecipeType::Smelting(value)
            | CookingRecipeType::Blasting(value)
            | CookingRecipeType::Smoking(value)
            | CookingRecipeType::CampfireCooking(value) => value.recipe_id,
        };
        result.entry(id.to_owned()).or_default().insert(display);
        display += 1;
    }
    for recipe in recipes {
        if matches!(
            recipe,
            pumpkin_protocol::codec::recipe::DynamicRecipe::Brewing(_)
        ) {
            continue;
        }
        result
            .entry(crate::command::commands::recipe::get_recipe_id(recipe))
            .or_default()
            .insert(display);
        display += 1;
    }
    result
}

pub fn publish_delta(
    server: &super::Server,
    player: &crate::entity::player::Player,
    before: &PlayerAdvancementState,
    after: &PlayerAdvancementState,
    reset: bool,
) -> bool {
    use pumpkin_data::advancement_data::{AdvancementProgressData, Criteria};
    use pumpkin_protocol::java::client::play::{CRecipeBookSelection, CUpdateAdvancements};
    let crate::net::ClientPlatform::Java(client) = player.client.as_ref() else {
        return true;
    };
    if before == after && !reset {
        return true;
    }
    let mut definitions = BTreeMap::new();
    let mut progress = Vec::new();
    for (id, state) in &after.progress {
        let Some(advancement) = pumpkin_data::Advancement::from_minecraft_name(id) else {
            return false;
        };
        let mut ancestor = Some(advancement);
        while let Some(value) = ancestor {
            definitions.insert(value.id.to_string(), value);
            ancestor = value
                .parent
                .as_ref()
                .and_then(|id| pumpkin_data::Advancement::from_minecraft_name(&id.to_string()));
        }
        progress.push(AdvancementProgressData {
            id: advancement.id.clone(),
            progress: state
                .criteria
                .iter()
                .map(|(name, achieved)| Criteria {
                    criterion_id: name.clone().into(),
                    achieve_date: achieved.map(|time| i64::try_from(time).unwrap_or(i64::MAX)),
                })
                .collect(),
        });
    }
    let removed = before
        .progress
        .keys()
        .filter(|id| !after.progress.contains_key(*id))
        .filter_map(|id| {
            pumpkin_data::Advancement::from_minecraft_name(id).map(|value| value.id.clone())
        })
        .collect();
    let packet = CUpdateAdvancements::new(
        reset,
        definitions.into_values().collect(),
        progress,
        removed,
        true,
    );
    let Ok(bytes) = client.serialize_packet(&packet) else {
        return false;
    };
    client.try_enqueue_packet(bytes);
    if reset || before.unlocked_recipes != after.unlocked_recipes {
        let recipes = server.recipe_manager.cluster_dynamic_recipes_snapshot();
        let displays = recipe_displays(&recipes);
        let display_ids = after
            .unlocked_recipes
            .iter()
            .filter_map(|id| displays.get(id))
            .flat_map(|ids| ids.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        let packet = CRecipeBookSelection {
            replace: true,
            dynamic_recipes: &recipes,
            display_ids: &display_ids,
        };
        let Ok(bytes) = client.serialize_packet(&packet) else {
            return false;
        };
        client.try_enqueue_packet(bytes);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_recipe_reward_and_recipe_book_ids_match() {
        assert_eq!(
            pumpkin_data::recipes::RECIPES_CRAFTING_IDS.len(),
            pumpkin_data::recipes::RECIPES_CRAFTING.len()
        );
        let definition = definition("minecraft:recipes/brewing/brewing_stand").unwrap();
        assert_eq!(definition.recipes, vec!["minecraft:brewing_stand"]);
        let displays = recipe_displays(&[]);
        assert!(displays.contains_key("minecraft:brewing_stand"));
    }

    #[test]
    fn primary_recipe_book_import_preserves_existing_unlocks_and_export() {
        let root =
            std::env::temp_dir().join(format!("pumpkin-advancement-{}", uuid::Uuid::new_v4()));
        let mut book = NbtCompound::new();
        book.put_list(
            "recipes",
            vec![NbtTag::String("minecraft:oak_planks".into())],
        );
        let mut data = NbtCompound::new();
        data.put("recipeBook", NbtTag::Compound(book));
        let state = load_primary_player(&root, &data).unwrap();
        assert!(state.unlocked_recipes.contains("minecraft:oak_planks"));
        assert!(write_nbt(&mut data, &state));
        assert_eq!(read_nbt(&data).unwrap(), state);
        assert_eq!(
            data.get_compound("recipeBook")
                .unwrap()
                .get_list("recipes")
                .unwrap(),
            &[NbtTag::String("minecraft:oak_planks".into())]
        );
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use pumpkin_cluster::player_advancement::AdvancementEvent as Event;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_data::{Advancement as A, biome::Biome, dimension::Dimension, item::Item};

    fn achieved(
        state: &PlayerAdvancementState,
        advancement: &pumpkin_data::Advancement,
        criterion: &str,
    ) -> bool {
        state
            .progress
            .get(&advancement.id.to_string())
            .and_then(|progress| progress.criteria.get(criterion))
            .is_some_and(Option::is_some)
    }

    #[test]
    fn unavailable_biome_does_not_block_inventory_or_enchantment_criteria() {
        let event = Event::InventoryChanged {
            items: vec![Item::CRAFTING_TABLE.id],
            enchanted: true,
            world: WorldId::new(Dimension::OVERWORLD.id, 77),
            biome: None,
        };
        let result = outcome(&PlayerAdvancementState::default(), &event, 42).unwrap();
        assert!(achieved(&result.state, A::STORY_ROOT, "crafting_table"));
        assert!(achieved(
            &result.state,
            A::STORY_ENCHANT_ITEM,
            "enchanted_item"
        ));
        assert!(
            !result
                .state
                .progress
                .contains_key(&A::ADVENTURE_ADVENTURING_TIME.id.to_string())
        );
        assert!(result.valid());
    }

    #[test]
    fn exploration_requires_matching_world_and_registered_biome_criterion() {
        let event = |dimension| Event::InventoryChanged {
            items: vec![],
            enchanted: false,
            world: WorldId::new(dimension, 77),
            biome: Some(Biome::NETHER_WASTES.id),
        };
        let wrong_world = outcome(
            &PlayerAdvancementState::default(),
            &event(Dimension::OVERWORLD.id),
            42,
        )
        .unwrap();
        assert!(wrong_world.state.progress.is_empty());
        let right_world = outcome(
            &PlayerAdvancementState::default(),
            &event(Dimension::THE_NETHER.id),
            42,
        )
        .unwrap();
        assert!(achieved(
            &right_world.state,
            A::NETHER_EXPLORE_NETHER,
            "minecraft:nether_wastes"
        ));
    }

    #[test]
    fn advancement_experience_is_only_new_vanilla_completion_reward() {
        let first = outcome(&PlayerAdvancementState::default(), &Event::SniperDuel, 42).unwrap();
        assert!(first.experience > 0);
        assert_eq!(
            first.experience,
            A::ADVENTURE_SNIPER_DUEL.reward.experience as u32
        );
        let replay = outcome(&first.state, &Event::SniperDuel, 99).unwrap();
        assert_eq!(replay.experience, 0);
        assert_eq!(replay.state, first.state);
    }

    #[test]
    fn typed_family_events_keep_legacy_registered_criterion_mapping() {
        use pumpkin_data::{Block, entity::EntityType};
        for (event, advancement, criterion) in [
            (
                Event::PlayerKilledEntity {
                    entity_type: EntityType::ZOMBIE.id,
                },
                A::ADVENTURE_KILL_A_MOB,
                "minecraft:zombie",
            ),
            (
                Event::PlacedBlock {
                    block: Block::WHEAT.id.as_u16(),
                },
                A::HUSBANDRY_PLANT_SEED,
                "wheat",
            ),
            (
                Event::ConsumeItem {
                    item: Item::APPLE.id,
                },
                A::HUSBANDRY_BALANCED_DIET,
                "apple",
            ),
            (Event::SleptInBed, A::ADVENTURE_SLEEP_IN_BED, "slept_in_bed"),
            (
                Event::FishedItem { item: Item::COD.id },
                A::HUSBANDRY_FISHY_BUSINESS,
                "cod",
            ),
            (
                Event::EnterDimension {
                    dimension: Dimension::THE_NETHER.id,
                },
                A::STORY_ENTER_THE_NETHER,
                "entered_nether",
            ),
            (
                Event::PlayerKilled,
                A::ADVENTURE_ROOT,
                "killed_by_something",
            ),
            (
                Event::DeflectedDamage,
                A::STORY_DEFLECT_ARROW,
                "deflected_projectile",
            ),
            (
                Event::LaunchedEyeOfEnder,
                A::STORY_FOLLOW_ENDER_EYE,
                "in_stronghold",
            ),
            (
                Event::GlowedSign,
                A::HUSBANDRY_MAKE_A_SIGN_GLOW,
                "make_a_sign_glow",
            ),
            (
                Event::BredAnimal {
                    parent_type: EntityType::COW.id,
                },
                A::HUSBANDRY_BRED_ALL_ANIMALS,
                "minecraft:cow",
            ),
            (
                Event::DealtOverkillDamage,
                A::ADVENTURE_OVEROVERKILL,
                "overoverkill",
            ),
            (
                Event::SniperDuel,
                A::ADVENTURE_SNIPER_DUEL,
                "killed_skeleton",
            ),
            (
                Event::TwoBirdsOneArrow,
                A::ADVENTURE_TWO_BIRDS_ONE_ARROW,
                "two_birds",
            ),
            (Event::Arbalistic, A::ADVENTURE_ARBALISTIC, "arbalistic"),
            (Event::Bullseye, A::ADVENTURE_BULLSEYE, "bullseye"),
            (
                Event::CuredZombieVillager,
                A::STORY_CURE_ZOMBIE_VILLAGER,
                "cured_zombie",
            ),
            (
                Event::TradedWithVillager { block_y: 318 },
                A::ADVENTURE_TRADE,
                "traded",
            ),
        ] {
            let result = outcome(&PlayerAdvancementState::default(), &event, 42).unwrap();
            assert!(
                achieved(&result.state, advancement, criterion),
                "{} {criterion}",
                advancement.id
            );
        }
        let low = outcome(
            &PlayerAdvancementState::default(),
            &Event::TradedWithVillager { block_y: 318 },
            42,
        )
        .unwrap();
        assert!(!achieved(
            &low.state,
            A::ADVENTURE_TRADE_AT_WORLD_HEIGHT,
            "trade_at_world_height"
        ));
        let high = outcome(
            &PlayerAdvancementState::default(),
            &Event::TradedWithVillager { block_y: 319 },
            42,
        )
        .unwrap();
        assert!(achieved(
            &high.state,
            A::ADVENTURE_TRADE_AT_WORLD_HEIGHT,
            "trade_at_world_height"
        ));
    }

    #[test]
    fn completion_guards_match_legacy_per_event_criterion_behavior() {
        let inventory = |mut items: Vec<u16>| {
            items.sort_unstable();
            Event::InventoryChanged {
                items,
                enchanted: false,
                world: WorldId::new(Dimension::OVERWORLD.id, 77),
                biome: None,
            }
        };
        let first = outcome(
            &PlayerAdvancementState::default(),
            &inventory(vec![Item::IRON_HELMET.id, Item::IRON_CHESTPLATE.id]),
            42,
        )
        .unwrap();
        assert!(achieved(&first.state, A::STORY_OBTAIN_ARMOR, "iron_helmet"));
        assert!(achieved(
            &first.state,
            A::STORY_OBTAIN_ARMOR,
            "iron_chestplate"
        ));
        let later = outcome(&first.state, &inventory(vec![Item::IRON_BOOTS.id]), 43).unwrap();
        assert!(!achieved(&later.state, A::STORY_OBTAIN_ARMOR, "iron_boots"));
        let killed = outcome(
            &PlayerAdvancementState::default(),
            &Event::PlayerKilledEntity {
                entity_type: pumpkin_data::entity::EntityType::ZOMBIE.id,
            },
            42,
        )
        .unwrap();
        let another = outcome(
            &killed.state,
            &Event::PlayerKilledEntity {
                entity_type: pumpkin_data::entity::EntityType::SKELETON.id,
            },
            43,
        )
        .unwrap();
        assert!(achieved(
            &another.state,
            A::ADVENTURE_KILL_A_MOB,
            "minecraft:zombie"
        ));
        assert!(achieved(
            &another.state,
            A::ADVENTURE_KILL_A_MOB,
            "minecraft:skeleton"
        ));
    }

    #[test]
    fn invalid_registry_facts_are_rejected_without_any_partial_award() {
        for event in [
            Event::ConsumeItem { item: u16::MAX },
            Event::FishedItem { item: u16::MAX },
            Event::PlayerKilledEntity {
                entity_type: u16::MAX,
            },
            Event::BredAnimal {
                parent_type: u16::MAX,
            },
            Event::PlacedBlock { block: u16::MAX },
            Event::EnterBlock { block: u16::MAX },
            Event::EnterDimension { dimension: u8::MAX },
            Event::InventoryChanged {
                items: vec![Item::CRAFTING_TABLE.id, u16::MAX],
                enchanted: true,
                world: WorldId::new(Dimension::OVERWORLD.id, 77),
                biome: None,
            },
            Event::InventoryChanged {
                items: vec![Item::CRAFTING_TABLE.id],
                enchanted: true,
                world: WorldId::new(Dimension::OVERWORLD.id, 77),
                biome: Some(u8::MAX),
            },
        ] {
            assert!(outcome(&PlayerAdvancementState::default(), &event, 42).is_none());
        }
    }
}
