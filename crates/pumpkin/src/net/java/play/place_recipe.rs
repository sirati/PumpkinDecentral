#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    #[allow(clippy::too_many_lines)]
    pub fn handle_place_recipe(
        &self,
        server: &Arc<Server>,
        player: &Arc<Player>,
        packet: &SPlaceRecipe,
    ) {
        if server.advanced_config.cluster.enabled {
            crate::server::cluster_inventory_protocol::stage_java_place_recipe(
                player,
                packet.recipe_display_id.0,
                packet.use_max_items,
            );
            return;
        }
        use crate::net::java::recipe_helper::{
            GenericIngredient, compute_biggest_craftable, take_n_ingredient,
        };
        use crate::server::recipe::DynamicRecipe;
        use pumpkin_data::recipes::{CraftingRecipeTypes, RECIPES_COOKING, RECIPES_CRAFTING};
        use pumpkin_data::screen::WindowType;
        use pumpkin_inventory::crafting::recipe_provider::RecipeProvider;

        let target_id = packet.recipe_display_id.0 as usize;
        let use_max = packet.use_max_items;

        let mut click_event = crate::plugin::api::events::player::player_recipe_book_click::PlayerRecipeBookClickEvent::new(
            player.clone(),
            format!("display_{}", packet.recipe_display_id.0),
            use_max,
        );
        server
            .plugin_manager
            .fire_blocking(server, &mut click_event);
        if click_event.cancelled {
            return;
        }

        // Count crafting display IDs.
        let crafting_display_count = RECIPES_CRAFTING
            .iter()
            .filter(|r| {
                !matches!(
                    r,
                    CraftingRecipeTypes::CraftingSpecial
                        | CraftingRecipeTypes::CraftingDecoratedPot { .. }
                )
            })
            .count();
        let cooking_display_count = RECIPES_COOKING.len();
        let dynamic_recipes = server.recipe_manager.get_dynamic_recipes();

        let (grid_width, crafting_inv) = {
            let screen_handler_arc = player
                .current_screen_handler
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let handler = screen_handler_arc
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let grid_width: usize = match handler.window_type() {
                Some(WindowType::Crafting) => 3,
                None => 2, // player inventory 2x2
                _ => return,
            };
            (grid_width, handler.get_behaviour().slots[1].get_inventory())
        };

        let grid_size = grid_width * grid_width;
        let mut ingredient_slots: Vec<Option<GenericIngredient<'_>>> = vec![None; grid_size];

        if target_id < crafting_display_count {
            // Crafting recipe
            let mut counter = 0usize;
            let recipe = RECIPES_CRAFTING.iter().find(|r| {
                if matches!(
                    r,
                    CraftingRecipeTypes::CraftingSpecial
                        | CraftingRecipeTypes::CraftingDecoratedPot { .. }
                ) {
                    return false;
                }
                let found = counter == target_id;
                counter += 1;
                found
            });
            let Some(recipe) = recipe else { return };

            match recipe {
                CraftingRecipeTypes::CraftingShaped { pattern, key, .. } => {
                    for (row, row_str) in pattern.iter().enumerate() {
                        for (col, ch) in row_str.chars().enumerate() {
                            if ch != ' '
                                && let Some(ing) =
                                    key.iter().find_map(|(k, v)| (*k == ch).then_some(v))
                                && row * grid_width + col < grid_size
                            {
                                ingredient_slots[row * grid_width + col] =
                                    Some(GenericIngredient::Vanilla(ing));
                            }
                        }
                    }
                }
                CraftingRecipeTypes::CraftingShapeless { ingredients, .. } => {
                    for (i, ing) in ingredients.iter().enumerate().take(grid_size) {
                        ingredient_slots[i] = Some(GenericIngredient::Vanilla(ing));
                    }
                }
                CraftingRecipeTypes::CraftingTransmute {
                    input, material, ..
                } => {
                    if grid_size >= 2 {
                        ingredient_slots[0] = Some(GenericIngredient::Vanilla(input));
                        ingredient_slots[1] = Some(GenericIngredient::Vanilla(material));
                    }
                }
                _ => return,
            }
        } else if target_id < crafting_display_count + cooking_display_count {
            // TODO: cooking recipes
            return;
        } else {
            let dynamic_id = target_id - crafting_display_count - cooking_display_count;
            let Some(DynamicRecipe::Crafting(crafting)) = dynamic_recipes.iter().filter(|recipe| !matches!(recipe, DynamicRecipe::Brewing(_))).nth(dynamic_id) else {
                return;
            };

            match crafting {
                pumpkin_protocol::codec::recipe::OwnedCraftingRecipe::Shaped {
                    pattern,
                    key,
                    ..
                } => {
                    for (row, row_str) in pattern.iter().enumerate() {
                        for (col, ch) in row_str.chars().enumerate() {
                            if ch != ' '
                                && let Some((_, ing)) = key.iter().find(|(k, _)| *k == ch)
                                && row * grid_width + col < grid_size
                            {
                                ingredient_slots[row * grid_width + col] =
                                    Some(GenericIngredient::Dynamic(ing));
                            }
                        }
                    }
                }

                pumpkin_protocol::codec::recipe::OwnedCraftingRecipe::Shapeless {
                    ingredients,
                    ..
                } => {
                    for (i, ing) in ingredients.iter().enumerate().take(grid_size) {
                        ingredient_slots[i] = Some(GenericIngredient::Dynamic(ing));
                    }
                }
            }
        }

        // Check if this exact recipe is already placed (determines stacking vs fresh fill).
        let recipe_matches = {
            let mut ok = true;
            for (idx, ing) in ingredient_slots.iter().enumerate() {
                let stack = crafting_inv.get_stack(idx);
                match ing {
                    None => {
                        if !stack.is_empty() {
                            ok = false;
                            break;
                        }
                    }
                    Some(ingredient) => {
                        if stack.is_empty() || !ingredient.match_item(stack.item) {
                            ok = false;
                            break;
                        }
                    }
                }
            }
            ok
        };

        // Read minimum count from occupied slots before clearing (needed for stacking).
        let current_min = if recipe_matches && !use_max {
            let mut min = u8::MAX;
            for (idx, ing) in ingredient_slots.iter().enumerate() {
                if ing.is_some() {
                    let stack = crafting_inv.get_stack(idx);
                    if !stack.is_empty() {
                        min = min.min(stack.item_count);
                    }
                }
            }
            if min == u8::MAX { 0 } else { min }
        } else {
            0
        };

        // Always clear the grid first, returning items to inventory.
        for i in 0..grid_size {
            let stack = crafting_inv.remove_stack(i);
            if !stack.is_empty() {
                player.inventory.offer(stack, false, player.as_ref());
            }
        }

        // Determine how many of each ingredient to place per slot.
        let active_ingredients: Vec<GenericIngredient<'_>> =
            ingredient_slots.iter().flatten().copied().collect();
        let amount_to_craft = if use_max {
            compute_biggest_craftable(&active_ingredients, &player.inventory)
        } else if recipe_matches {
            current_min.saturating_add(1)
        } else {
            1
        };

        if amount_to_craft == 0 {
            let screen_handler_arc = player
                .current_screen_handler
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            screen_handler_arc
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .send_content_updates();
            return;
        }

        // Fill each grid slot with exactly `amount_to_craft` matching items.
        for (idx, ing) in ingredient_slots.iter().enumerate() {
            let Some(ingredient) = ing else { continue };
            let taken = take_n_ingredient(&player.inventory, ingredient, amount_to_craft);
            if !taken.is_empty() {
                crafting_inv.set_stack(idx, taken);
            }
        }

        let screen_handler_arc = player
            .current_screen_handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        screen_handler_arc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .send_content_updates();
    }
}

pub(crate) fn process_cluster_place_recipe(
    player: &Arc<Player>,
    tick: pumpkin_cluster::time::TickStamp,
    display_id: i32,
    use_max: bool,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
    recipes: Arc<Vec<crate::server::recipe::DynamicRecipe>>,
) -> Result<(), pumpkin_cluster::inventory::InventoryApplyError> {
    use crate::net::java::recipe_helper::GenericIngredient;
    use crate::server::recipe::DynamicRecipe;
    use pumpkin_cluster::inventory::{
        INV_MAIN, InventoryApplyError, InventoryGesture, InventoryIntentPayload,
        InventoryMutationIntent, InventoryScreenSession, InventoryTarget,
        RecipePlacementIngredient,
    };
    use pumpkin_data::recipes::{CraftingRecipeTypes, RECIPES_COOKING, RECIPES_CRAFTING};

    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let session = InventoryScreenSession {
        actor,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
    };
    let screen = crate::server::cluster_invsee::inventory_screen_snapshot(&server, session)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let grid_width = if session.session == 0 {
        2
    } else if screen.window_kind == pumpkin_data::screen::WindowType::Crafting as u16 {
        3
    } else {
        return Err(InventoryApplyError::Rejected);
    };
    let grid_size = grid_width * grid_width;
    let player_target = InventoryTarget::Player(actor);
    let target_id = usize::try_from(display_id).map_err(|_| InventoryApplyError::Rejected)?;
    let crafting_display_count = RECIPES_CRAFTING
        .iter()
        .filter(|recipe| {
            !matches!(
                recipe,
                CraftingRecipeTypes::CraftingSpecial
                    | CraftingRecipeTypes::CraftingDecoratedPot { .. }
            )
        })
        .count();
    let matching_items = |ingredient: GenericIngredient<'_>| {
        (0..=u16::MAX)
            .filter(|item| {
                pumpkin_data::item::Item::from_id(*item)
                    .is_some_and(|item| ingredient.match_item(item))
            })
            .collect::<Vec<_>>()
    };
    let mut layout: Vec<Option<Vec<u16>>> = vec![None; grid_size];
    if target_id < crafting_display_count {
        let mut counter = 0_usize;
        let recipe = RECIPES_CRAFTING
            .iter()
            .find(|recipe| {
                if matches!(
                    recipe,
                    CraftingRecipeTypes::CraftingSpecial
                        | CraftingRecipeTypes::CraftingDecoratedPot { .. }
                ) {
                    return false;
                }
                let found = counter == target_id;
                counter += 1;
                found
            })
            .ok_or(InventoryApplyError::Rejected)?;
        match recipe {
            CraftingRecipeTypes::CraftingShaped { pattern, key, .. } => {
                if pattern.len() > grid_width
                    || pattern.iter().any(|row| row.chars().count() > grid_width)
                {
                    return Err(InventoryApplyError::Rejected);
                }
                for (row, value) in pattern.iter().enumerate() {
                    for (column, character) in value.chars().enumerate() {
                        if character != ' '
                            && let Some(ingredient) = key
                                .iter()
                                .find_map(|(key, value)| (*key == character).then_some(value))
                            && row * grid_width + column < grid_size
                        {
                            layout[row * grid_width + column] =
                                Some(matching_items(GenericIngredient::Vanilla(ingredient)));
                        }
                    }
                }
            }
            CraftingRecipeTypes::CraftingShapeless { ingredients, .. } => {
                if ingredients.len() > grid_size {
                    return Err(InventoryApplyError::Rejected);
                }
                for (index, ingredient) in ingredients.iter().enumerate().take(grid_size) {
                    layout[index] = Some(matching_items(GenericIngredient::Vanilla(ingredient)));
                }
            }
            CraftingRecipeTypes::CraftingTransmute {
                input, material, ..
            } if grid_size >= 2 => {
                layout[0] = Some(matching_items(GenericIngredient::Vanilla(input)));
                layout[1] = Some(matching_items(GenericIngredient::Vanilla(material)));
            }
            _ => return Err(InventoryApplyError::Rejected),
        }
    } else {
        let cooking_display_count = RECIPES_COOKING.len();
        if target_id < crafting_display_count + cooking_display_count {
            return Err(InventoryApplyError::Rejected);
        }
        let dynamic_id = target_id - crafting_display_count - cooking_display_count;
        let DynamicRecipe::Crafting(recipe) = recipes.iter().filter(|recipe| !matches!(recipe, DynamicRecipe::Brewing(_)))
            .nth(dynamic_id)
            .ok_or(InventoryApplyError::Rejected)?
        else {
            return Err(InventoryApplyError::Rejected);
        };
        match recipe {
            pumpkin_protocol::codec::recipe::OwnedCraftingRecipe::Shaped {
                pattern, key, ..
            } => {
                if pattern.len() > grid_width
                    || pattern.iter().any(|row| row.chars().count() > grid_width)
                {
                    return Err(InventoryApplyError::Rejected);
                }
                for (row, value) in pattern.iter().enumerate() {
                    for (column, character) in value.chars().enumerate() {
                        if character != ' '
                            && let Some((_, ingredient)) =
                                key.iter().find(|(key, _)| *key == character)
                            && row * grid_width + column < grid_size
                        {
                            layout[row * grid_width + column] =
                                Some(matching_items(GenericIngredient::Dynamic(ingredient)));
                        }
                    }
                }
            }
            pumpkin_protocol::codec::recipe::OwnedCraftingRecipe::Shapeless {
                ingredients, ..
            } => {
                if ingredients.len() > grid_size {
                    return Err(InventoryApplyError::Rejected);
                }
                for (index, ingredient) in ingredients.iter().enumerate().take(grid_size) {
                    layout[index] = Some(matching_items(GenericIngredient::Dynamic(ingredient)));
                }
            }
        }
    }
    let grid_slots = (0..grid_size)
        .map(|index| {
            screen
                .slots
                .iter()
                .find(|slot| usize::from(slot.screen_slot) == index + 1)
                .map(|slot| slot.slot)
                .ok_or(InventoryApplyError::TargetUnavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let grid_target = grid_slots
        .first()
        .map(|slot| slot.target)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let mut ingredients = Vec::new();
    for (index, acceptable_items) in layout.into_iter().enumerate() {
        let Some(acceptable_items) = acceptable_items else {
            continue;
        };
        ingredients.push(RecipePlacementIngredient {
            grid_slot: grid_slots[index],
            acceptable_items,
            count_per_craft: 1,
        });
    }
    let mut player_sources = screen
        .slots
        .iter()
        .filter(|slot| slot.slot.target == player_target && slot.slot.loc.inv == INV_MAIN)
        .map(|slot| slot.slot)
        .collect::<Vec<_>>();
    player_sources.sort_unstable_by_key(|slot| slot.loc.slot);
    player_sources.dedup();
    let gesture = InventoryGesture::PlaceRecipe {
        player: player_target,
        grid: grid_target,
        ingredients,
        grid_slots,
        player_sources,
        use_max,
    };
    let screen_session = screen.session;
    let mut affected = screen.affected;
    let actor_chunk = crate::server::cluster_inventory_protocol::player_chunk(player);
    affected.push(actor_chunk);
    affected.sort_unstable();
    affected.dedup();
    crate::server::cluster_invsee::submit_inventory_intent_with_sequence(
        &server,
        InventoryMutationIntent {
            actor: actor.into(),
            tick,
            payload: InventoryIntentPayload::Gestures(vec![gesture]),
            actor_chunk,
            affected,
            screen_session: Some(screen_session),
        },
        None,
        Some(input_sequence),
    )
}
