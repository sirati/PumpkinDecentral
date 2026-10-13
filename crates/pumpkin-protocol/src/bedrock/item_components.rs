use pumpkin_data::data_component_impl::{
    BlockEntityDataImpl, CustomDataImpl, CustomNameImpl, DamageImpl, DyedColorImpl,
    EnchantmentsImpl, ItemNameImpl, LoreImpl, PotionContentsImpl, RepairCostImpl,
    StoredEnchantmentsImpl, UnbreakableImpl, WritableBookContentImpl, WrittenBookContentImpl,
};
use pumpkin_data::item_stack::ItemStack;
use pumpkin_nbt::{Nbt, NbtCompound, tag::NbtTag};

pub(super) fn item_component_aux(stack: &ItemStack, fallback: u32) -> u32 {
    use pumpkin_data::{item::Item, potion::Potion};
    if ![
        Item::POTION.id,
        Item::SPLASH_POTION.id,
        Item::LINGERING_POTION.id,
        Item::TIPPED_ARROW.id,
    ]
    .contains(&stack.item.id)
    {
        return fallback;
    }
    let Some(potion) = stack
        .get_data_component::<PotionContentsImpl>()
        .and_then(|value| value.potion_id)
        .and_then(|id| u8::try_from(id).ok())
        .and_then(Potion::from_id)
    else {
        return fallback;
    };
    let names = [
        "water",
        "mundane",
        "luck",
        "thick",
        "awkward",
        "night_vision",
        "long_night_vision",
        "invisibility",
        "long_invisibility",
        "leaping",
        "long_leaping",
        "strong_leaping",
        "fire_resistance",
        "long_fire_resistance",
        "swiftness",
        "long_swiftness",
        "strong_swiftness",
        "slowness",
        "long_slowness",
        "water_breathing",
        "long_water_breathing",
        "healing",
        "strong_healing",
        "harming",
        "strong_harming",
        "poison",
        "long_poison",
        "strong_poison",
        "regeneration",
        "long_regeneration",
        "strong_regeneration",
        "strength",
        "long_strength",
        "strong_strength",
        "weakness",
        "long_weakness",
        "wither",
        "turtle_master",
        "long_turtle_master",
        "strong_turtle_master",
        "slow_falling",
        "long_slow_falling",
        "strong_slowness",
        "wind_charged",
        "weaving",
        "oozing",
        "infested",
    ];
    names
        .iter()
        .position(|name| *name == potion.name)
        .map(|id| id as u32 + u32::from(stack.item.id == Item::TIPPED_ARROW.id))
        .unwrap_or(fallback)
}

pub(super) fn item_component_nbt(stack: &ItemStack) -> Nbt {
    let mut root = stack
        .get_data_component::<CustomDataImpl>()
        .map(|data| data.data.clone())
        .unwrap_or_default();
    let mut display = root.get_compound("display").cloned().unwrap_or_default();
    display.child_tags.remove("Name");
    display.child_tags.remove("Lore");
    root.child_tags.remove("display");
    for key in [
        "Damage",
        "RepairCost",
        "Unbreakable",
        "customColor",
        "BlockEntityTag",
        "ench",
        "PumpkinComponents",
        "pages",
        "title",
        "author",
        "generation",
    ] {
        root.child_tags.remove(key);
    }
    if let Some(name) = stack.get_data_component::<CustomNameImpl>() {
        display.put_string("Name", name.name.clone().get_text());
    } else if stack.patch.iter().any(|(id, data)| {
        *id == pumpkin_data::data_component::DataComponent::ItemName && data.is_some()
    }) && let Some(name) = stack.get_data_component::<ItemNameImpl>()
    {
        display.put_string("Name", name.name.to_string());
    }
    if let Some(lore) = stack.get_data_component::<LoreImpl>() {
        display.put_list(
            "Lore",
            lore.lines
                .iter()
                .map(|line| NbtTag::String(line.clone().get_text().into_boxed_str()))
                .collect(),
        );
    }
    if !display.child_tags.is_empty() {
        root.put_compound("display", display);
    }
    if let Some(damage) = stack.get_data_component::<DamageImpl>() {
        root.put_int("Damage", damage.damage);
    }
    if let Some(cost) = stack.get_data_component::<RepairCostImpl>() {
        root.put_int("RepairCost", cost.cost);
    }
    if stack.get_data_component::<UnbreakableImpl>().is_some() {
        root.put_byte("Unbreakable", 1);
    }
    if let Some(color) = stack.get_data_component::<DyedColorImpl>() {
        root.put_int("customColor", color.rgb | 0xff000000_u32 as i32);
    }
    if let Some(data) = stack.get_data_component::<BlockEntityDataImpl>() {
        root.put_compound("BlockEntityTag", data.nbt.clone());
    }
    let enchantments = stack
        .get_data_component::<EnchantmentsImpl>()
        .map(|value| value.enchantment.as_ref())
        .filter(|values| !values.is_empty())
        .or_else(|| {
            stack
                .get_data_component::<StoredEnchantmentsImpl>()
                .map(|value| value.enchantment.as_ref())
        });
    if let Some(enchantments) = enchantments {
        let list = enchantments
            .iter()
            .filter_map(|(enchantment, level)| {
                let id = bedrock_enchantment_id(enchantment.name)?;
                let mut entry = NbtCompound::new();
                entry.put_short("id", id);
                entry.put_short("lvl", (*level).clamp(0, i32::from(i16::MAX)) as i16);
                Some(NbtTag::Compound(entry))
            })
            .collect();
        root.put_list("ench", list);
    }
    let pages = if let Some(book) = stack.get_data_component::<WrittenBookContentImpl>() {
        root.put_string("title", book.title.clone());
        root.put_string("author", book.author.clone());
        root.put_int("generation", 0);
        Some(&book.pages)
    } else {
        stack
            .get_data_component::<WritableBookContentImpl>()
            .map(|book| &book.pages)
    };
    if let Some(pages) = pages {
        root.put_list(
            "pages",
            pages
                .iter()
                .map(|text| {
                    let mut page = NbtCompound::new();
                    page.put_string("text", text.clone());
                    page.put_string("photoname", String::new());
                    NbtTag::Compound(page)
                })
                .collect(),
        );
    }
    if !stack.patch.is_empty() {
        let mut canonical = NbtCompound::new();
        stack.write_item_stack(&mut canonical);
        if let Some(components) = canonical.get_compound("components") {
            root.put_compound("PumpkinComponents", components.clone());
        }
    }
    Nbt {
        name: String::new(),
        root_tag: root,
    }
}

fn bedrock_enchantment_id(name: &str) -> Option<i16> {
    let name = name.strip_prefix("minecraft:")?;
    Some(match name {
        "protection" => 0,
        "fire_protection" => 1,
        "feather_falling" => 2,
        "blast_protection" => 3,
        "projectile_protection" => 4,
        "thorns" => 5,
        "respiration" => 6,
        "depth_strider" => 7,
        "aqua_affinity" => 8,
        "sharpness" => 9,
        "smite" => 10,
        "bane_of_arthropods" => 11,
        "knockback" => 12,
        "fire_aspect" => 13,
        "looting" => 14,
        "efficiency" => 15,
        "silk_touch" => 16,
        "unbreaking" => 17,
        "fortune" => 18,
        "power" => 19,
        "punch" => 20,
        "flame" => 21,
        "infinity" => 22,
        "luck_of_the_sea" => 23,
        "lure" => 24,
        "frost_walker" => 25,
        "mending" => 26,
        "binding_curse" => 27,
        "vanishing_curse" => 28,
        "impaling" => 29,
        "riptide" => 30,
        "loyalty" => 31,
        "channeling" => 32,
        "multishot" => 33,
        "piercing" => 34,
        "quick_charge" => 35,
        "soul_speed" => 36,
        "swift_sneak" => 37,
        "wind_burst" => 38,
        "density" => 39,
        "breach" => 40,
        "lunge" => 41,
        _ => return None,
    })
}
