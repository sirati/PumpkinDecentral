use pumpkin_data::sound::Sound;

// Verified against Mojang's vanilla definitions, rather than guessing Java names:
// https://github.com/Mojang/bedrock-samples/blob/main/resource_pack/sounds/sound_definitions.json
// https://github.com/Mojang/bedrock-samples/blob/main/resource_pack/sounds.json
pub(super) fn bedrock_name(sound: Sound) -> Option<&'static str> {
    Some(match sound.to_name() {
        "item.axe.strip" => "use.wood",
        "item.axe.scrape" => "scrape",
        "item.axe.wax_off" => "copper.wax.off",
        "item.honeycomb.wax_on" => "copper.wax.on",
        "block.wooden_door.open" => "open.wooden_door",
        "block.wooden_door.close" => "close.wooden_door",
        "block.iron_door.open" => "open.iron_door",
        "block.iron_door.close" => "close.iron_door",
        "block.copper_door.open" => "open_door.copper",
        "block.copper_door.close" => "close_door.copper",
        "block.bamboo_wood_door.open" => "open.bamboo_wood_door",
        "block.bamboo_wood_door.close" => "close.bamboo_wood_door",
        "block.cherry_wood_door.open" => "open.cherry_wood_door",
        "block.cherry_wood_door.close" => "close.cherry_wood_door",
        "block.nether_wood_door.open" => "open.nether_wood_door",
        "block.nether_wood_door.close" => "close.nether_wood_door",
        "block.wooden_trapdoor.open" => "open.wooden_trapdoor",
        "block.wooden_trapdoor.close" => "close.wooden_trapdoor",
        "block.iron_trapdoor.open" => "open.iron_trapdoor",
        "block.iron_trapdoor.close" => "close.iron_trapdoor",
        "block.copper_trapdoor.open" | "block.copper_trapdoor.close" => "open_trapdoor.copper",
        "block.bamboo_wood_trapdoor.open" => "open.bamboo_wood_trapdoor",
        "block.bamboo_wood_trapdoor.close" => "close.bamboo_wood_trapdoor",
        "block.cherry_wood_trapdoor.open" => "open.cherry_wood_trapdoor",
        "block.cherry_wood_trapdoor.close" => "close.cherry_wood_trapdoor",
        "block.nether_wood_trapdoor.open" => "open.nether_wood_trapdoor",
        "block.nether_wood_trapdoor.close" => "close.nether_wood_trapdoor",
        "block.fence_gate.open" => "open.fence_gate",
        "block.fence_gate.close" => "close.fence_gate",
        "block.bamboo_wood_fence_gate.open" => "open.bamboo_wood_fence_gate",
        "block.bamboo_wood_fence_gate.close" => "close.bamboo_wood_fence_gate",
        "block.cherry_wood_fence_gate.open" => "open.cherry_wood_fence_gate",
        "block.cherry_wood_fence_gate.close" => "close.cherry_wood_fence_gate",
        "block.nether_wood_fence_gate.open" => "open.nether_wood_fence_gate",
        "block.nether_wood_fence_gate.close" => "close.nether_wood_fence_gate",
        "block.wooden_button.click_on"
        | "block.wooden_button.click_off"
        | "block.bamboo_wood_button.click_on"
        | "block.bamboo_wood_button.click_off"
        | "block.cherry_wood_button.click_on"
        | "block.cherry_wood_button.click_off"
        | "block.nether_wood_button.click_on"
        | "block.nether_wood_button.click_off" => "random.wood_click",
        "block.stone_button.click_on" | "block.stone_button.click_off" => "random.stone_click",
        "block.lever.click" => "random.lever_click",
        "block.respawn_anchor.ambient" => "respawn_anchor.ambient",
        "block.respawn_anchor.charge" => "respawn_anchor.charge",
        "block.respawn_anchor.deplete" => "respawn_anchor.deplete",
        "block.respawn_anchor.set_spawn" => "respawn_anchor.set_spawn",
        "block.copper_bulb.turn_on" => "block.copper_bulb.turn_on",
        "block.copper_bulb.turn_off" => "block.copper_bulb.turn_off",
        "block.end_portal_frame.fill" => "block.end_portal_frame.fill",
        "block.end_portal.spawn" => "block.end_portal.spawn",
        "block.lava.extinguish" => "random.fizz",
        "item.bucket.empty" => "bucket.empty_water",
        "item.bucket.empty_lava" => "bucket.empty_lava",
        "item.bucket.empty_powder_snow" => "bucket.empty_powder_snow",
        "item.bucket.fill" => "bucket.fill_water",
        "item.bucket.fill_lava" => "bucket.fill_lava",
        "item.bucket.fill_powder_snow" => "bucket.fill_powder_snow",
        _ => return None,
    })
}
