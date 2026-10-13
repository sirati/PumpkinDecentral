use crate::entity::player::Player;
use crate::item::{ItemBehaviour, ItemMetadata};
use pumpkin_data::item::Item;
use pumpkin_util::GameMode;

pub struct MaceItem;

impl ItemMetadata for MaceItem {
    fn ids() -> Box<[u16]> {
        [Item::MACE.id].into()
    }
}
#[pumpkin_macros::canonical_item_defaults]
impl ItemBehaviour for MaceItem {
    fn can_mine_intent(&self, game_mode: u8) -> Option<bool> {
        Some(game_mode != GameMode::Creative as u8)
    }

    fn can_mine(&self, player: &Player) -> bool {
        player.gamemode.load() != GameMode::Creative
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
