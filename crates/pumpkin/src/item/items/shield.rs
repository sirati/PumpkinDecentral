use std::any::Any;

use crate::item::{ItemBehaviour, ItemMetadata};
use pumpkin_data::item::Item;

pub struct ShieldItem;

impl ItemMetadata for ShieldItem {
    fn ids() -> Box<[u16]> {
        Box::new([Item::SHIELD.id])
    }
}

#[pumpkin_macros::canonical_item_defaults]
impl ItemBehaviour for ShieldItem {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
