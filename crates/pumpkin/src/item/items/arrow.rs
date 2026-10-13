use std::any::Any;

use crate::item::{ItemBehaviour, ItemMetadata};
use pumpkin_data::item::Item;

pub struct ArrowItem;

impl ItemMetadata for ArrowItem {
    fn ids() -> Box<[u16]> {
        Box::new([
            Item::ARROW.id,
            Item::TIPPED_ARROW.id,
            Item::SPECTRAL_ARROW.id,
        ])
    }
}

#[pumpkin_macros::canonical_item_defaults]
impl ItemBehaviour for ArrowItem {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
