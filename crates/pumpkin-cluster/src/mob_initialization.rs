use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::canonical_merchant::MerchantOffersState;
use crate::inventory::InventoryCell;
use crate::mob_continuation::{MobReproduction, MobSpecies};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobInitialization {
    pub attributes: std::collections::BTreeMap<u16, u64>,
    pub attribute_modifiers: std::collections::BTreeMap<u16, Vec<crate::combat_action::CombatAttributeModifier>>,
    pub effects: crate::active_effects::ActiveEffectSet,
    pub species: MobSpecies,
    pub reproduction: MobReproduction,
    pub tags: BTreeSet<String>,
    pub health_milli: Option<u32>,
    pub merchant: Option<MerchantOffersState>,
    pub equipment: Vec<InventoryCell>,
    pub fall_distance: u32,
    pub ai_disabled: bool,
    pub persistence_required: bool,
    pub can_pick_up_loot: bool,
    pub left_handed: bool,
}

impl MobInitialization {
    pub fn valid(&self) -> bool {
        self.attributes.len() <= 256 && self.attributes.values().all(|base| f64::from_bits(*base).is_finite())
            && self.attribute_modifiers.len() <= 256
            && self.attribute_modifiers.iter().all(|(id, modifiers)|
                modifiers.len() <= 256 && modifiers.iter().enumerate().all(|(index, modifier)|
                    modifier.attribute == *id && modifier.slot <= 7 && !modifier.id.is_empty() && modifier.id.len() <= 256
                        && f64::from_bits(modifier.amount).is_finite()
                        && modifiers[..index].iter().all(|other| other.id != modifier.id)))
            && self.effects.valid() && self.species.valid() && self.reproduction.valid() && crate::actor_tags::valid(&self.tags)
            && f32::from_bits(self.fall_distance).is_finite()
            && (0.0..=30_000_000.0).contains(&f32::from_bits(self.fall_distance))
            && self.merchant.as_ref().is_none_or(MerchantOffersState::valid)
            && self.equipment.len() <= 8
            && self.equipment.iter().enumerate().all(|(index, item)|
                item.loc.inv == crate::entity_inventory_admission::INV_EQUIPMENT
                    && item.loc.slot < 8 && !item.is_empty()
                    && self.equipment[..index].iter().all(|other| other.loc != item.loc)
                    && item.count <= crate::inventory::INV_MAX_STACK
                    && item.nbt.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityAdmissionRequest {
    pub kind: u16,
    pub spatial: crate::entity_body::EntitySpatial,
    pub initial: MobInitialization,
}

impl EntityAdmissionRequest {
    pub fn chunk(&self) -> Option<crate::protocol::ChunkKey> {
        let position = self.spatial.position.map(f64::from_bits);
        if position.iter().any(|value| !value.is_finite() || value.abs() > 30_000_000.0) {
            return None;
        }
        let chunk = crate::protocol::ChunkKey::new(self.spatial.world,
            (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
        self.spatial.belongs_to(chunk).then_some(chunk)
    }
    pub fn valid(&self) -> bool { self.chunk().is_some() && self.initial.valid() }
}
