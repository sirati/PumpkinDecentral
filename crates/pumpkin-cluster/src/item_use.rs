use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::{ActionId, ActionState, ApplyFailure, Cell, Effect, Target};
use crate::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemUseOperation {
    Start,
    Release,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemLaunchReservation {
    pub identities: Vec<crate::entity_origin::EntityOriginIdentity>,
    pub shooter: crate::item_contact::ContactObservation,
    pub eye_height: u64,
    pub yaw: u32,
    pub pitch: u32,
    pub velocity_micro: [i64; 3],
    pub on_ground: bool,
    pub spawn_chunk: crate::protocol::ChunkKey,
    pub ammunition: Vec<InventorySlotBinding>,
}

impl ItemLaunchReservation {
    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(self.shooter.target()).chain(self.ammunition.iter().map(|binding| binding.target))
            .chain(self.identities.iter().map(|identity| Target::Chunk { key: self.spawn_chunk,
                part: crate::canonical_action::ChunkPart::Entity(identity.id), inventory: None }))
    }

    pub fn valid(&self) -> bool {
        let height = f64::from_bits(self.eye_height);
        self.shooter.valid() && height.is_finite() && (0.0..=2048.0).contains(&height)
            && f32::from_bits(self.yaw).is_finite() && f32::from_bits(self.pitch).is_finite()
            && (1..=3).contains(&self.identities.len()) && self.spawn_chunk.world == self.shooter.world
            && self.identities.iter().all(|identity| identity.source == self.shooter.owner
                && identity.id.origin == identity.source.server && identity.id.generation == identity.source.generation
                && identity.id.local_id > 0 && !identity.uuid.is_nil())
            && self.identities.iter().map(|identity| identity.id).collect::<BTreeSet<_>>().len() == self.identities.len()
            && self.ammunition.len() <= 37 && self.ammunition.iter().all(InventorySlotBinding::is_valid)
            && self.ammunition.iter().map(|binding| binding.target).collect::<BTreeSet<_>>().len() == self.ammunition.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemUseIntent {
    pub vehicle: Option<crate::item_contact::ContactObservation>,
    pub vehicle_source: Option<crate::item_contact::ContactObservation>,
    pub living: Target,
    pub source: InventorySlotBinding,
    pub operation: ItemUseOperation,
    pub blocking_delay_ticks: u8,
    pub max_health_attribute: u16,
    pub used_statistic: Option<(u16, u32)>,
    pub remainder_destinations: Vec<InventorySlotBinding>,
    pub equipment: Vec<InventoryEquipmentBinding>,
    pub launch: Option<ItemLaunchReservation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemUseClaim {
    pub target_observation: Option<crate::item_contact::ContactObservation>,
    pub intent: crate::item_action::ItemActionIntent,
    pub effective_frame: crate::controller::ControllerFrame,
    pub eye_height: f64,
    pub owner_install_generation: u64,
}

impl ItemUseIntent {
    pub fn property_target(&self, property: crate::living_property::LivingProperty) -> Target {
        match self.living {
            Target::Chunk {
                key,
                part: crate::canonical_action::ChunkPart::Player(player),
                ..
            } => Target::Chunk {
                key,
                part: crate::canonical_action::ChunkPart::LivingProperty {
                    actor: crate::actor::ActorId::Player(player),
                    property,
                },
                inventory: None,
            },
            Target::Chunk {
                key,
                part: crate::canonical_action::ChunkPart::Entity(entity),
                ..
            } => Target::Chunk {
                key,
                part: crate::canonical_action::ChunkPart::LivingProperty {
                    actor: crate::actor::ActorId::Entity(entity),
                    property,
                },
                inventory: None,
            },
            _ => self.living,
        }
    }
    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        use crate::living_property::LivingProperty as P;
        [
            self.property_target(P::ItemUse),
            self.property_target(P::Food),
            self.property_target(P::Cooldowns),
            self.property_target(P::StatusEffects),
            self.property_target(P::Strength),
            self.property_target(P::Weakness),
            self.property_target(P::Resistance),
            self.property_target(P::FireResistance),
            self.property_target(P::Blindness),
            self.source.target,
        ]
        .into_iter()
        .chain(crate::combat_action::instant_health_targets(
            self.living,
            self.max_health_attribute,
        ))
        .chain([P::Advancements, P::Experience].into_iter().filter(|_| matches!(self.living,
            Target::Chunk { part: crate::canonical_action::ChunkPart::Player(_), inventory: None, .. }))
            .map(|property| self.property_target(property)))
        .chain(
            self.used_statistic
                .map(|(category, key)| self.property_target(P::Statistic { category, key })),
        )
        .chain(
            self.remainder_destinations
                .iter()
                .map(|binding| binding.target),
        )
        .chain(
            self.equipment
                .iter()
                .flat_map(InventoryEquipmentBinding::targets),
        )
        .chain(self.launch.iter().flat_map(ItemLaunchReservation::targets))
        .chain(self.vehicle.iter().flat_map(|vehicle| {
            let property = |property| match &vehicle.identity {
                crate::item_contact::ContactIdentity::Entity { id, .. } => Target::Chunk {
                    key: vehicle.chunk, part: crate::canonical_action::ChunkPart::VehicleProperty { actor: *id, property }, inventory: None },
                _ => vehicle.target(),
            };
            [vehicle.target(), property(crate::vehicle_property::VehicleProperty::Fuel), property(crate::vehicle_property::VehicleProperty::FurnacePush),
             property(crate::vehicle_property::VehicleProperty::Fuse)]
        }))
    }

    pub fn valid(&self) -> bool {
        self.living.is_valid()
            && self.vehicle.as_ref().is_none_or(|vehicle| vehicle.valid()
                && matches!(vehicle.identity, crate::item_contact::ContactIdentity::Entity { .. })
                && self.operation == ItemUseOperation::Complete && self.launch.is_none())
            && self.vehicle_source.as_ref().is_none_or(|source| source.valid() && self.vehicle.is_some())
            && self.launch.as_ref().is_none_or(ItemLaunchReservation::valid)
            && self.source.is_valid()
            && self.blocking_delay_ticks <= 5
            && self
                .used_statistic
                .is_none_or(|(category, _)| category <= 8)
            && self.remainder_destinations.len() <= 128
            && self.equipment.len() <= 1
            && self
                .remainder_destinations
                .iter()
                .all(InventorySlotBinding::is_valid)
            && self
                .remainder_destinations
                .iter()
                .map(|binding| binding.target)
                .collect::<BTreeSet<_>>()
                .len()
                == self.remainder_destinations.len()
            && self
                .equipment
                .iter()
                .all(|binding| binding.living == self.living)
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &S,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        context.derive_item_use(self, id, tick, values)
    }
}
