use serde::{Deserialize, Serialize};

use crate::canonical_action::{Cell, ChunkPart, LivePlayerLease, Target, Value};
use crate::entity_body::{EntityPayload, VehicleKind, VehicleState};
use crate::identity::PlayerSeq;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VehicleProperty {
    SimulationTick,
    LifecycleTick,
    Control,
    Underwater,
    Wobble,
    Damage,
    DropLoot,
    HopperEnabled,
    Fuel,
    FurnacePush,
    Fuse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VehiclePropertyValue {
    Tick(Option<TickStamp>),
    Control {
        driver: Option<LivePlayerLease>,
        sequence: Option<PlayerSeq>,
        paddle_left: bool,
        paddle_right: bool,
    },
    Unsigned(u32),
    Boolean(bool),
    Wobble {
        ticks: u16,
        side: i8,
    },
    Fuse(Option<u16>),
    Push([i64; 2]),
}

impl VehicleProperty {
    pub const ALL: [Self; 11] = [
        Self::SimulationTick,
        Self::LifecycleTick,
        Self::Control,
        Self::Underwater,
        Self::Wobble,
        Self::Damage,
        Self::DropLoot,
        Self::HopperEnabled,
        Self::Fuel,
        Self::FurnacePush,
        Self::Fuse,
    ];

    pub fn accepts(self, value: &VehiclePropertyValue) -> bool {
        match (self, value) {
            (Self::SimulationTick | Self::LifecycleTick, VehiclePropertyValue::Tick(_)) => true,
            (
                Self::Control,
                VehiclePropertyValue::Control {
                    driver, sequence, ..
                },
            ) => {
                driver.is_none_or(|lease| lease.incarnation != 0 && !lease.key.0.is_nil())
                    && (sequence.is_none() || driver.is_some())
            }
            (Self::Underwater, VehiclePropertyValue::Unsigned(value)) => *value <= 60,
            (Self::Wobble, VehiclePropertyValue::Wobble { side, .. }) => matches!(side, -1 | 1),
            (Self::Damage | Self::Fuel, VehiclePropertyValue::Unsigned(_)) => true,
            (Self::DropLoot | Self::HopperEnabled, VehiclePropertyValue::Boolean(_)) => true,
            (Self::Fuse, VehiclePropertyValue::Fuse(_)) => true,
            (Self::FurnacePush, VehiclePropertyValue::Push(value)) => {
                value.iter().all(|axis| axis.unsigned_abs() <= 1_000_000)
            }
            _ => false,
        }
    }
}

fn state(value: &Value) -> Option<&VehicleState> {
    let Value::Entity(body) = value else {
        return None;
    };
    let EntityPayload::Vehicle(vehicle) = &body.payload else {
        return None;
    };
    Some(vehicle)
}

fn state_mut(value: &mut Value) -> Option<&mut VehicleState> {
    let Value::Entity(body) = value else {
        return None;
    };
    let EntityPayload::Vehicle(vehicle) = &mut body.payload else {
        return None;
    };
    Some(vehicle)
}

pub fn body_target(target: Target) -> Option<(Target, VehicleProperty)> {
    let Target::Chunk {
        key,
        part: ChunkPart::VehicleProperty { actor, property },
        inventory: None,
    } = target
    else {
        return None;
    };
    Some((
        Target::Chunk {
            key,
            part: ChunkPart::Entity(actor),
            inventory: None,
        },
        property,
    ))
}

pub fn read(body: &Value, property: VehicleProperty) -> Option<VehiclePropertyValue> {
    let vehicle = state(body)?;
    Some(match property {
        VehicleProperty::SimulationTick => VehiclePropertyValue::Tick(vehicle.simulation_tick),
        VehicleProperty::LifecycleTick => VehiclePropertyValue::Tick(vehicle.lifecycle_tick),
        VehicleProperty::Control => VehiclePropertyValue::Control {
            driver: vehicle.driver,
            sequence: vehicle.control_sequence,
            paddle_left: vehicle.paddle_left,
            paddle_right: vehicle.paddle_right,
        },
        VehicleProperty::Underwater => {
            VehiclePropertyValue::Unsigned(u32::from(vehicle.underwater_ticks))
        }
        VehicleProperty::Wobble => VehiclePropertyValue::Wobble {
            ticks: vehicle.wobble_ticks,
            side: vehicle.wobble_side,
        },
        VehicleProperty::Damage => VehiclePropertyValue::Unsigned(vehicle.damage_milli),
        VehicleProperty::DropLoot => VehiclePropertyValue::Boolean(vehicle.drop_loot),
        VehicleProperty::HopperEnabled => VehiclePropertyValue::Boolean(vehicle.hopper_enabled),
        VehicleProperty::Fuel => VehiclePropertyValue::Unsigned(vehicle.fuel_ticks),
        VehicleProperty::FurnacePush => VehiclePropertyValue::Push(vehicle.furnace_push_micro),
        VehicleProperty::Fuse => VehiclePropertyValue::Fuse(vehicle.fuse_ticks),
    })
}

pub fn patch(body: &mut Value, property: VehicleProperty, value: &VehiclePropertyValue) -> bool {
    if !property.accepts(value) {
        return false;
    }
    let Some(vehicle) = state_mut(body) else {
        return false;
    };
    match (property, value) {
        (VehicleProperty::FurnacePush, VehiclePropertyValue::Push(push)) => {
            if vehicle.kind != VehicleKind::FurnaceMinecart && *push != [0; 2] {
                return false;
            }
            vehicle.furnace_push_micro = *push;
        }
        (VehicleProperty::LifecycleTick, VehiclePropertyValue::Tick(tick)) => {
            vehicle.lifecycle_tick = *tick
        }
        (VehicleProperty::SimulationTick, VehiclePropertyValue::Tick(tick)) => {
            vehicle.simulation_tick = *tick
        }
        (
            VehicleProperty::Control,
            VehiclePropertyValue::Control {
                driver,
                sequence,
                paddle_left,
                paddle_right,
            },
        ) => {
            if (paddle_left | paddle_right)
                && !matches!(vehicle.kind, VehicleKind::Boat | VehicleKind::ChestBoat)
            {
                return false;
            }
            vehicle.driver = *driver;
            vehicle.control_sequence = *sequence;
            vehicle.paddle_left = *paddle_left;
            vehicle.paddle_right = *paddle_right;
        }
        (VehicleProperty::Underwater, VehiclePropertyValue::Unsigned(value)) => {
            vehicle.underwater_ticks = *value as u16
        }
        (VehicleProperty::Wobble, VehiclePropertyValue::Wobble { ticks, side }) => {
            vehicle.wobble_ticks = *ticks;
            vehicle.wobble_side = *side;
        }
        (VehicleProperty::Damage, VehiclePropertyValue::Unsigned(value)) => {
            vehicle.damage_milli = *value
        }
        (VehicleProperty::HopperEnabled, VehiclePropertyValue::Boolean(value)) => {
            if !*value && vehicle.kind != VehicleKind::HopperMinecart {
                return false;
            }
            vehicle.hopper_enabled = *value;
        }
        (VehicleProperty::DropLoot, VehiclePropertyValue::Boolean(value)) => {
            vehicle.drop_loot = *value;
        }
        (VehicleProperty::Fuel, VehiclePropertyValue::Unsigned(value)) => {
            if *value != 0 && vehicle.kind != VehicleKind::FurnaceMinecart {
                return false;
            }
            vehicle.fuel_ticks = *value;
        }
        (VehicleProperty::Fuse, VehiclePropertyValue::Fuse(value)) => {
            if value.is_some() && vehicle.kind != VehicleKind::TntMinecart {
                return false;
            }
            vehicle.fuse_ticks = *value;
        }
        _ => return false,
    }
    true
}

pub fn cell(body: &Cell, property: VehicleProperty) -> Option<Cell> {
    Some(Cell {
        revision: state(&body.value)?
            .property_versions
            .get(&property)
            .copied()
            .unwrap_or(0),
        value: Value::VehicleProperty(read(&body.value, property)?),
    })
}

pub fn patch_cell(body: &mut Cell, property: VehicleProperty, next: &Cell) -> bool {
    let Value::VehicleProperty(value) = &next.value else {
        return false;
    };
    if !patch(&mut body.value, property, value) {
        return false;
    }
    let versions = &mut state_mut(&mut body.value).unwrap().property_versions;
    if next.revision == 0 {
        versions.remove(&property);
    } else {
        versions.insert(property, next.revision);
    }
    true
}

pub fn normalize_body_versions(before: &Cell, after: &mut Cell) -> bool {
    if state(&before.value).is_none() || state(&after.value).is_none() {
        return false;
    }
    let mut versions = state(&before.value).unwrap().property_versions.clone();
    for property in VehicleProperty::ALL {
        if read(&before.value, property) != read(&after.value, property) {
            let Some(revision) = versions.get(&property).copied().unwrap_or(0).checked_add(1)
            else {
                return false;
            };
            versions.insert(property, revision);
        }
    }
    state_mut(&mut after.value).unwrap().property_versions = versions;
    true
}

pub fn project_body_writes(
    target: Target,
    before: Option<&Cell>,
    writes: &[crate::canonical_action::AppliedWrite],
) -> Result<Option<Cell>, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::ApplyFailure;
    let facets = writes
        .iter()
        .filter_map(|write| {
            body_target(write.target)
                .filter(|(body, _)| *body == target)
                .map(|(_, property)| (write, property))
        })
        .collect::<Vec<_>>();
    if facets.is_empty() {
        return Ok(writes
            .iter()
            .find(|write| write.target == target)
            .map_or_else(|| before.cloned(), |write| write.after.clone()));
    }
    let mut result = before.cloned().ok_or(ApplyFailure::Condition(target))?;
    for (write, property) in facets {
        if cell(&result, property) != write.before {
            return Err(ApplyFailure::Condition(write.target));
        }
        let next = write
            .after
            .as_ref()
            .ok_or(ApplyFailure::Type(write.target))?;
        if !patch_cell(&mut result, property, next) {
            return Err(ApplyFailure::Type(write.target));
        }
    }
    result.revision = result
        .revision
        .checked_add(1)
        .ok_or(ApplyFailure::Overflow(target))?;
    if let Some(existing) = writes.iter().find(|write| write.target == target) {
        if existing.before.as_ref() != before || existing.after.as_ref() != Some(&result) {
            return Err(ApplyFailure::Condition(target));
        }
    }
    Ok(Some(result))
}
