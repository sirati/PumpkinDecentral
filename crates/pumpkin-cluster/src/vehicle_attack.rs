use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canonical_action::{ApplyFailure, Cell, ChunkPart, Effect, Target, Value};
use crate::combat_action::{AttackIntent, living_value};
use crate::living_property::{LivingProperty as L, LivingPropertyValue};
use crate::vehicle_property::{VehicleProperty as P, VehiclePropertyValue};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleAttackIntent {
    pub attack: AttackIntent,
}

impl VehicleAttackIntent {
    pub fn valid(&self) -> bool {
        self.attack.is_valid()
            && !self.attack.sweeping
            && matches!(
                self.attack.contact.victim.identity,
                crate::item_contact::ContactIdentity::Entity { .. }
            )
    }

    pub fn targets(&self) -> Vec<Target> {
        let mut targets = vec![self.attack.source, self.attack.target];
        if let Target::Chunk {
            key,
            part: ChunkPart::Entity(actor),
            inventory: None,
        } = self.attack.target
        {
            targets.extend([P::Damage, P::Wobble, P::DropLoot].map(|property| Target::Chunk {
                key,
                part: ChunkPart::VehicleProperty { actor, property },
                inventory: None,
            }));
        }
        if let Target::Chunk { key, .. } = self.attack.source {
            targets.extend(
                [
                    L::Weapon,
                    L::AttackStarted,
                    L::AttackTicks,
                    L::Attribute(self.attack.attributes.attack_damage),
                    L::Attribute(self.attack.attributes.attack_speed),
                ]
                .map(|property| Target::Chunk {
                    key,
                    part: ChunkPart::LivingProperty {
                        actor: self.attack.actor,
                        property,
                    },
                    inventory: None,
                }),
            );
            if matches!(self.attack.actor, crate::actor::ActorId::Player(_)) {
                targets.push(Target::Chunk {
                    key,
                    part: ChunkPart::LivingProperty {
                        actor: self.attack.actor,
                        property: L::GameMode,
                    },
                    inventory: None,
                });
            }
        }
        targets
    }

    pub fn derive(&self, values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
        let attack = &self.attack;
        let mut source_cell = values
            .get(&attack.source)
            .cloned()
            .ok_or(ApplyFailure::Condition(attack.source))?;
        for target in self.targets() {
            if let Some((body, property)) = crate::living_property::body_target(target)
                && body == attack.source
                && let Some(Cell {
                    value: Value::LivingProperty(value),
                    ..
                }) = values.get(&target)
            {
                let living = crate::living_property::state_mut(&mut source_cell.value)
                    .ok_or(ApplyFailure::Type(attack.source))?;
                if !property.write(living, value) {
                    return Err(ApplyFailure::Type(target));
                }
            }
        }
        let source = living_value(&source_cell.value).ok_or(ApplyFailure::Type(attack.source))?;
        let Some(Cell {
            value: Value::Entity(body),
            ..
        }) = values.get(&attack.target)
        else {
            return Err(ApplyFailure::Type(attack.target));
        };
        let crate::entity_body::EntityPayload::Vehicle(_) = &body.payload else {
            return Err(ApplyFailure::Type(attack.target));
        };
        if !self.valid() || !source.belongs_to(attack.actor) {
            return Err(ApplyFailure::InvalidAction);
        }
        let attribute = |id| -> Result<f64, ApplyFailure> {
            let base = source
                .combat
                .effective_attributes
                .get(&id)
                .copied()
                .map(f64::from_bits)
                .ok_or(ApplyFailure::InvalidAction)?;
            let result = source.attribute(id, base);
            if result.is_finite() {
                Ok(result)
            } else {
                Err(ApplyFailure::InvalidAction)
            }
        };
        let speed = attribute(attack.attributes.attack_speed)?
            - if source.combat.equipment.unarmed {
                2.4
            } else {
                0.0
            };
        let cooldown = if attack.pose.bedrock {
            1.0
        } else {
            source.combat.timing.last_attack_tick.map_or(1.0, |last| {
                ((f64::from(attack.tick.distance_since(last)) + 0.5) * speed / 20.0).clamp(0.0, 1.0)
            })
        };
        let sharpness = source.combat.equipment.sharpness;
        let enchantment = if sharpness > 0 {
            0.5 * f64::from(sharpness) + 0.5
        } else {
            0.0
        };
        let damage = (attribute(attack.attributes.attack_damage)?.max(0.0)
            * cooldown.powi(2).mul_add(0.8, 0.2)
            + enchantment * cooldown)
            * 10_000.0;
        if !damage.is_finite() || damage > f64::from(u32::MAX) {
            return Err(ApplyFailure::InvalidAction);
        }
        let Target::Chunk {
            key,
            part: ChunkPart::Entity(actor),
            inventory: None,
        } = attack.target
        else {
            return Err(ApplyFailure::Type(attack.target));
        };
        let damage_target = Target::Chunk {
            key,
            part: ChunkPart::VehicleProperty {
                actor,
                property: P::Damage,
            },
            inventory: None,
        };
        let wobble_target = Target::Chunk {
            key,
            part: ChunkPart::VehicleProperty {
                actor,
                property: P::Wobble,
            },
            inventory: None,
        };
        let Some(Cell {
            value: Value::VehicleProperty(VehiclePropertyValue::Unsigned(before)),
            ..
        }) = values.get(&damage_target)
        else {
            return Err(ApplyFailure::Type(damage_target));
        };
        if *before > 40_000 { return Err(ApplyFailure::InvalidAction); }
        let Some(Cell {
            value: Value::VehicleProperty(VehiclePropertyValue::Wobble { side, .. }),
            ..
        }) = values.get(&wobble_target)
        else {
            return Err(ApplyFailure::Type(wobble_target));
        };
        let creative = if let Target::Chunk { key, .. } = attack.source {
            let target = Target::Chunk {
                key,
                part: ChunkPart::LivingProperty {
                    actor: attack.actor,
                    property: L::GameMode,
                },
                inventory: None,
            };
            matches!(
                values.get(&target).map(|cell| &cell.value),
                Some(Value::LivingProperty(LivingPropertyValue::Unsigned(1)))
            )
        } else {
            false
        };
        let after = if creative {
            (*before).max(40_001)
        } else {
            before
                .checked_add(damage.round() as u32)
                .ok_or(ApplyFailure::Overflow(damage_target))?
        };
        Ok(vec![
            Effect::Put {
                target: Target::Chunk { key, part: ChunkPart::VehicleProperty { actor, property: P::DropLoot }, inventory: None },
                value: Value::VehicleProperty(VehiclePropertyValue::Boolean(!creative)),
            },
            Effect::Put {
                target: damage_target,
                value: Value::VehicleProperty(VehiclePropertyValue::Unsigned(after)),
            },
            Effect::Put {
                target: wobble_target,
                value: Value::VehicleProperty(VehiclePropertyValue::Wobble {
                    ticks: 10,
                    side: -*side,
                }),
            },
        ])
    }
}
