use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_data::attributes::Attributes;
use pumpkin_protocol::bedrock::client::update_attributes::AttributeData;
use pumpkin_protocol::java::client::play::{AttributeModifier, Property};

pub(super) fn player_attributes_presentation_equal(
    before: &ActorLivingState,
    after: &ActorLivingState,
) -> bool {
    before.combat.effective_attributes == after.combat.effective_attributes
        && before.combat.equipment.modifiers == after.combat.equipment.modifiers
        && before.permanent_attribute_modifiers == after.permanent_attribute_modifiers
}

fn base(state: &ActorLivingState, attribute: &Attributes) -> f64 {
    state
        .combat
        .effective_attributes
        .get(&u16::from(attribute.id))
        .copied()
        .map(f64::from_bits)
        .unwrap_or_else(|| {
            pumpkin_data::entity::EntityType::PLAYER
                .attributes
                .iter()
                .find(|(candidate, _)| candidate.id == attribute.id)
                .map_or(attribute.default_value, |(_, value)| *value)
        })
}

fn bedrock_name(attribute: &Attributes) -> Option<&'static str> {
    Some(match attribute.id {
        id if id == Attributes::MOVEMENT_SPEED.id => "minecraft:movement",
        id if id == Attributes::MAX_HEALTH.id => "minecraft:health",
        id if id == Attributes::MAX_ABSORPTION.id => "minecraft:absorption",
        id if id == Attributes::ATTACK_DAMAGE.id => "minecraft:attack_damage",
        id if id == Attributes::KNOCKBACK_RESISTANCE.id => "minecraft:knockback_resistance",
        id if id == Attributes::LUCK.id => "minecraft:luck",
        id if id == Attributes::FOLLOW_RANGE.id => "minecraft:follow_range",
        id if id == Attributes::JUMP_STRENGTH.id => "minecraft:horse.jump_strength",
        _ => return None,
    })
}

fn projections(
    before: Option<&ActorLivingState>,
    after: &ActorLivingState,
    mask_health: bool,
) -> Result<(Vec<Property>, Vec<AttributeData>), String> {
    let mut ids = std::collections::BTreeSet::new();
    for state in before.into_iter().chain(std::iter::once(after)) {
        ids.extend(state.combat.effective_attributes.keys().copied());
        ids.extend(state.permanent_attribute_modifiers.keys().copied());
        ids.extend(
            state
                .combat
                .equipment
                .modifiers
                .iter()
                .map(|modifier| modifier.attribute),
        );
    }
    let mut java = Vec::new();
    let mut bedrock = Vec::new();
    for id in ids {
        let attribute = Attributes::ALL
            .iter()
            .find(|attribute| u16::from(attribute.id) == id)
            .ok_or("unknown player attribute")?;
        let health = attribute.id == Attributes::MAX_HEALTH.id;
        let absorption = attribute.id == Attributes::MAX_ABSORPTION.id;
        if mask_health && (health || absorption) {
            continue;
        }
        let value = base(after, attribute);
        let modifiers = after.attribute_modifiers(id);
        let effective = after.attribute(id, value);
        if !value.is_finite()
            || !effective.is_finite()
            || modifiers
                .iter()
                .any(|modifier| !f64::from_bits(modifier.amount).is_finite())
        {
            return Err("invalid player attribute".to_owned());
        }
        let unchanged = before.is_some_and(|before| {
            base(before, attribute).to_bits() == value.to_bits()
                && before.attribute_modifiers(id) == modifiers
        });
        if !unchanged {
            java.push(Property::new(
                i32::from(id).into(),
                value,
                modifiers
                    .iter()
                    .map(|modifier| {
                        AttributeModifier::new(
                modifier.id.clone(), f64::from_bits(modifier.amount), match modifier.operation {
                    pumpkin_cluster::combat_action::CombatModifierOperation::Add => 0,
                    pumpkin_cluster::combat_action::CombatModifierOperation::MultiplyBase => 1,
                    pumpkin_cluster::combat_action::CombatModifierOperation::MultiplyTotal => 2,
                },
            )
                    })
                    .collect(),
            ));
        }
        let vitals_unchanged = before.is_some_and(|before| {
            (!health || before.combat.health_milli == after.combat.health_milli)
                && (!absorption || before.combat.absorption_milli == after.combat.absorption_milli)
        });
        if unchanged && vitals_unchanged {
            continue;
        }
        let Some(name) = bedrock_name(attribute) else {
            continue;
        };
        let float = |value: f64| value.clamp(f64::from(f32::MIN), f64::from(f32::MAX)) as f32;
        let current = if health {
            after.combat.health_milli as f64 / 1000.0
        } else if absorption {
            after.combat.absorption_milli as f64 / 1000.0
        } else {
            effective
        };
        bedrock.push(AttributeData {
            min_value: if health || absorption { 0.0 } else { f32::MIN },
            max_value: if health || absorption {
                float(effective.max(current).max(0.0))
            } else {
                f32::MAX
            },
            current_value: float(current),
            default_min_value: if health || absorption { 0.0 } else { f32::MIN },
            default_max_value: if health || absorption {
                float(value.max(0.0))
            } else {
                f32::MAX
            },
            default_value: float(value),
            name: name.to_owned(),
            modifiers: Vec::new(),
        });
    }
    Ok((java, bedrock))
}

pub(super) fn prepare_player_attributes(
    client: &crate::net::ClientPlatform,
    entity_id: i32,
    before: Option<&ActorLivingState>,
    after: &ActorLivingState,
    tick: pumpkin_cluster::time::TickStamp,
    mask_health: bool,
) -> Result<Vec<bytes::Bytes>, String> {
    let (java, bedrock) = projections(before, after, mask_health)?;
    match client {
        crate::net::ClientPlatform::Java(client) if !java.is_empty() => client
            .serialize_packet(
                &pumpkin_protocol::java::client::play::CUpdateAttributes::new(
                    entity_id.into(),
                    java,
                ),
            )
            .map(|packet| vec![packet])
            .map_err(|error| error.to_string()),
        crate::net::ClientPlatform::Bedrock(client) if !bedrock.is_empty() => client
            .serialize_packet(
                &pumpkin_protocol::bedrock::client::update_attributes::CUpdateAttributes {
                    target_runtime_id: (entity_id as u64).into(),
                    attribute_list: bedrock,
                    tick: u64::from(tick.0).into(),
                },
            )
            .map(|packet| vec![packet])
            .map_err(|error| error.to_string()),
        _ => Ok(Vec::new()),
    }
}

pub(super) fn publish_player_attributes(
    client: &crate::net::ClientPlatform,
    entity_id: i32,
    before: Option<&ActorLivingState>,
    after: &ActorLivingState,
    tick: pumpkin_cluster::time::TickStamp,
    mask_health: bool,
) -> bool {
    let Ok(packets) =
        prepare_player_attributes(client, entity_id, before, after, tick, mask_health)
    else {
        return false;
    };
    for packet in packets {
        client.try_enqueue_packet_data(packet);
    }
    !client.closed()
}

#[cfg(test)]
mod replica_attribute_tests {
    use super::*;
    use pumpkin_cluster::combat_action::{CombatAttributeModifier, CombatModifierOperation};
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};

    fn living() -> ActorLivingState {
        let mut state = super::super::cluster_actor_import::from_player_nbt(
            GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            None,
        )
        .unwrap()
        .state;
        state
            .combat
            .effective_attributes
            .insert(u16::from(Attributes::ATTACK_DAMAGE.id), 4.0_f64.to_bits());
        state
            .combat
            .effective_attributes
            .insert(u16::from(Attributes::MAX_HEALTH.id), 20.0_f64.to_bits());
        state.combat.health_milli = 7_000;
        state
    }

    #[test]
    fn permanent_and_gear_attributes_project_once_and_undo_on_both_protocols() {
        use pumpkin_protocol::{ClientPacket, ServerPacket};
        let mut original = living();
        let id = u16::from(Attributes::ATTACK_DAMAGE.id);
        original.combat.equipment.modifiers = vec![
            CombatAttributeModifier {
                attribute: id,
                slot: 0,
                id: "minecraft:shared".to_owned(),
                amount: 1.0_f64.to_bits(),
                operation: CombatModifierOperation::Add,
            },
            CombatAttributeModifier {
                attribute: id,
                slot: 0,
                id: "minecraft:multiplier".to_owned(),
                amount: 0.5_f64.to_bits(),
                operation: CombatModifierOperation::MultiplyTotal,
            },
        ];
        let mut applied = original.clone();
        applied.permanent_attribute_modifiers.insert(
            id,
            vec![CombatAttributeModifier {
                attribute: id,
                slot: 0,
                id: "minecraft:shared".to_owned(),
                amount: 2.0_f64.to_bits(),
                operation: CombatModifierOperation::Add,
            }],
        );
        let (java, bedrock) = projections(Some(&original), &applied, false).unwrap();
        assert_eq!(java.len(), 1);
        assert_eq!(java[0].value, 4.0);
        assert_eq!(java[0].modifiers.len(), 2);
        let attack = bedrock
            .iter()
            .find(|attribute| attribute.name == "minecraft:attack_damage")
            .unwrap();
        assert_eq!(attack.current_value, 9.0);
        assert!(attack.modifiers.is_empty());
        for version in [
            pumpkin_util::version::JavaMinecraftVersion::V_1_8,
            pumpkin_util::version::JavaMinecraftVersion::V_1_21_11,
        ] {
            let packet = pumpkin_protocol::java::client::play::CUpdateAttributes::new(
                35.into(),
                java.clone(),
            );
            let mut encoded = Vec::new();
            packet.write_packet_data(&mut encoded, &version).unwrap();
            let mut input = encoded.as_slice();
            let decoded =
                <pumpkin_protocol::java::client::play::CUpdateAttributes as ServerPacket>::read(
                    &mut input, &version,
                )
                .unwrap();
            assert_eq!(decoded.entity_id.0, 35);
            assert_eq!(decoded.properties[0].value, 4.0);
            assert_eq!(decoded.properties[0].modifiers.len(), 2);
            assert!(input.is_empty());
        }
        let (_, undone) = projections(Some(&applied), &original, false).unwrap();
        assert_eq!(
            undone
                .iter()
                .find(|attribute| attribute.name == "minecraft:attack_damage")
                .unwrap()
                .current_value,
            7.5
        );
        assert!(player_attributes_presentation_equal(&original, &original));
        assert!(!player_attributes_presentation_equal(&original, &applied));
    }

    #[test]
    fn remote_health_mask_omits_numeric_attributes_and_unmasked_health_updates() {
        let original = living();
        let (java, bedrock) = projections(None, &original, true).unwrap();
        assert!(
            java.iter()
                .all(|property| property.id.0 != i32::from(Attributes::MAX_HEALTH.id))
        );
        assert!(
            bedrock
                .iter()
                .all(|attribute| attribute.name != "minecraft:health")
        );
        let mut hurt = original.clone();
        hurt.combat.health_milli = 3_000;
        let (java, bedrock) = projections(Some(&original), &hurt, false).unwrap();
        assert!(java.is_empty());
        assert_eq!(bedrock.len(), 1);
        assert_eq!(bedrock[0].name, "minecraft:health");
        assert_eq!(bedrock[0].current_value, 3.0);
        assert_eq!(bedrock[0].max_value, 20.0);
        assert!(
            projections(Some(&original), &hurt, true)
                .unwrap()
                .1
                .is_empty()
        );
    }
}

#[cfg(test)]
mod bedrock_attribute_wire_tests {
    use super::*;
    use pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder;
    use pumpkin_protocol::codec::{var_uint::VarUInt, var_ulong::VarULong};
    use pumpkin_protocol::serial::PacketRead;

    #[test]
    fn remote_bedrock_attribute_wire_uses_effective_value_without_duplicate_modifiers() {
        let mut state = super::super::cluster_actor_import::from_player_nbt(
            pumpkin_cluster::identity::GlobalPlayerId::new(
                pumpkin_cluster::identity::ServerId(2),
                pumpkin_cluster::identity::PlayerSlot(7),
            ),
            None,
        )
        .unwrap()
        .state;
        let id = u16::from(Attributes::ATTACK_DAMAGE.id);
        state
            .combat
            .effective_attributes
            .insert(id, 4.0_f64.to_bits());
        state.permanent_attribute_modifiers.insert(
            id,
            vec![pumpkin_cluster::combat_action::CombatAttributeModifier {
                attribute: id,
                slot: 0,
                id: "minecraft:bonus".to_owned(),
                amount: 3.0_f64.to_bits(),
                operation: pumpkin_cluster::combat_action::CombatModifierOperation::Add,
            }],
        );
        let (_, attributes) = projections(None, &state, true).unwrap();
        let encoded = BedrockBatchEncoder::new()
            .serialize_packet(
                &pumpkin_protocol::bedrock::client::update_attributes::CUpdateAttributes {
                    target_runtime_id: 35_u64.into(),
                    attribute_list: attributes,
                    tick: 21_u64.into(),
                },
            )
            .unwrap();
        assert_eq!(encoded[0], 254);
        let mut input = std::io::Cursor::new(&encoded[1..]);
        let _length = VarUInt::read(&mut input).unwrap();
        assert_eq!(VarUInt::read(&mut input).unwrap().0, 29);
        assert_eq!(VarULong::read(&mut input).unwrap().0, 35);
        assert_eq!(VarUInt::read(&mut input).unwrap().0, 1);
        let _minimum = f32::read(&mut input).unwrap();
        let _maximum = f32::read(&mut input).unwrap();
        assert_eq!(f32::read(&mut input).unwrap(), 7.0);
        let _default_minimum = f32::read(&mut input).unwrap();
        let _default_maximum = f32::read(&mut input).unwrap();
        assert_eq!(f32::read(&mut input).unwrap(), 4.0);
        assert_eq!(String::read(&mut input).unwrap(), "minecraft:attack_damage");
        assert_eq!(VarUInt::read(&mut input).unwrap().0, 0);
        assert_eq!(VarULong::read(&mut input).unwrap().0, 21);
        assert_eq!(input.position() as usize, encoded.len() - 1);
    }
}
