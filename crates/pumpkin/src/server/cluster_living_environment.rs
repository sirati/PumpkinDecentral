use pumpkin_cluster::canonical_action::ApplyFailure;
use pumpkin_cluster::combat_action::{
    EnvironmentalHazardFacts, EnvironmentalHazardKind, MAX_ENVIRONMENTAL_BLOCKS,
};

pub(super) fn capture(
    contact: &pumpkin_cluster::item_contact::ContactObservation,
    eye_height: f64,
    fire_immune: bool,
    moved: bool,
    elapsed: u16,
    minimum_y: i32,
    maximum_y: i32,
    rules: pumpkin_cluster::combat_action::EnvironmentalRules,
    world_rules: Option<pumpkin_cluster::world_rules::WorldRulesWitness>,
    shape_properties: Vec<pumpkin_cluster::living_property::LivingProperty>,
) -> Result<Vec<pumpkin_cluster::combat_action::EnvironmentalDamageIntent>, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    use pumpkin_cluster::combat_action::EnvironmentalDamageIntent;
    use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
    let position = contact.position.map(f64::from_bits);
    let radius = f64::from_bits(contact.half_width);
    let height = f64::from_bits(contact.height);
    if !contact.valid() || !eye_height.is_finite() || eye_height < 0.0 || eye_height > height {
        return Err(ApplyFailure::InvalidAction);
    }
    let min = [
        (position[0] - radius).floor() as i32,
        position[1].floor() as i32,
        (position[2] - radius).floor() as i32,
    ];
    let max = [
        (position[0] + radius).ceil() as i32,
        (position[1] + height).ceil() as i32,
        (position[2] + radius).ceil() as i32,
    ];
    let count = (0..3)
        .try_fold(1_usize, |count, axis| {
            count.checked_mul((max[axis] - min[axis]).max(0) as usize)
        })
        .ok_or(ApplyFailure::ResourceLimit)?;
    if count > MAX_ENVIRONMENTAL_BLOCKS {
        return Err(ApplyFailure::ResourceLimit);
    }
    let kinds = [
        EnvironmentalHazardKind::Fire,
        EnvironmentalHazardKind::Lava,
        EnvironmentalHazardKind::Cactus,
        EnvironmentalHazardKind::SweetBerry,
        EnvironmentalHazardKind::Suffocation,
        EnvironmentalHazardKind::PowderSnow,
        EnvironmentalHazardKind::Water,
    ];
    let mut groups: [Vec<Target>; 7] = core::array::from_fn(|_| Vec::new());
    for x in min[0]..max[0] {
        for y in min[1].max(minimum_y)..max[1].min(maximum_y) {
            for z in min[2]..max[2] {
                let block = BlockPos { x, y, z };
                let key = ChunkKey::new(contact.world, x.div_euclid(16), z.div_euclid(16));
                let target = Target::Chunk {
                    key,
                    part: ChunkPart::Block(block),
                    inventory: None,
                };
                for (index, kind) in kinds.iter().enumerate() {
                    if (*kind == EnvironmentalHazardKind::SweetBerry && !moved)
                        || (*kind == EnvironmentalHazardKind::Suffocation
                            && y != (position[1] + eye_height).floor() as i32)
                    {
                        continue;
                    }
                    groups[index].push(target);
                }
            }
        }
    }
    Ok(kinds
        .into_iter()
        .zip(groups)
        .filter_map(|(kind, mut blocks)| {
            if blocks.is_empty()
                && !(world_rules.is_some()
                    && matches!(
                        kind,
                        EnvironmentalHazardKind::Water | EnvironmentalHazardKind::PowderSnow
                    )
                    && (position[1] + height <= f64::from(minimum_y)
                        || position[1] >= f64::from(maximum_y)))
            {
                return None;
            }
            blocks.sort_unstable();
            let mut support_blocks = if kind == EnvironmentalHazardKind::Water
                && (minimum_y..maximum_y).contains(&max[1])
            {
                (min[0]..max[0])
                    .flat_map(|x| {
                        (min[2]..max[2]).map(move |z| {
                            let block = BlockPos { x, y: max[1], z };
                            Target::Chunk {
                                key: ChunkKey::new(
                                    contact.world,
                                    x.div_euclid(16),
                                    z.div_euclid(16),
                                ),
                                part: ChunkPart::Block(block),
                                inventory: None,
                            }
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            support_blocks.sort_unstable();
            Some(EnvironmentalDamageIntent {
                support_blocks,
                shape_properties: shape_properties.clone(),
                world_rules,
                elapsed,
                rules,
                contact: contact.clone(),
                kind,
                blocks,
                armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
                toughness_attribute: u16::from(
                    pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id,
                ),
                fire_immune,
            })
        })
        .collect())
}

pub(super) fn hazard(
    kind: EnvironmentalHazardKind,
    block_state: u16,
) -> Result<Option<EnvironmentalHazardFacts>, ApplyFailure> {
    let state = pumpkin_data::BlockStateId::new(block_state).ok_or(ApplyFailure::InvalidAction)?;
    let block = pumpkin_data::Block::from_state_id(state);
    let damage = match kind {
        EnvironmentalHazardKind::Fire if matches!(block.name, "fire" | "soul_fire") => {
            Some((1000, 160, false))
        }
        EnvironmentalHazardKind::Lava if block.name == "lava" => Some((4000, 300, false)),
        EnvironmentalHazardKind::Cactus if block.name == "cactus" => Some((1000, 0, false)),
        EnvironmentalHazardKind::SweetBerry
            if block.name == "sweet_berry_bush" && state != block.default_state.id =>
        {
            Some((1000, 0, false))
        }
        EnvironmentalHazardKind::Suffocation
            if state.to_state().is_solid_block() && state.to_state().is_full_cube() =>
        {
            Some((1000, 0, true))
        }
        EnvironmentalHazardKind::PowderSnow if block.name == "powder_snow" => Some((1000, 0, true)),
        EnvironmentalHazardKind::Water
            if pumpkin_world::fluid_snapshot::fluid_from_block_state(state)
                .matches_type(&pumpkin_data::fluid::Fluid::WATER) =>
        {
            Some((0, 0, true))
        }
        _ => None,
    };
    Ok(damage.map(
        |(damage_milli, fire_ticks, bypass_armor)| EnvironmentalHazardFacts {
            damage_milli,
            fire_ticks,
            bypass_armor,
        },
    ))
}

pub(super) fn fall_damage_factor(state: u16, sneaking: bool) -> Result<u16, ApplyFailure> {
    let state = pumpkin_data::BlockStateId::new(state).ok_or(ApplyFailure::InvalidAction)?;
    Ok(match pumpkin_data::Block::from_state_id(state).name {
        "slime_block" if !sneaking => 0,
        "hay_block" | "honey_block" => 200,
        name if name.ends_with("_bed") => 500,
        "water" => 0,
        _ => 1000,
    })
}

#[cfg(test)]
mod tests {
    use super::{fall_damage_factor, hazard};
    use pumpkin_cluster::combat_action::EnvironmentalHazardKind;
    use pumpkin_data::Block;

    #[test]
    fn water_does_not_create_fire_or_direct_contact_damage() {
        let water = Block::WATER.default_state.id.as_u16();
        let facts = hazard(EnvironmentalHazardKind::Water, water)
            .unwrap()
            .unwrap();
        assert_eq!(facts.damage_milli, 0);
        assert_eq!(facts.fire_ticks, 0);
        assert!(
            hazard(EnvironmentalHazardKind::Fire, water)
                .unwrap()
                .is_none()
        );
        assert!(
            hazard(
                EnvironmentalHazardKind::Water,
                Block::STONE.default_state.id.as_u16()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn sneaking_controls_slime_landing_protection() {
        let slime = Block::SLIME_BLOCK.default_state.id.as_u16();
        assert_eq!(fall_damage_factor(slime, false).unwrap(), 0);
        assert_eq!(fall_damage_factor(slime, true).unwrap(), 1000);
        assert_eq!(
            fall_damage_factor(Block::HAY_BLOCK.default_state.id.as_u16(), true).unwrap(),
            200
        );
    }
}

pub(super) fn can_breathe_water(kind: u16) -> bool {
    use pumpkin_data::tag::Taggable;
    pumpkin_data::entity::EntityType::from_raw(kind).is_some_and(|kind| {
        kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_UNDEAD)
            || (kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_AQUATIC)
                && kind.resource_name != "dolphin")
            || kind.resource_name == "frog"
    })
}

#[cfg(test)]
mod respiration_tests {
    use super::can_breathe_water;
    use pumpkin_data::entity::EntityType;

    #[test]
    fn amphibians_and_undead_breathe_water_but_dolphins_need_air() {
        assert!(can_breathe_water(EntityType::FROG.id));
        assert!(can_breathe_water(EntityType::TADPOLE.id));
        assert!(can_breathe_water(EntityType::ZOMBIE.id));
        assert!(can_breathe_water(EntityType::NAUTILUS.id));
        assert!(!can_breathe_water(EntityType::DOLPHIN.id));
        assert!(!can_breathe_water(EntityType::PLAYER.id));
    }
}
pub(super) fn shape_properties(
    value: &pumpkin_cluster::canonical_action::Value,
) -> Result<Vec<pumpkin_cluster::living_property::LivingProperty>, ApplyFailure> {
    use pumpkin_cluster::canonical_action::Value;
    use pumpkin_cluster::living_property::LivingProperty as P;
    let mut properties = match value {
        Value::Living(living) if living.avatar.is_some() => vec![
            P::PlayerStance,
            P::Attribute(u16::from(pumpkin_data::attributes::Attributes::SCALE.id)),
        ],
        Value::Entity(body) => {
            let pumpkin_cluster::entity_body::EntityPayload::Living(living) = &body.payload else {
                return Err(ApplyFailure::InvalidAction);
            };
            let mut properties = vec![P::Attribute(u16::from(
                pumpkin_data::attributes::Attributes::SCALE.id,
            ))];
            if living.mob.is_some() {
                properties.extend([P::MobSpecies, P::MobReproduction]);
            }
            if living.stand.is_some() {
                properties.push(P::ArmorStand(
                    pumpkin_cluster::armor_stand::StandProperty::Flags,
                ));
            }
            properties
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    properties.sort_unstable();
    Ok(properties)
}

pub(super) fn eye_height(
    contact: &pumpkin_cluster::item_contact::ContactObservation,
    value: &pumpkin_cluster::canonical_action::Value,
) -> Result<f64, ApplyFailure> {
    use pumpkin_cluster::canonical_action::Value;
    let (width, height, eye) = match value {
        Value::Living(living) => {
            if living.avatar.is_none() {
                return Err(ApplyFailure::InvalidAction);
            }
            let size = super::cluster_living_metadata::player_dimensions(
                living,
                super::cluster_living_metadata::player_pose(living),
            );
            (
                f64::from(size.width),
                f64::from(size.height),
                f64::from(size.eye_height),
            )
        }
        Value::Entity(body) => {
            let pumpkin_cluster::entity_body::EntityPayload::Living(living) = &body.payload else {
                return Err(ApplyFailure::InvalidAction);
            };
            let size = super::cluster_owned_mob_metadata::dimensions(living, body.kind)
                .ok_or(ApplyFailure::InvalidAction)?;
            (
                f64::from(size.width),
                f64::from(size.height),
                f64::from(size.eye_height),
            )
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if !contact.valid()
        || (f64::from_bits(contact.half_width) * 2.0 - width).abs() > 1.0e-6
        || (f64::from_bits(contact.height) - height).abs() > 1.0e-6
        || eye < 0.0
        || eye > height
    {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(eye.min(height - 1.0e-6))
}

fn shape_value<'a>(
    contact: &pumpkin_cluster::item_contact::ContactObservation,
    property: pumpkin_cluster::living_property::LivingProperty,
    values: &'a std::collections::BTreeMap<
        pumpkin_cluster::canonical_action::Target,
        pumpkin_cluster::canonical_action::Cell,
    >,
) -> Result<&'a pumpkin_cluster::living_property::LivingPropertyValue, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
    let target = Target::Chunk {
        key: contact.chunk,
        part: ChunkPart::LivingProperty {
            actor: contact.actor(),
            property,
        },
        inventory: None,
    };
    let Some(pumpkin_cluster::canonical_action::Cell {
        value: Value::LivingProperty(value),
        ..
    }) = values.get(&target)
    else {
        return Err(ApplyFailure::Condition(target));
    };
    if !property.accepts(value, contact.actor()) {
        return Err(ApplyFailure::Type(target));
    }
    Ok(value)
}

fn winning_eye_height(
    contact: &pumpkin_cluster::item_contact::ContactObservation,
    original: &pumpkin_cluster::canonical_action::Value,
    values: &std::collections::BTreeMap<
        pumpkin_cluster::canonical_action::Target,
        pumpkin_cluster::canonical_action::Cell,
    >,
) -> Result<f64, ApplyFailure> {
    use pumpkin_cluster::canonical_action::Value;
    use pumpkin_cluster::living_property::{LivingProperty as P, LivingPropertyValue as V};
    use pumpkin_data::entity::EntityPose;
    let attribute = &pumpkin_data::attributes::Attributes::SCALE;
    let id = u16::from(attribute.id);
    let V::Attribute {
        base, modifiers, ..
    } = shape_value(contact, P::Attribute(id), values)?
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let scale = pumpkin_cluster::combat_action::CombatEquipmentFacts::attribute_from_sources(
        id,
        base.map(f64::from_bits).unwrap_or(attribute.default_value),
        modifiers,
        &[],
    );
    if !scale.is_finite() || scale <= 0.0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let dimensions = match original {
        Value::Living(living) if living.avatar.is_some() => {
            let V::PlayerStance(stance) = shape_value(contact, P::PlayerStance, values)? else {
                return Err(ApplyFailure::InvalidAction);
            };
            let original_pose = super::cluster_living_metadata::player_pose(living);
            let pose = if matches!(original_pose, EntityPose::Dying | EntityPose::Sleeping) {
                original_pose
            } else if stance.gliding {
                EntityPose::FallFlying
            } else if stance.swimming || stance.crawling {
                EntityPose::Swimming
            } else if stance.sneaking {
                EntityPose::Crouching
            } else {
                EntityPose::Standing
            };
            let dimensions = crate::entity::Entity::get_entity_dimensions(pose);
            let factor = scale.clamp(0.0625, 16.0) as f32;
            pumpkin_util::math::boundingbox::EntityDimensions::new(
                dimensions.width * factor,
                dimensions.height * factor,
                dimensions.eye_height * factor,
            )
        }
        Value::Entity(body) => {
            let pumpkin_cluster::entity_body::EntityPayload::Living(living) = &body.payload else {
                return Err(ApplyFailure::InvalidAction);
            };
            let (species, reproduction) = if living.mob.is_some() {
                let V::MobSpecies(species) = shape_value(contact, P::MobSpecies, values)? else {
                    return Err(ApplyFailure::InvalidAction);
                };
                let V::MobReproduction(reproduction) =
                    shape_value(contact, P::MobReproduction, values)?
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                (
                    Some(species.as_ref().ok_or(ApplyFailure::InvalidAction)?),
                    Some(reproduction.as_ref().ok_or(ApplyFailure::InvalidAction)?),
                )
            } else {
                (None, None)
            };
            let flags = if living.stand.is_some() {
                let V::ArmorStand(Some(pumpkin_cluster::armor_stand::StandPropertyValue::Flags(
                    flags,
                ))) = shape_value(
                    contact,
                    P::ArmorStand(pumpkin_cluster::armor_stand::StandProperty::Flags),
                    values,
                )?
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                Some(*flags)
            } else {
                None
            };
            super::cluster_owned_mob_metadata::dimensions_parts(
                body.kind,
                scale,
                species,
                reproduction,
                flags,
            )
            .ok_or(ApplyFailure::InvalidAction)?
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let width = f64::from(dimensions.width);
    let height = f64::from(dimensions.height);
    let eye = f64::from(dimensions.eye_height);
    if !contact.valid()
        || (f64::from_bits(contact.half_width) * 2.0 - width).abs() > 1.0e-6
        || (f64::from_bits(contact.height) - height).abs() > 1.0e-6
        || eye < 0.0
        || eye > height
    {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(eye.min(height - 1.0e-6))
}

pub(super) fn contact<S: pumpkin_cluster::canonical_action::ActionState + ?Sized>(
    context: &S,
    intent: &pumpkin_cluster::combat_action::EnvironmentalDamageIntent,
    values: &std::collections::BTreeMap<
        pumpkin_cluster::canonical_action::Target,
        pumpkin_cluster::canonical_action::Cell,
    >,
) -> Result<pumpkin_cluster::combat_action::EnvironmentalContactFacts, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
    use pumpkin_cluster::combat_action::{EnvironmentalContactFacts, EnvironmentalHazardKind};
    use pumpkin_cluster::item_contact::ContactIdentity;
    use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
    use pumpkin_data::tag::Taggable;
    use pumpkin_util::math::position::BlockPos as WorldPos;
    let body = &values
        .get(&intent.contact.target())
        .ok_or(ApplyFailure::Condition(intent.contact.target()))?
        .value;
    let identity_matches = match (&intent.contact.identity, body) {
        (ContactIdentity::Player(lease), Value::Living(living)) => {
            living.belongs_to(pumpkin_cluster::actor::ActorId::Player(lease.gid))
                && living
                    .avatar
                    .as_ref()
                    .is_some_and(|avatar| avatar.spatial_epoch == intent.contact.epoch)
        }
        (ContactIdentity::Entity { id, birth }, Value::Entity(body)) => {
            body.id == *id
                && body.birth == *birth
                && body.owner == intent.contact.owner
                && body.spatial_epoch == intent.contact.epoch
        }
        _ => false,
    };
    if !identity_matches || shape_properties(body)? != intent.shape_properties {
        return Err(ApplyFailure::InvalidAction);
    }
    let eye_height = winning_eye_height(&intent.contact, body, values)?;
    let freeze_damage_milli = match body {
        Value::Entity(body)
            if pumpkin_data::entity::EntityType::from_raw(body.kind).is_some_and(|kind| {
                kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_FREEZE_HURTS_EXTRA_TYPES)
            }) =>
        {
            5000
        }
        _ => 1000,
    };
    let mut facts = EnvironmentalContactFacts {
        touching_water: false,
        immersed: false,
        breathable: false,
        freeze_damage_milli,
    };
    if intent.kind != EnvironmentalHazardKind::Water {
        return Ok(facts);
    }
    let witness = intent.world_rules.ok_or(ApplyFailure::InvalidAction)?;
    let policy = context.world_rules(witness, intent.contact.simulation_tick)?;
    if witness.rules.world != intent.contact.world {
        return Err(ApplyFailure::InvalidAction);
    }
    let minimum = policy.min_y;
    let maximum = i32::try_from(i64::from(minimum) + i64::from(policy.height))
        .map_err(|_| ApplyFailure::InvalidAction)?;
    let position = intent.contact.position.map(f64::from_bits);
    let radius = f64::from_bits(intent.contact.half_width);
    let height = f64::from_bits(intent.contact.height);
    let min = [
        (position[0] - radius).floor() as i32,
        position[1].floor() as i32,
        (position[2] - radius).floor() as i32,
    ];
    let max = [
        (position[0] + radius).ceil() as i32,
        (position[1] + height).ceil() as i32,
        (position[2] + radius).ceil() as i32,
    ];
    let target = |block: BlockPos| Target::Chunk {
        key: ChunkKey::new(
            intent.contact.world,
            block.x.div_euclid(16),
            block.z.div_euclid(16),
        ),
        part: ChunkPart::Block(block),
        inventory: None,
    };
    let mut expected = (min[0]..max[0])
        .flat_map(|x| {
            (min[1].max(minimum)..max[1].min(maximum))
                .flat_map(move |y| (min[2]..max[2]).map(move |z| target(BlockPos { x, y, z })))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let mut supports = if (minimum..maximum).contains(&max[1]) {
        (min[0]..max[0])
            .flat_map(|x| (min[2]..max[2]).map(move |z| target(BlockPos { x, y: max[1], z })))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    supports.sort_unstable();
    if expected != intent.blocks || supports != intent.support_blocks {
        return Err(ApplyFailure::InvalidAction);
    }
    for target in intent.blocks.iter().chain(&intent.support_blocks) {
        if !matches!(
            values.get(target),
            Some(pumpkin_cluster::canonical_action::Cell {
                value: Value::Block(_),
                ..
            })
        ) {
            return Err(ApplyFailure::Condition(*target));
        }
    }
    let read = |position: WorldPos| -> Result<pumpkin_data::BlockStateId, ApplyFailure> {
        if !(minimum..maximum).contains(&position.0.y) {
            return Ok(pumpkin_data::Block::AIR.default_state.id);
        }
        let target = target(BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        });
        let Some(pumpkin_cluster::canonical_action::Cell {
            value: Value::Block(state),
            ..
        }) = values.get(&target)
        else {
            return Err(ApplyFailure::Condition(target));
        };
        pumpkin_data::BlockStateId::new(*state).ok_or(ApplyFailure::InvalidAction)
    };
    let contacts = pumpkin_world::fluid_snapshot::sample_entity_fluid_contacts(
        pumpkin_util::math::boundingbox::BoundingBox::new_array(
            [position[0] - radius, position[1], position[2] - radius],
            [
                position[0] + radius,
                position[1] + height,
                position[2] + radius,
            ],
        ),
        MAX_ENVIRONMENTAL_BLOCKS,
        false,
        read,
    )
    .map_err(|failure| match failure {
        pumpkin_world::fluid_snapshot::FluidReadFailure::Read { reason, .. } => reason,
        pumpkin_world::fluid_snapshot::FluidReadFailure::Budget => ApplyFailure::ResourceLimit,
        _ => ApplyFailure::InvalidAction,
    })?;
    facts.touching_water = contacts.contacts[0].touching;
    let eye = position[1] + eye_height;
    let eye_pos = WorldPos::new(
        position[0].floor() as i32,
        eye.floor() as i32,
        position[2].floor() as i32,
    );
    let eye_state = read(eye_pos)?;
    let (fluid, state) = pumpkin_world::fluid_snapshot::fluid_state_from_block_state(eye_state);
    if fluid.matches_type(&pumpkin_data::fluid::Fluid::WATER) {
        let surface = pumpkin_world::fluid_snapshot::fluid_height(eye_pos, fluid, &state, read)
            .map_err(|failure| match failure {
                pumpkin_world::fluid_snapshot::FluidReadFailure::Read { reason, .. } => reason,
                _ => ApplyFailure::InvalidAction,
            })?;
        facts.immersed = eye < f64::from(eye_pos.0.y) + f64::from(surface);
        facts.breathable =
            facts.immersed && eye_state.to_block() == &pumpkin_data::Block::BUBBLE_COLUMN;
    }
    Ok(facts)
}

#[cfg(test)]
mod fluid_contact_tests {
    use super::*;
    use pumpkin_cluster::canonical_action::{
        ActionState, Cell, ChunkPart, LivePlayerLease, PlayerKey, SpatialEpoch, Target, TruthView,
        Value,
    };
    use pumpkin_cluster::combat_action::{EnvironmentalDamageIntent, EnvironmentalRules};
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::item_action::PlayerAvatarState;
    use pumpkin_cluster::item_contact::{ContactIdentity, ContactObservation};
    use pumpkin_cluster::living_property::{LivingProperty as P, LivingPropertyValue as V};
    use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
    use pumpkin_cluster::realm_completion::PeerGeneration;
    use pumpkin_cluster::time::TickStamp;
    use pumpkin_cluster::world_rules::{WorldRulesId, WorldRulesSnapshot, WorldRulesWitness};
    use std::collections::BTreeMap;

    struct Registry(WorldRulesSnapshot);
    impl ActionState for Registry {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> {
            None
        }
        fn commit(
            &mut self,
            _: pumpkin_cluster::canonical_action::ActionContext,
            _: TruthView,
            _: &[pumpkin_cluster::canonical_action::AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
        fn world_rules(
            &self,
            witness: WorldRulesWitness,
            tick: TickStamp,
        ) -> Result<WorldRulesSnapshot, ApplyFailure> {
            if witness.rules == self.0.rules && witness.slot as u16 == tick.0 {
                Ok(self.0.clone())
            } else {
                Err(ApplyFailure::InvalidAction)
            }
        }
    }
    fn fixture(x: f64) -> (Registry, EnvironmentalDamageIntent, BTreeMap<Target, Cell>) {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let owner = PeerGeneration {
            server: gid.server,
            generation: 3,
        };
        let world = WorldId {
            dimension: 0,
            key: 1,
        };
        let chunk = ChunkKey::new(world, 0, 0);
        let lease = LivePlayerLease {
            gid,
            key: PlayerKey(uuid::Uuid::from_u128(4)),
            incarnation: 5,
        };
        let epoch = SpatialEpoch::Admission {
            lease,
            admitted_at: TickStamp(0),
        };
        let position = [x.to_bits(), 0_f64.to_bits(), 0.5_f64.to_bits()];
        let contact = ContactObservation {
            identity: ContactIdentity::Player(lease),
            owner,
            epoch,
            world,
            chunk,
            position,
            half_width: 0.3_f64.to_bits(),
            height: 1.8_f64.to_bits(),
            simulation_tick: TickStamp(10),
            observed_sequence: PlayerSeq(0),
        };
        let mut living = super::super::cluster_actor_import::from_player_nbt(gid, None)
            .unwrap()
            .state;
        living.avatar = Some(PlayerAvatarState {
            stance: Default::default(),
            mount: None,
            relation_versions: Default::default(),
            experience: Default::default(),
            statistics: Default::default(),
            presentation: Default::default(),
            spatial_epoch: epoch,
            world,
            position,
            yaw: 0,
            head_yaw: 0,
            pitch: 0,
            on_ground: true,
            movement_tick: TickStamp(10),
            movement_sequence: PlayerSeq(0),
            game_mode: 0,
            fall_distance: 0,
            on_fire: false,
        });
        let rules = WorldRulesSnapshot {
            rules: WorldRulesId {
                world,
                issuer: owner,
                epoch: 1,
            },
            configuration: pumpkin_cluster::world_rules::WorldConfigurationVersion {
                issuer: owner,
                revision: 1,
                from_slot: 0,
            },
            from_slot: 0,
            keep_inventory: false,
            do_entity_drops: true,
            do_mob_loot: true,
            mob_griefing: true,
            tnt_explodes: true,
            random_tick_speed: 3,
            natural_regeneration: true,
            advance_time: true,
            advance_weather: true,
            players_sleeping_percentage: 100,
            do_mob_spawning: true,
            water_source_conversion: true,
            lava_source_conversion: false,
            fire_spread_radius_around_player: 128,
            min_y: -64,
            height: 384,
            fire_damage: true,
            drowning_damage: true,
            freeze_damage: true,
            fall_damage: true,
            difficulty: 2,
        };
        let witness = WorldRulesWitness {
            rules: rules.rules,
            slot: 10,
        };
        let intent = capture(
            &contact,
            1.62,
            false,
            false,
            1,
            -64,
            320,
            EnvironmentalRules {
                fire_damage: true,
                drowning_damage: true,
                freeze_damage: true,
                water_breathing_effect: 0,
                can_breathe_water: false,
                freeze_immune: false,
            },
            Some(witness),
            vec![
                P::PlayerStance,
                P::Attribute(u16::from(pumpkin_data::attributes::Attributes::SCALE.id)),
            ],
        )
        .unwrap()
        .into_iter()
        .find(|intent| intent.kind == EnvironmentalHazardKind::Water)
        .unwrap();
        let mut values = intent
            .blocks
            .iter()
            .chain(&intent.support_blocks)
            .map(|target| {
                (
                    *target,
                    Cell {
                        revision: 0,
                        value: Value::Block(pumpkin_data::Block::AIR.default_state.id.as_u16()),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let scale = P::Attribute(u16::from(pumpkin_data::attributes::Attributes::SCALE.id));
        values.insert(
            Target::Chunk {
                key: chunk,
                part: ChunkPart::LivingProperty {
                    actor: contact.actor(),
                    property: scale,
                },
                inventory: None,
            },
            Cell {
                revision: 0,
                value: Value::LivingProperty(scale.read(&living)),
            },
        );
        values.insert(
            contact.target(),
            Cell {
                revision: 0,
                value: Value::Living((living).into()),
            },
        );
        values.insert(
            Target::Chunk {
                key: chunk,
                part: ChunkPart::LivingProperty {
                    actor: contact.actor(),
                    property: P::PlayerStance,
                },
                inventory: None,
            },
            Cell {
                revision: 0,
                value: Value::LivingProperty(V::PlayerStance(Default::default())),
            },
        );
        (Registry(rules), intent, values)
    }
    fn set(
        values: &mut BTreeMap<Target, Cell>,
        intent: &EnvironmentalDamageIntent,
        x: i32,
        y: i32,
        state: pumpkin_data::BlockStateId,
    ) {
        let target = Target::Chunk {
            key: ChunkKey::new(intent.contact.world, x.div_euclid(16), 0),
            part: ChunkPart::Block(BlockPos { x, y, z: 0 }),
            inventory: None,
        };
        assert!(values.contains_key(&target));
        values.insert(
            target,
            Cell {
                revision: 1,
                value: Value::Block(state.as_u16()),
            },
        );
    }
    #[test]
    fn feet_and_neighbor_water_extinguish_without_submerging_eyes() {
        let (registry, intent, mut values) = fixture(0.5);
        set(
            &mut values,
            &intent,
            0,
            0,
            pumpkin_data::Block::WATER.default_state.id,
        );
        let facts = contact(&registry, &intent, &values).unwrap();
        assert!(facts.touching_water);
        assert!(!facts.immersed);
        let (registry, intent, mut values) = fixture(0.95);
        set(
            &mut values,
            &intent,
            1,
            1,
            pumpkin_data::Block::WATER.default_state.id,
        );
        let facts = contact(&registry, &intent, &values).unwrap();
        assert!(facts.touching_water);
        assert!(!facts.immersed);
    }
    #[test]
    fn shallow_flow_and_above_support_determine_eye_depth() {
        use pumpkin_data::block_properties::WaterLikeProperties;
        let (registry, intent, mut values) = fixture(0.5);
        let shallow = WaterLikeProperties { level: 7 }.to_state_id(&pumpkin_data::Block::WATER);
        set(&mut values, &intent, 0, 1, shallow);
        assert!(!contact(&registry, &intent, &values).unwrap().immersed);
        set(
            &mut values,
            &intent,
            0,
            2,
            pumpkin_data::Block::WATER.default_state.id,
        );
        assert!(contact(&registry, &intent, &values).unwrap().immersed);
        values.remove(&intent.support_blocks[0]);
        assert_eq!(
            contact(&registry, &intent, &values),
            Err(ApplyFailure::Condition(intent.support_blocks[0]))
        );
    }
    #[test]
    fn aquatic_plants_drown_and_bubble_columns_restore_air() {
        for block in [
            &pumpkin_data::Block::KELP,
            &pumpkin_data::Block::SEAGRASS,
            &pumpkin_data::Block::BUBBLE_COLUMN,
        ] {
            let (registry, intent, mut values) = fixture(0.5);
            set(&mut values, &intent, 0, 1, block.default_state.id);
            let facts = contact(&registry, &intent, &values).unwrap();
            assert!(facts.immersed);
            assert!(facts.touching_water);
            assert_eq!(
                facts.breathable,
                block == &pumpkin_data::Block::BUBBLE_COLUMN
            );
        }
    }
    #[test]
    fn geometry_budget_wide_low_water_capture_keeps_the_entire_support_plane() {
        let (_, mut template, _) = fixture(0.5);
        template.contact.half_width = 10.0_f64.to_bits();
        template.contact.height = 1.0_f64.to_bits();
        let intents = capture(
            &template.contact,
            0.5,
            false,
            true,
            1,
            -64,
            320,
            template.rules,
            template.world_rules,
            template.shape_properties,
        )
        .unwrap();
        let water = intents
            .into_iter()
            .find(|intent| intent.kind == EnvironmentalHazardKind::Water)
            .unwrap();
        assert_eq!(water.blocks.len(), 441);
        assert_eq!(water.support_blocks.len(), 441);
        assert!(water.is_valid(template.contact.simulation_tick));
    }
}

pub(super) fn geometry_resource_limit(position: [u64; 3], width: f32, height: f32) -> bool {
    let position = position.map(f64::from_bits);
    let radius = f64::from(width) * 0.5;
    let height = f64::from(height);
    if position
        .into_iter()
        .any(|coordinate| !coordinate.is_finite())
        || !radius.is_finite()
        || !height.is_finite()
        || radius < 0.0
        || height < 0.0
        || radius > 1024.0
        || height > 2048.0
    {
        return true;
    }
    if radius == 0.0 || height == 0.0 {
        return false;
    }
    [
        (position[0] + radius).ceil() - (position[0] - radius).floor(),
        (position[1] + height).ceil() - position[1].floor(),
        (position[2] + radius).ceil() - (position[2] - radius).floor(),
    ]
    .into_iter()
    .try_fold(1_usize, |count, span| count.checked_mul(span as usize))
    .is_none_or(|count| count > MAX_ENVIRONMENTAL_BLOCKS)
}

#[cfg(test)]
mod geometry_budget_tests {
    use super::geometry_resource_limit;

    #[test]
    fn scaled_shapes_use_cell_volume_instead_of_a_sixteen_block_height_cutoff() {
        let position = [0.5_f64.to_bits(), 80.0_f64.to_bits(), 0.5_f64.to_bits()];
        assert!(!geometry_resource_limit(position, 9.6, 28.8));
        assert!(!geometry_resource_limit(position, 20.0, 1.0));
        assert!(geometry_resource_limit(position, 20.0, 20.0));
    }

    #[test]
    fn fractional_alignment_counts_every_intersected_block() {
        let aligned = [0.0_f64.to_bits(), 80.0_f64.to_bits(), 0.0_f64.to_bits()];
        let fractional = [0.5_f64.to_bits(), 80.5_f64.to_bits(), 0.5_f64.to_bits()];
        assert!(!geometry_resource_limit(aligned, 16.0, 19.0));
        assert!(geometry_resource_limit(fractional, 16.0, 19.0));
    }
}
