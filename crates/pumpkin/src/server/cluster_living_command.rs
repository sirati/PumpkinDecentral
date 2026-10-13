use pumpkin_cluster::action_command::ActorReference;
use pumpkin_cluster::canonical_action::{
    ActionState, ApplyFailure, Cell, Target, TruthView, Value,
};
use pumpkin_cluster::combat_command_living::{
    EnvironmentalRegistryFacts, LivingCommandState, LivingObservation, LivingRegistryFacts,
};
use pumpkin_cluster::entity_body::EntityPayload;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::tag::Taggable;
use std::collections::BTreeMap;

use super::cluster_world_callback_snapshot::CallbackTickSnapshot;

fn original_value(
    snapshot: &CallbackTickSnapshot,
    source: &LivingObservation,
    tick: TickStamp,
) -> Result<Value, ApplyFailure> {
    let identity = source.identity(tick)?;
    let target = identity.target();
    if tick != snapshot.original_tick() {
        return Err(ApplyFailure::InvalidAction);
    }
    let value = snapshot
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?
        .value;
    let (position, sequence) = match (&source.actor, &value) {
        (
            ActorReference::Player {
                lease,
                chunk,
                epoch,
                owner,
            },
            Value::Living(living),
        ) => {
            if snapshot.player_association(*lease) != Some((*chunk, *owner))
                || living.avatar.as_ref().is_none_or(|avatar| {
                    avatar.spatial_epoch != *epoch
                        || living.combat.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let sample = snapshot
                .player_projection(lease.gid)
                .ok_or(ApplyFailure::Condition(target))?;
            (
                sample.frame.effective.position.map(f64::to_bits),
                sample.frame.effective.source_seq,
            )
        }
        (
            ActorReference::Entity {
                id,
                birth,
                chunk,
                epoch,
                owner,
            },
            Value::Entity(body),
        ) => {
            if body.id != *id
                || body.birth != *birth
                || body.owner != *owner
                || body.spatial_epoch != *epoch
                || body.spatial.world != chunk.world
                || !matches!(body.payload, EntityPayload::Living(_))
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let sample = snapshot
                .entity_effective_spatial(*id)
                .ok_or(ApplyFailure::Condition(target))?;
            let record = snapshot.original_entity_sample(*id).ok_or(ApplyFailure::Condition(target))?;
            if !record.matches_body(body) || record.spatial != *sample
                || record.issuer != *owner || record.tick.is_after(tick) {
                return Err(ApplyFailure::InvalidAction);
            }
            (sample.position, record.sequence)
        }
        _ => return Err(ApplyFailure::Type(target)),
    };
    if source.position != position || source.sequence != sequence {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(value)
}

fn fire_immune(value: &Value) -> bool {
    matches!(value, Value::Entity(body) if pumpkin_data::entity::EntityType::from_raw(body.kind)
        .is_some_and(|kind| kind.fire_immune))
}

impl LivingCommandState for CallbackTickSnapshot {
    fn living_registry(
        &self,
        source: &LivingObservation,
        tick: TickStamp,
        _values: &BTreeMap<Target, Cell>,
    ) -> Result<LivingRegistryFacts, ApplyFailure> {
        use pumpkin_data::attributes::Attributes as A;
        use pumpkin_data::effect::StatusEffect as E;
        let value = original_value(self, source, tick)?;
        Ok(LivingRegistryFacts {
            attributes: self.inventory_equipment_attribute_ids(),
            armor: u16::from(A::ARMOR.id),
            toughness: u16::from(A::ARMOR_TOUGHNESS.id),
            max_health: u16::from(A::MAX_HEALTH.id),
            regeneration: u16::from(E::REGENERATION.id),
            poison: u16::from(E::POISON.id),
            wither: u16::from(E::WITHER.id),
            hunger: u16::from(E::HUNGER.id),
            fire_immune: fire_immune(&value),
        })
    }

    fn environmental_registry(
        &self,
        source: &LivingObservation,
        tick: TickStamp,
        _values: &BTreeMap<Target, Cell>,
    ) -> Result<EnvironmentalRegistryFacts, ApplyFailure> {
        use pumpkin_data::attributes::Attributes as A;
        let value = original_value(self, source, tick)?;
        let size = match &value {
            Value::Living(living) => super::cluster_living_metadata::player_dimensions(
                living,
                super::cluster_living_metadata::player_pose(living),
            ),
            Value::Entity(body) => {
                let EntityPayload::Living(living) = &body.payload else {
                    return Err(ApplyFailure::InvalidAction);
                };
                super::cluster_owned_mob_metadata::dimensions(living, body.kind)
                    .ok_or(ApplyFailure::InvalidAction)?
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let kind = match &value {
            Value::Entity(body) => pumpkin_data::entity::EntityType::from_raw(body.kind),
            _ => None,
        };
        Ok(EnvironmentalRegistryFacts {
            half_width: (f64::from(size.width) * 0.5).to_bits(),
            height: f64::from(size.height).to_bits(),
            eye_height: f64::from(size.eye_height).to_bits(),
            shape_properties: super::cluster_living_environment::shape_properties(&value)?,
            armor: u16::from(A::ARMOR.id),
            toughness: u16::from(A::ARMOR_TOUGHNESS.id),
            water_breathing: u16::from(pumpkin_data::effect::StatusEffect::WATER_BREATHING.id),
            fire_immune: fire_immune(&value),
            can_breathe_water: matches!(&value, Value::Entity(body) if super::cluster_living_environment::can_breathe_water(body.kind)),
            freeze_immune: kind.is_some_and(|kind| {
                kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_FREEZE_IMMUNE_ENTITY_TYPES)
            }),
        })
    }

    fn previous_environment_position(
        &self,
        source: &LivingObservation,
        tick: TickStamp,
    ) -> Result<[u64; 3], ApplyFailure> {
        original_value(self, source, tick)?;
        let identity = source.identity(tick)?;
        self.original_motion(&identity.identity, tick)?
            .previous_position
            .ok_or(ApplyFailure::MissingStakeholder(
                identity.target().stakeholder(),
            ))
    }
}
