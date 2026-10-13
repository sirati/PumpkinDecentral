use std::collections::BTreeMap;

use pumpkin_cluster::{
    area_cloud::{AreaCloudIntent, CloudPotionOutcome, property},
    canonical_action::{ActionId, ActionState, ApplyFailure, Cell, Target, Value},
    entity_body::EntityPayload,
    item_contact::{ContactIdentity, ContactObservation},
    living_property::{LivingProperty as P, LivingPropertyValue as V},
    object_state::{ObjectKind, ObjectState},
    time::TickStamp,
};

pub(super) fn potion<S: ActionState>(
    context: &S,
    intent: &AreaCloudIntent,
    victim: &ContactObservation,
    id: ActionId,
    tick: TickStamp,
    values: &BTreeMap<Target, Cell>,
) -> Result<CloudPotionOutcome, ApplyFailure> {
    let mut attributes = pumpkin_data::attributes::Attributes::ALL
        .iter()
        .map(|attribute| u16::from(attribute.id))
        .collect::<Vec<_>>();
    attributes.sort_unstable();
    if intent.status_attributes != attributes
        || intent.max_health_attribute
            != u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let Some(Cell {
        value: Value::Entity(body),
        ..
    }) = values.get(&intent.cloud.target())
    else {
        return Err(ApplyFailure::Type(intent.cloud.target()));
    };
    let EntityPayload::Object(ObjectState {
        kind: ObjectKind::AreaEffectCloud(cloud),
        ..
    }) = &body.payload
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Some(Cell {
        value: Value::LivingProperty(V::Unsigned(health)),
        ..
    }) = values.get(&property(victim, P::Health))
    else {
        return Err(ApplyFailure::Type(property(victim, P::Health)));
    };
    if *health == 0
        || matches!(
            values.get(&property(victim, P::GameMode)),
            Some(Cell {
                value: Value::LivingProperty(V::Unsigned(3)),
                ..
            })
        )
    {
        return Ok(CloudPotionOutcome {
            affected: false,
            effects: Vec::new(),
        });
    }
    if matches!(values.get(&victim.target()), Some(Cell { value: Value::Entity(body), .. })
        if matches!(&body.payload, EntityPayload::Living(living) if living.stand.is_some()))
    {
        return Ok(CloudPotionOutcome {
            affected: false,
            effects: Vec::new(),
        });
    }
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(&cloud.potion)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    let effects = super::cluster_projectile_potion::potion_effects(&stack)?;
    if effects.is_empty() {
        return Ok(CloudPotionOutcome {
            affected: false,
            effects: Vec::new(),
        });
    }
    let shooter = match cloud.shooter {
        ContactIdentity::Player(lease) => pumpkin_cluster::actor::ActorId::Player(lease.gid),
        ContactIdentity::Entity { id, .. } => pumpkin_cluster::actor::ActorId::Entity(id),
    };
    let effects = super::cluster_projectile_potion::apply(
        context,
        id,
        tick,
        shooter,
        victim.target(),
        &effects,
        0.5,
        0.5,
        20,
        intent.max_health_attribute,
        &intent.status_attributes,
        values,
    )?;
    Ok(CloudPotionOutcome {
        affected: true,
        effects,
    })
}
