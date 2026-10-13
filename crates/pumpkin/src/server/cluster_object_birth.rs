use pumpkin_cluster::{
    actor::ActorId,
    canonical_action::{ActionId, ActionRef, ApplyFailure, SpatialEpoch},
    entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial},
    protocol::{ChunkKey, GlobalEntityId},
    realm_completion::PeerGeneration,
    time::TickStamp,
};

pub(super) struct ObjectBirthInput {
    pub entity: GlobalEntityId,
    pub action: ActionId,
    pub owner: PeerGeneration,
    pub tick: TickStamp,
    pub uuid: uuid::Uuid,
    pub kind: pumpkin_data::entity::EntityType,
    pub spatial: EntitySpatial,
    pub state: pumpkin_cluster::object_state::ObjectState,
}

pub(super) fn prepare(
    input: ObjectBirthInput,
    chunk: ChunkKey,
) -> Result<EntityBody, ApplyFailure> {
    if input.action.actor != ActorId::Entity(input.entity)
        || input.action.incarnation != u64::from(input.entity.generation)
        || input.entity.origin != input.owner.server
        || input.owner.generation == 0
        || input.uuid.is_nil()
        || !input.state.valid()
        || !super::cluster_object_metadata::kind_matches(&input.state, input.kind.id)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let reference = ActionRef {
        id: input.action,
        tick: input.tick,
    };
    let body = EntityBody {
        mount: None,
        id: input.entity,
        birth: EntityBirth {
            action: reference,
            source: input.owner,
            uuid: input.uuid,
        },
        owner: input.owner,
        kind: input.kind.id,
        spatial: input.spatial,
        spatial_epoch: SpatialEpoch::Action(reference),
        payload: EntityPayload::Object(input.state),
        relation_versions: Default::default(),
        passengers: Default::default(),
        tags: Default::default(),
        tags_revision: 1,
    };
    if !super::cluster_world_canonical::valid_entity_body(&body, chunk, input.entity) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(body)
}

pub(super) fn prepare_stand(
    entity: GlobalEntityId,
    action: ActionId,
    owner: PeerGeneration,
    tick: TickStamp,
    uuid: uuid::Uuid,
    spatial: EntitySpatial,
    stand: pumpkin_cluster::armor_stand::ArmorStandState,
    chunk: ChunkKey,
) -> Result<EntityBody, ApplyFailure> {
    if action.actor != ActorId::Entity(entity)
        || action.incarnation != u64::from(entity.generation)
        || entity.origin != owner.server
        || owner.generation == 0
        || uuid.is_nil()
        || !stand.valid()
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let reference = ActionRef { id: action, tick };
    let kind = pumpkin_data::entity::EntityType::ARMOR_STAND;
    let mut living =
        super::cluster_owned_mob_birth::inherited_living(ActorId::Entity(entity), &kind)?;
    living.stand = Some(stand);
    living.combat.velocity_micro = spatial.velocity_micro;
    let body = EntityBody {
        mount: None,
        id: entity,
        birth: EntityBirth {
            action: reference,
            source: owner,
            uuid,
        },
        owner,
        kind: kind.id,
        spatial,
        spatial_epoch: SpatialEpoch::Action(reference),
        payload: EntityPayload::Living(living),
        relation_versions: Default::default(),
        passengers: Default::default(),
        tags: Default::default(),
        tags_revision: 1,
    };
    if !super::cluster_world_canonical::valid_entity_body(&body, chunk, entity) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(body)
}

pub(super) fn prepare_reserved(
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    reference: ActionRef,
    owner: PeerGeneration,
    kind: pumpkin_data::entity::EntityType,
    spatial: EntitySpatial,
    state: pumpkin_cluster::object_state::ObjectState,
    chunk: ChunkKey,
) -> Result<EntityBody, ApplyFailure> {
    if identity.source.generation == 0
        || identity.id.origin != identity.source.server
        || identity.id.generation != identity.source.generation
        || identity.uuid.is_nil()
        || owner.generation == 0
        || !state.valid()
        || !super::cluster_object_metadata::kind_matches(&state, kind.id)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let body = EntityBody {
        mount: None,
        id: identity.id,
        birth: EntityBirth {
            action: reference,
            source: identity.source,
            uuid: identity.uuid,
        },
        owner,
        kind: kind.id,
        spatial,
        spatial_epoch: SpatialEpoch::Action(reference),
        payload: EntityPayload::Object(state),
        tags: Default::default(),
        tags_revision: 1,
        relation_versions: Default::default(),
        passengers: Default::default(),
    };
    if !super::cluster_world_canonical::valid_entity_body(&body, chunk, identity.id) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(body)
}

pub(super) fn prepare_reserved_stand(
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    reference: ActionRef,
    owner: PeerGeneration,
    spatial: EntitySpatial,
    stand: pumpkin_cluster::armor_stand::ArmorStandState,
    chunk: ChunkKey,
) -> Result<EntityBody, ApplyFailure> {
    if identity.source.generation == 0
        || identity.id.origin != identity.source.server
        || identity.id.generation != identity.source.generation
        || identity.uuid.is_nil()
        || owner.generation == 0
        || !stand.valid()
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let kind = pumpkin_data::entity::EntityType::ARMOR_STAND;
    let mut living =
        super::cluster_owned_mob_birth::inherited_living(ActorId::Entity(identity.id), &kind)?;
    living.stand = Some(stand);
    living.combat.velocity_micro = spatial.velocity_micro;
    let body = EntityBody {
        mount: None,
        id: identity.id,
        birth: EntityBirth {
            action: reference,
            source: identity.source,
            uuid: identity.uuid,
        },
        owner,
        kind: kind.id,
        spatial,
        spatial_epoch: SpatialEpoch::Action(reference),
        payload: EntityPayload::Living(living),
        tags: Default::default(),
        tags_revision: 1,
        relation_versions: Default::default(),
        passengers: Default::default(),
    };
    if !super::cluster_world_canonical::valid_entity_body(&body, chunk, identity.id) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(body)
}

pub(super) fn prepare_cloud(
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    reference: ActionRef,
    owner: PeerGeneration,
    mut spatial: EntitySpatial,
    potion: pumpkin_cluster::inventory::InventoryCell,
    shooter: pumpkin_cluster::item_contact::ContactIdentity,
) -> Result<EntityBody, ApplyFailure> {
    spatial.velocity_micro = [0; 3];
    spatial.on_ground = false;
    let at = spatial.position.map(f64::from_bits);
    let chunk = ChunkKey::new(
        spatial.world,
        (at[0].floor() as i32).div_euclid(16),
        (at[2].floor() as i32).div_euclid(16),
    );
    let state = pumpkin_cluster::object_state::ObjectState {
        simulation_tick: Some(reference.tick),
        property_versions: Default::default(),
        kind: pumpkin_cluster::object_state::ObjectKind::AreaEffectCloud(
            pumpkin_cluster::object_state::AreaEffectCloudState {
                potion,
                shooter,
                initial_radius: 3.0_f32.to_bits(),
                radius_per_tick: (-3.0_f32 / 600.0).to_bits(),
                radius_on_use: (-0.5_f32).to_bits(),
                duration: 600,
                wait: 10,
                reapply: 20,
                continuation: pumpkin_cluster::object_state::CloudContinuation {
                    radius: 3.0_f32.to_bits(),
                    age: 0,
                    cooldowns: Default::default(),
                },
            },
        ),
    };
    prepare_reserved(
        identity,
        reference,
        owner,
        pumpkin_data::entity::EntityType::AREA_EFFECT_CLOUD,
        spatial,
        state,
        chunk,
    )
}

pub(super) fn prepare_primed_tnt(
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    reference: ActionRef,
    owner: PeerGeneration,
    spatial: EntitySpatial,
    fuse: u16,
    power: f32,
    block_state: u16,
    igniter: Option<pumpkin_cluster::item_contact::ContactIdentity>,
) -> Result<EntityBody, ApplyFailure> {
    if pumpkin_data::Block::from_state_id(
        pumpkin_data::BlockStateId::new(block_state).ok_or(ApplyFailure::InvalidAction)?,
    )
    .id != pumpkin_data::Block::TNT.id
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let at = spatial.position.map(f64::from_bits);
    if at
        .iter()
        .any(|coordinate| !coordinate.is_finite() || coordinate.abs() > 30_000_000.0)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let chunk = ChunkKey::new(
        spatial.world,
        (at[0].floor() as i32).div_euclid(16),
        (at[2].floor() as i32).div_euclid(16),
    );
    prepare_reserved(
        identity,
        reference,
        owner,
        pumpkin_data::entity::EntityType::TNT,
        spatial,
        pumpkin_cluster::object_state::ObjectState {
            simulation_tick: Some(reference.tick),
            property_versions: Default::default(),
            kind: pumpkin_cluster::object_state::ObjectKind::PrimedTnt(
                pumpkin_cluster::object_state::TntState {
                    fuse,
                    power: power.to_bits(),
                    block_state,
                    igniter,
                },
            ),
        },
        chunk,
    )
}
