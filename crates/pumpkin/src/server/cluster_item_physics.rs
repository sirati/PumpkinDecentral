use pumpkin_cluster::entity_body::{EntityBody, EntityPayload, EntitySpatial};
use pumpkin_cluster::identity::PlayerSeq;
use pumpkin_cluster::item_spatial::ItemSpatialSample;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

pub(crate) struct IncomingItemSpatial {
    pub source: pumpkin_cluster::identity::ServerId,
    pub transport_generation: u64,
    pub sample: ItemSpatialSample,
}

pub(crate) fn route_datagram(
    server: &super::Server,
    source: pumpkin_cluster::identity::ServerId,
    transport_generation: u64,
    bytes: Vec<u8>,
) -> Result<(), pumpkin_cluster::item_spatial_wire::DecodeFailure> {
    use pumpkin_cluster::item_spatial_wire::DecodeFailure;
    if bytes.len() > super::cluster_datagram::UDP_PATH_BYTES {
        return Err(DecodeFailure::Length);
    }
    let batch = pumpkin_cluster::item_spatial_wire::Batch::decode(bytes.into())?;
    let Some(route) = server.cluster_item_spatial_route.load_full() else {
        return Ok(());
    };
    for sample in batch.samples() {
        if sample.issuer.server == source {
            let _ = route.try_send(IncomingItemSpatial {
                source,
                transport_generation,
                sample,
            });
        } else {
            return Err(DecodeFailure::Provenance);
        }
    }
    Ok(())
}

pub(crate) fn forward_samples(
    server: &super::Server,
    peer: pumpkin_cluster::identity::ServerId,
    limit: usize,
    samples: &[ItemSpatialSample],
) {
    let Some(outlet) = server.cluster_datagram_outlet.load_full() else {
        return;
    };
    let mut first = 0;
    while first < samples.len() {
        let mut end = first + 1;
        while end < samples.len()
            && pumpkin_cluster::item_spatial_wire::batch_length(end + 1 - first)
                .is_some_and(|length| length <= limit)
        {
            end += 1;
        }
        if !pumpkin_cluster::item_spatial_wire::batch_length(end - first)
            .is_some_and(|length| length <= limit)
        {
            return;
        }
        if let Ok(bytes) = pumpkin_cluster::item_spatial_wire::encode(&samples[first..end]) {
            let _ = outlet.try_send((peer, bytes.to_vec()));
        }
        first = end;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ItemPhysicsFailure {
    InvalidItem,
    MissingWorld,
    Read,
    MissingChunk(ChunkKey),
    StaleTick,
}

pub(crate) struct ItemPhysicsStep {
    pub sample: ItemSpatialSample,
    pub source_chunk: ChunkKey,
    pub destination_chunk: ChunkKey,
    pub lava_damage: bool,
    pub fire_damage: bool,
    pub cactus_damage: bool,
    pub fluids: Vec<u16>,
}

pub(crate) fn simulate(
    body: &EntityBody,
    prior: Option<&ItemSpatialSample>,
    source_chunk: ChunkKey,
    primary: PeerGeneration,
    tick: TickStamp,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    ultra_warm: bool,
) -> Result<ItemPhysicsStep, ItemPhysicsFailure> {
    simulate_with_attraction(
        body,
        prior,
        source_chunk,
        primary,
        tick,
        blocks,
        ultra_warm,
        None,
    )
}

pub(super) fn simulate_with_attraction(
    body: &EntityBody,
    prior: Option<&ItemSpatialSample>,
    source_chunk: ChunkKey,
    primary: PeerGeneration,
    tick: TickStamp,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    ultra_warm: bool,
    attraction: Option<[f64; 3]>,
) -> Result<ItemPhysicsStep, ItemPhysicsFailure> {
    use pumpkin_util::math::item_motion::{
        ItemMedium, apply_fluid_push, apply_item_drag, apply_item_friction, item_movement_due,
    };
    let (age, fire_immune, half_width, height) = match &body.payload {
        EntityPayload::Item(item) => (item.age, item.fire_immune, 0.125, 0.25),
        EntityPayload::Experience(orb) if body.owner == primary => (orb.age, false, 0.25, 0.5),
        _ => return Err(ItemPhysicsFailure::InvalidItem),
    };
    if !body.belongs_to(body.id, source_chunk) || primary.generation == 0 {
        return Err(ItemPhysicsFailure::InvalidItem);
    }
    let prior = prior.filter(|sample| {
        sample.birth == body.birth && sample.epoch == body.spatial_epoch && sample.issuer == primary
    });
    if prior.is_some_and(|sample| !tick.is_after(sample.tick)) {
        return Err(ItemPhysicsFailure::StaleTick);
    }
    let spatial = prior.map_or(&body.spatial, |sample| &sample.spatial);
    let position = spatial.position.map(f64::from_bits);
    let position = Vector3::new(position[0], position[1], position[2]);
    let bounds = BoundingBox::new(
        position.add_raw(-half_width, 0.0, -half_width),
        position.add_raw(half_width, height, half_width),
    );
    let read = |at: pumpkin_util::math::position::BlockPos| {
        let chunk = ChunkKey::new(spatial.world, at.0.x.div_euclid(16), at.0.z.div_euclid(16));
        match blocks.block(
            chunk,
            pumpkin_cluster::protocol::BlockPos {
                x: at.0.x,
                y: at.0.y,
                z: at.0.z,
            },
        ) {
            Err(super::cluster_chunk_state::BlockTransactionError::OutsideHeight) => {
                Ok(pumpkin_data::Block::AIR.default_state.id)
            }
            result => result.map_err(|_| ItemPhysicsFailure::MissingChunk(chunk)),
        }
    };
    let contacts =
        pumpkin_world::fluid_snapshot::sample_entity_fluid_contacts(bounds, 256, true, read)
            .map_err(|failure| match failure {
                pumpkin_world::fluid_snapshot::FluidReadFailure::Read { reason, .. } => reason,
                _ => ItemPhysicsFailure::Read,
            })?;
    let mut fire_damage = false;
    let mut cactus_damage = false;
    for x in bounds.min.x.floor() as i32..=bounds.max.x.floor() as i32 {
        for y in bounds.min.y.floor() as i32..=bounds.max.y.floor() as i32 {
            for z in bounds.min.z.floor() as i32..=bounds.max.z.floor() as i32 {
                let block = read(pumpkin_util::math::position::BlockPos::new(x, y, z))?
                    .to_block();
                fire_damage |= block.id == pumpkin_data::Block::FIRE.id
                    || block.id == pumpkin_data::Block::SOUL_FIRE.id;
                cactus_damage |= block.id == pumpkin_data::Block::CACTUS.id;
            }
        }
    }
    let medium = if contacts.contacts[0].height > 0.1 {
        ItemMedium::Water
    } else if contacts.contacts[1].height > 0.1 {
        ItemMedium::Lava
    } else {
        ItemMedium::Air
    };
    let mut velocity = Vector3::new(
        spatial.velocity_micro[0] as f64 / 1_000_000.0,
        spatial.velocity_micro[1] as f64 / 1_000_000.0,
        spatial.velocity_micro[2] as f64 / 1_000_000.0,
    );
    velocity = apply_item_drag(velocity, medium, 0.04);
    if let Some(target) = attraction {
        let delta = Vector3::new(
            target[0] - position.x,
            target[1] - position.y,
            target[2] - position.z,
        );
        let distance = (delta.x * delta.x + delta.y * delta.y + delta.z * delta.z).sqrt();
        if distance > 0.0 && distance < 8.0 {
            let acceleration = (1.0 - distance / 8.0).powi(2) * 0.1 / distance;
            velocity = velocity.add_raw(
                delta.x * acceleration,
                delta.y * acceleration,
                delta.z * acceleration,
            );
        }
    }
    for (index, contact) in contacts.contacts.iter().enumerate() {
        if contact.touching {
            let speed = if index == 0 {
                0.014
            } else if ultra_warm {
                0.007
            } else {
                0.002333333
            };
            velocity = apply_fluid_push(velocity, contact.push, contact.samples, speed, true);
        }
    }
    let mut moved_position = position;
    let mut on_ground = spatial.on_ground;
    if matches!(body.payload, EntityPayload::Experience(_))
        || item_movement_due(age, body.id.local_id, on_ground, velocity)
    {
        let collisions = pumpkin_world::collision::collect_block_collisions(
            bounds.stretch(velocity),
            4096,
            |at| read(at).map(|id| id.to_state()),
            |_, _| None,
        )
        .map_err(|failure| match failure {
            pumpkin_world::collision::CollisionReadFailure::Read { reason, .. } => reason,
            _ => ItemPhysicsFailure::Read,
        })?;
        let collision = pumpkin_util::math::collision::resolve_block_movement(
            bounds,
            velocity,
            &collisions.shapes,
            &collisions.positions,
        );
        moved_position += collision.movement;
        on_ground = collision.supporting_block.is_some();
        if collision.movement.x != velocity.x {
            velocity.x = 0.0;
        }
        if collision.movement.y != velocity.y {
            velocity.y = 0.0;
        }
        if collision.movement.z != velocity.z {
            velocity.z = 0.0;
        }
        let slipperiness = match collision.supporting_block {
            Some(at) => Some(f64::from(
                read(at)?
                    .to_block()
                    .slipperiness,
            )),
            None => None,
        };
        velocity = apply_item_friction(velocity, slipperiness);
    }
    let destination_chunk = ChunkKey::new(
        spatial.world,
        (moved_position.x.floor() as i32).div_euclid(16),
        (moved_position.z.floor() as i32).div_euclid(16),
    );
    Ok(ItemPhysicsStep {
        sample: ItemSpatialSample {
            head_yaw: spatial.yaw,
            id: body.id,
            birth: body.birth.clone(),
            epoch: body.spatial_epoch,
            issuer: primary,
            tick,
            sequence: prior.map_or(PlayerSeq(0), |sample| {
                PlayerSeq(sample.sequence.0.wrapping_add(1))
            }),
            spatial: EntitySpatial {
                world: spatial.world,
                position: [
                    moved_position.x.to_bits(),
                    moved_position.y.to_bits(),
                    moved_position.z.to_bits(),
                ],
                velocity_micro: [velocity.x, velocity.y, velocity.z]
                    .map(|value| (value * 1_000_000.0).round() as i64),
                yaw: spatial.yaw,
                pitch: spatial.pitch,
                on_ground,
            },
        },
        source_chunk,
        destination_chunk,
        lava_damage: contacts.contacts[1].touching && !fire_immune,
        fire_damage,
        cactus_damage,
        fluids: contacts.fluids.iter().map(|fluid| fluid.id).collect(),
    })
}
