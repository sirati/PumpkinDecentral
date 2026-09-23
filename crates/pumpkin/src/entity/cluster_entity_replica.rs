use std::sync::Arc;
use std::sync::atomic::Ordering;

use pumpkin_cluster::protocol::EntityRef;
use pumpkin_data::entity::EntityType;
use pumpkin_util::math::vector3::Vector3;
use sha2::{Digest, Sha256};

use crate::entity::living::LivingEntity;
use crate::entity::{Entity, EntityBase};
use crate::server::Server;
use crate::world::World;

pub struct RemoteEntityReplica {
    inner: RemoteEntityReplicaInner,
    pub cluster_ref: EntityRef,
}

enum RemoteEntityReplicaInner {
    Entity(Entity),
    Living(LivingEntity),
}

impl RemoteEntityReplica {
    pub fn new(
        world: Arc<World>,
        cluster_ref: EntityRef,
        entity_type: &'static EntityType,
        position: Vector3<f64>,
    ) -> Arc<Self> {
        let entity = Entity::from_uuid(replica_uuid(cluster_ref), world, position, entity_type);
        entity.no_physics.store(true, Ordering::Relaxed);
        let inner = if entity_type.mob {
            RemoteEntityReplicaInner::Living(LivingEntity::new(entity))
        } else {
            RemoteEntityReplicaInner::Entity(entity)
        };
        Arc::new(Self { inner, cluster_ref })
    }
}

#[must_use]
pub fn replica_uuid(cluster_ref: EntityRef) -> uuid::Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"pumpkin-cluster-entity");
    hasher.update(cluster_ref.origin.0.to_be_bytes());
    hasher.update(cluster_ref.local_id.to_be_bytes());
    hasher.update(cluster_ref.generation.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

impl EntityBase for RemoteEntityReplica {
    fn tick(&self, _caller: &dyn EntityBase, _server: &Server) {}

    fn get_entity(&self) -> &Entity {
        match &self.inner {
            RemoteEntityReplicaInner::Entity(entity) => entity,
            RemoteEntityReplicaInner::Living(living) => &living.entity,
        }
    }

    fn get_living_entity(&self) -> Option<&crate::entity::living::LivingEntity> {
        match &self.inner {
            RemoteEntityReplicaInner::Entity(_) => None,
            RemoteEntityReplicaInner::Living(living) => Some(living),
        }
    }

    fn cast_any(&self) -> &dyn std::any::Any {
        self
    }

    fn can_hit(&self) -> bool {
        true
    }

    fn is_pushable(&self) -> bool {
        false
    }

    fn is_pushed_by_fluids(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::{ChunkKey, WorldId};

    fn entity(owner: u16, local_id: i32, generation: u32) -> EntityRef {
        EntityRef {
            origin: ServerId(owner),
            local_id,
            generation,
            chunk: ChunkKey::new(WorldId::new(0, 7), 0, 0),
        }
    }

    #[test]
    fn replica_uuid_prevents_origin_and_generation_reuse_collisions() {
        let original = replica_uuid(entity(2, 41, 7));
        let reused = replica_uuid(entity(2, 41, 8));
        let other_owner = replica_uuid(entity(3, 41, 7));

        assert_ne!(original, reused);
        assert_ne!(original, other_owner);
        assert_eq!(original.get_version_num(), 4);
        assert_eq!(original.get_variant(), uuid::Variant::RFC4122);
    }
}
