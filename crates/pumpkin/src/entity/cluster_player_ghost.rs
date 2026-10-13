use pumpkin_data::damage::DamageType;
use pumpkin_data::entity::EntityType;
use pumpkin_util::math::vector3::Vector3;
use std::sync::Arc;

use crate::entity::{Entity, EntityBase, living::LivingEntity};
use crate::server::Server;
use crate::world::World;

pub(crate) struct RemotePlayerReplica {
    entity: Arc<Entity>,
    pub frame: crate::server::cluster_remote_player_presentation::RemotePlayerFrame,
}

impl RemotePlayerReplica {
    pub(crate) fn new(
        world: Arc<World>,
        frame: crate::server::cluster_remote_player_presentation::RemotePlayerFrame,
        previous: Option<&Self>,
    ) -> Arc<Self> {
        if let Some(previous) = previous.filter(|previous| {
            previous.frame.same_session(&frame)
                && previous.frame.spatial.world == frame.spatial.world
                && previous.frame.spatial.frame.effective.position
                    == frame.spatial.frame.effective.position
                && previous.frame.spatial.frame.effective.velocity
                    == frame.spatial.frame.effective.velocity
                && previous.frame.spatial.frame.effective.yaw == frame.spatial.frame.effective.yaw
                && previous.frame.spatial.frame.effective.pitch
                    == frame.spatial.frame.effective.pitch
                && previous.frame.spatial.head_yaw == frame.spatial.head_yaw
                && previous.frame.spatial.sneaking == frame.spatial.sneaking
                && previous.frame.spatial.sprinting == frame.spatial.sprinting
                && previous.frame.spatial.swimming == frame.spatial.swimming
                && previous.frame.spatial.gliding == frame.spatial.gliding
                && previous.frame.pose() == frame.pose()
                && crate::server::cluster_living_metadata::player_scale(&previous.frame.living)
                    == crate::server::cluster_living_metadata::player_scale(&frame.living)
                && previous.frame.living.combat.timing.fire_ticks
                    == frame.living.combat.timing.fire_ticks
                && previous.frame.living.environment.frozen_ticks
                    == frame.living.environment.frozen_ticks
                && previous.frame.living.combat.status.invulnerable
                    == frame.living.combat.status.invulnerable
                && crate::server::cluster_living_metadata::java_flags(&previous.frame.living)
                    == crate::server::cluster_living_metadata::java_flags(&frame.living)
                && previous.frame.spatial.frame.effective.on_ground
                    == frame.spatial.frame.effective.on_ground
        }) {
            return Arc::new(Self {
                entity: previous.entity.clone(),
                frame,
            });
        }
        let effective = frame.spatial.frame.effective;
        let position = Vector3::new(
            effective.position[0],
            effective.position[1],
            effective.position[2],
        );
        let biome = frame
            .spatial
            .biome_id
            .and_then(pumpkin_data::biome::Biome::from_id)
            .unwrap_or(&pumpkin_data::biome::Biome::THE_VOID);
        let mut entity = Entity::from_uuid_with_id_in_biome(
            frame.client_entity_id,
            frame.lease.key.0,
            world,
            position,
            &EntityType::PLAYER,
            biome,
        );
        entity.yaw = crossbeam::atomic::AtomicCell::new(effective.yaw);
        entity.body_yaw = crossbeam::atomic::AtomicCell::new(effective.yaw);
        entity.head_yaw = crossbeam::atomic::AtomicCell::new(frame.spatial.head_yaw);
        entity.pitch = crossbeam::atomic::AtomicCell::new(effective.pitch);
        entity.velocity = crossbeam::atomic::AtomicCell::new(Vector3::new(
            effective.velocity[0],
            effective.velocity[1],
            effective.velocity[2],
        ));
        entity.on_ground = std::sync::atomic::AtomicBool::new(effective.on_ground);
        entity.sneaking = std::sync::atomic::AtomicBool::new(frame.spatial.sneaking);
        entity.sprinting = std::sync::atomic::AtomicBool::new(frame.spatial.sprinting);
        entity.swimming = std::sync::atomic::AtomicBool::new(frame.spatial.swimming);
        entity.fall_flying = std::sync::atomic::AtomicBool::new(frame.spatial.gliding);
        entity.no_physics = std::sync::atomic::AtomicBool::new(true);
        entity.fire_ticks = std::sync::atomic::AtomicI32::new(
            frame.living.combat.timing.fire_ticks.min(i32::MAX as u32) as i32,
        );
        entity.has_visual_fire =
            std::sync::atomic::AtomicBool::new(frame.living.combat.timing.fire_ticks != 0);
        entity.frozen_ticks =
            std::sync::atomic::AtomicI32::new(i32::from(frame.living.environment.frozen_ticks));
        entity.invulnerable =
            std::sync::atomic::AtomicBool::new(frame.living.combat.status.invulnerable);
        let flags = crate::server::cluster_living_metadata::java_flags(&frame.living);
        entity.invisible = std::sync::atomic::AtomicBool::new(flags & 0x20 != 0);
        entity.glowing = std::sync::atomic::AtomicBool::new(flags & 0x40 != 0);
        let pose = frame.pose();
        let dimensions =
            crate::server::cluster_living_metadata::player_dimensions(&frame.living, pose);
        entity.pose = crossbeam::atomic::AtomicCell::new(pose);
        entity.entity_dimension = crossbeam::atomic::AtomicCell::new(dimensions);
        entity.bounding_box = crossbeam::atomic::AtomicCell::new(
            pumpkin_util::math::boundingbox::BoundingBox::new_from_pos(
                position.x,
                position.y,
                position.z,
                &dimensions,
            ),
        );
        Arc::new(Self {
            entity: Arc::new(entity),
            frame,
        })
    }
}

impl EntityBase for RemotePlayerReplica {
    fn write_nbt(&self, nbt: &mut pumpkin_nbt::compound::NbtCompound) {
        nbt.child_tags.remove("EnderItems");
        nbt.put_string("id", "minecraft:player".to_owned());
        nbt.put_uuid("UUID", self.frame.lease.key.0);
        let world = self.entity.world.load();
        let Some(server) = world.server.upgrade() else {
            return;
        };
        if crate::server::cluster_player_projection::write_owned_player_nbt(
            &server,
            &self.frame.living,
            self.frame.spatial,
            nbt,
        )
        .is_err()
        {
            return;
        }
        if let Some(cells) = self.frame.inventory_cells() {
            let _ = crate::server::cluster_invsee::write_player_inventory_cells_nbt(nbt, cells);
        }
        if let Some(cells) = self.frame.canonical_ender_cells(&server) {
            let _ = crate::server::cluster_player_ender::write_cells_nbt(nbt, cells);
        }
    }

    fn tick(&self, _caller: &dyn EntityBase, _server: &Server) {}

    fn init_data_tracker(&self) {}

    fn get_entity(&self) -> &Entity {
        &self.entity
    }

    fn get_living_entity(&self) -> Option<&LivingEntity> {
        None
    }

    fn get_name(&self) -> pumpkin_util::text::TextComponent {
        pumpkin_util::text::TextComponent::text(self.frame.profile.name.clone())
    }

    fn get_display_name(&self) -> pumpkin_util::text::TextComponent {
        self.get_name()
    }

    fn is_passenger(&self) -> bool {
        self.frame
            .living
            .avatar
            .as_ref()
            .is_some_and(|avatar| avatar.mount.is_some())
    }

    fn is_vehicle(&self) -> bool {
        false
    }

    fn has_passenger(&self, _other: &dyn EntityBase) -> bool {
        false
    }

    fn cast_any(&self) -> &dyn std::any::Any {
        self
    }

    fn can_hit(&self) -> bool {
        self.frame.living.combat.health_milli != 0 && self.frame.spatial.game_mode != 3
    }

    fn is_pushable(&self) -> bool {
        false
    }

    fn is_pushed_by_fluids(&self) -> bool {
        false
    }

    fn damage_with_context(
        &self,
        _caller: &dyn EntityBase,
        _amount: f32,
        _damage_type: DamageType,
        _position: Option<Vector3<f64>>,
        _source: Option<&dyn EntityBase>,
        _cause: Option<&dyn EntityBase>,
    ) -> bool {
        false
    }
}
