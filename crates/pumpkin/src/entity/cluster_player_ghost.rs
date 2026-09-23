use std::sync::Arc;
use std::sync::atomic::Ordering;

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_data::damage::DamageType;
use pumpkin_data::entity::EntityType;
use pumpkin_protocol::bedrock::client::add_player::CAddPlayer;
use pumpkin_protocol::bedrock::client::common::{
    BuildPlatform, SerializedAbilitiesData, SerializedAbilitiesDataSerializedLayer,
};
use pumpkin_protocol::bedrock::client::set_actor_data::PropertySyncData;
use pumpkin_protocol::bedrock::network_item::NetworkItemStackDescriptor;
use pumpkin_protocol::codec::var_ulong::VarULong;
use pumpkin_protocol::java::client::play::CSpawnEntity;
use pumpkin_util::GameMode;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;

use crate::entity::{Entity, EntityBase, living::LivingEntity};
use crate::net::{bedrock::BedrockClient, java::JavaClient};
use crate::server::Server;
use crate::world::World;

pub struct RemotePlayerReplica {
    pub living_entity: LivingEntity,
    pub gid: GlobalPlayerId,
    pub name: String,
    pub game_mode: GameMode,
}

impl RemotePlayerReplica {
    pub fn new(
        world: Arc<World>,
        gid: GlobalPlayerId,
        uuid: uuid::Uuid,
        name: String,
        position: Vector3<f64>,
        game_mode: GameMode,
    ) -> Arc<Self> {
        let entity = Entity::from_uuid(uuid, world, position, &EntityType::PLAYER);
        entity.no_physics.store(true, Ordering::Relaxed);
        Arc::new(Self {
            living_entity: LivingEntity::new(entity),
            gid,
            name,
            game_mode,
        })
    }
}

impl EntityBase for RemotePlayerReplica {
    fn tick(&self, _caller: &dyn EntityBase, _server: &Server) {}

    fn init_data_tracker(&self) {}

    fn get_entity(&self) -> &Entity {
        &self.living_entity.entity
    }

    fn get_living_entity(&self) -> Option<&LivingEntity> {
        Some(&self.living_entity)
    }

    fn cast_any(&self) -> &dyn std::any::Any {
        self
    }

    fn is_pushable(&self) -> bool {
        false
    }

    fn is_pushed_by_fluids(&self) -> bool {
        false
    }

    fn can_hit(&self) -> bool {
        true
    }

    fn is_immune_to_explosion(&self) -> bool {
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
        self.living_entity.damage_with_context(
            self,
            _amount,
            _damage_type,
            _position,
            _source,
            _cause,
        )
    }

    fn send_java_spawn_packet(&self, client: &JavaClient) {
        let entity = &self.living_entity.entity;
        let packet = CSpawnEntity::new(
            entity.entity_id.into(),
            entity.entity_uuid,
            i32::from(EntityType::PLAYER.id).into(),
            entity.pos.load(),
            entity.pitch.load(),
            entity.yaw.load(),
            entity.head_yaw.load(),
            0.into(),
            entity.velocity.load(),
        );
        if let Ok(data) = client.serialize_packet(&packet) {
            client.try_enqueue_packet(data);
        }
    }

    fn send_bedrock_spawn_packet(&self, client: &BedrockClient) {
        let entity = &self.living_entity.entity;
        let entity_id = entity.entity_id;
        let packet = CAddPlayer {
            uuid: entity.entity_uuid,
            player_name: self.name.clone(),
            target_runtime_id: VarULong(entity_id as u64),
            platform_chat_id: String::new(),
            position: entity.pos.load().to_f32_lossy(),
            velocity: entity.velocity.load().to_f32_lossy(),
            rotation: Vector2::new(entity.pitch.load(), entity.yaw.load()),
            y_head_rotation: entity.head_yaw.load(),
            carried_item: NetworkItemStackDescriptor::default(),
            player_game_type: self.game_mode.into(),
            entity_data: entity.bedrock_metadata(),
            synced_properties: PropertySyncData::default(),
            abilities_data: SerializedAbilitiesData {
                target_player_raw_id: i64::from(entity_id),
                player_permissions:
                    pumpkin_protocol::bedrock::client::PlayerPermissionLevel::Visitor,
                command_permissions: pumpkin_protocol::bedrock::client::CommandPermissionLevel::Any,
                layers: vec![SerializedAbilitiesDataSerializedLayer {
                    serialized_layer: 0,
                    abilities_set: 0,
                    ability_value: 0,
                    fly_speed: 0.05,
                    vertical_fly_speed: 0.05,
                    walk_speed: 0.1,
                }],
            },
            actor_links: Vec::new(),
            device_id: String::new(),
            build_platform: BuildPlatform::Unknown,
        };
        if let Ok(data) = client.serialize_packet(&packet) {
            client.try_enqueue_packet(data);
        }
    }
}
