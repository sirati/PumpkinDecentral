use pumpkin_cluster::entity_body::{VehicleKind, VehicleState};
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::Metadata;
use pumpkin_protocol::ser::WritingError;
use pumpkin_util::version::JavaMinecraftVersion;

pub(super) fn bedrock(
    vehicle: &VehicleState,
) -> pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList {
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        MetadataValue, SyncedActorDataList, entity_data_key as key,
    };
    let mut data = SyncedActorDataList::new();
    data.set(
        key::HURT,
        MetadataValue::Int(i32::from(vehicle.wobble_ticks)),
    );
    data.set(
        key::HURT_DIRECTION,
        MetadataValue::Int(i32::from(vehicle.wobble_side)),
    );
    if matches!(vehicle.kind, VehicleKind::Boat | VehicleKind::ChestBoat) {
        data.set(
            key::ROW_TIME_LEFT,
            MetadataValue::Float(if vehicle.paddle_left { 1.0 } else { 0.0 }),
        );
        data.set(
            key::ROW_TIME_RIGHT,
            MetadataValue::Float(if vehicle.paddle_right { 1.0 } else { 0.0 }),
        );
    }
    if let Some(fuse) = vehicle.fuse_ticks {
        data.set(key::FUSE_TIME, MetadataValue::Int(i32::from(fuse)));
    }
    data
}

pub(super) fn write_java(
    vehicle: &VehicleState,
    version: JavaMinecraftVersion,
    metadata: &mut Vec<u8>,
) -> Result<(), WritingError> {
    let boat = matches!(vehicle.kind, VehicleKind::Boat | VehicleKind::ChestBoat);
    let hurt = if boat {
        pumpkin_data::tracked_data::boat::ID_HURT
    } else {
        pumpkin_data::tracked_data::abstract_minecart::ID_HURT
    };
    let direction = if boat {
        pumpkin_data::tracked_data::boat::ID_HURTDIR
    } else {
        pumpkin_data::tracked_data::abstract_minecart::ID_HURTDIR
    };
    let damage = if boat {
        pumpkin_data::tracked_data::boat::ID_DAMAGE
    } else {
        pumpkin_data::tracked_data::abstract_minecart::ID_DAMAGE
    };
    if hurt.id.get(&version) != 255 {
        Metadata::new(hurt, VarInt(i32::from(vehicle.wobble_ticks)))
            .write(&mut *metadata, &version)?;
    }
    if direction.id.get(&version) != 255 {
        Metadata::new(direction, VarInt(i32::from(vehicle.wobble_side)))
            .write(&mut *metadata, &version)?;
    }
    if damage.id.get(&version) != 255 {
        Metadata::new(damage, vehicle.damage_milli as f32 / 1000.0)
            .write(&mut *metadata, &version)?;
    }
    if boat {
        let left = pumpkin_data::tracked_data::boat::ID_PADDLE_LEFT;
        let right = pumpkin_data::tracked_data::boat::ID_PADDLE_RIGHT;
        if left.id.get(&version) != 255 {
            Metadata::new(left, vehicle.paddle_left).write(&mut *metadata, &version)?;
        }
        if right.id.get(&version) != 255 {
            Metadata::new(right, vehicle.paddle_right).write(&mut *metadata, &version)?;
        }
    }
    if vehicle.kind == VehicleKind::FurnaceMinecart {
        let fuel = pumpkin_data::tracked_data::furnace_minecart::ID_FUEL;
        if fuel.id.get(&version) != 255 {
            Metadata::new(fuel, vehicle.fuel_ticks != 0).write(&mut *metadata, &version)?;
        }
    }
    Ok(())
}
