use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure};
use pumpkin_cluster::entity_body::EntitySpatial;
use pumpkin_cluster::mob_breeding::MobBirthAdmission;
use pumpkin_cluster::mob_spawn_command::{NaturalMobSpawn, NaturalSpawnCause};

pub(super) fn natural_admission(command: &NaturalMobSpawn, reference: ActionRef)
    -> Result<MobBirthAdmission, ApplyFailure> {
    if !command.valid() || command.cause != NaturalSpawnCause::Natural
        || reference.id.actor != ActorId::Peer(command.identity.source.server)
        || reference.id.incarnation != command.identity.source.generation
        || reference.tick.0 != command.rules.slot as u16 {
        return Err(ApplyFailure::InvalidAction);
    }
    let kind = pumpkin_data::entity::EntityType::from_raw(command.kind)
        .filter(|kind| kind.mob).ok_or(ApplyFailure::InvalidAction)?;
    let chunk = command.chunk().ok_or(ApplyFailure::InvalidAction)?;
    let spatial = EntitySpatial { world: chunk.world, position: command.position,
        velocity_micro: [0; 3], yaw: ((command.seed % 360_000) as f32 / 1000.0).to_bits(),
        pitch: 0.0_f32.to_bits(), on_ground: false };
    let root = pumpkin_nbt::compound::NbtCompound::new();
    let initial = super::cluster_owned_mob_initial::parse_initialization(kind, &spatial, &root)?;
    let (body, inventory) = super::cluster_owned_mob_initial::prepare_typed_body(
        &command.identity, reference, command.identity.source, kind, spatial, &initial)?;
    super::cluster_owned_mob_birth::paired_admission(body, chunk, 0, inventory)
}

pub(super) fn spawn_position_ok(
    kind: &'static pumpkin_data::entity::EntityType,
    position: pumpkin_cluster::protocol::BlockPos,
    within_border: bool,
    mut block: impl FnMut(pumpkin_cluster::protocol::BlockPos) -> Result<&'static pumpkin_data::BlockState, ApplyFailure>,
) -> Result<bool, ApplyFailure> {
    use pumpkin_data::entity::SpawnLocation;
    use pumpkin_data::fluid::Fluid;
    let above = pumpkin_cluster::protocol::BlockPos { y: position.y.checked_add(1).ok_or(ApplyFailure::InvalidAction)?, ..position };
    let below = pumpkin_cluster::protocol::BlockPos { y: position.y.checked_sub(1).ok_or(ApplyFailure::InvalidAction)?, ..position };
    match kind.spawn_restriction.location {
        SpawnLocation::Unrestricted => Ok(true),
        _ if !within_border => Ok(false),
        SpawnLocation::InLava => Ok(pumpkin_world::fluid_snapshot::fluid_from_block_state(block(position)?.id).matches_type(&Fluid::LAVA)),
        SpawnLocation::InWater => Ok(pumpkin_world::fluid_snapshot::fluid_from_block_state(block(position)?.id).matches_type(&Fluid::WATER)
            && !block(above)?.is_full_cube()),
        SpawnLocation::OnGround => Ok(crate::world::natural_spawner::is_valid_spawn_floor(block(below)?, kind)
            && crate::world::natural_spawner::is_valid_empty_spawn_block(block(position)?, kind)
            && crate::world::natural_spawner::is_valid_empty_spawn_block(block(above)?, kind)),
    }
}
