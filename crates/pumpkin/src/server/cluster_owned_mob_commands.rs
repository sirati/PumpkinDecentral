use std::sync::Arc;
use pumpkin_cluster::item_contact::ContactIdentity;
use pumpkin_cluster::mob_continuation::MobSpecies;
use crate::entity::EntityBase;

pub(crate) fn contact_entity(
    server: &super::Server,
    world: &crate::world::World,
    identity: &ContactIdentity,
) -> Option<Arc<dyn EntityBase>> {
    match identity {
        ContactIdentity::Player(lease) => {
            if let Some(player) = world.get_player_by_uuid(lease.key.0)
                .filter(|player| super::cluster_world_owner::player_action_lease(server, player) == Some(*lease)) {
                return Some(player as Arc<dyn EntityBase>);
            }
            server.cluster_remote_players.load().get(&lease.gid)
                .filter(|player| player.frame.lease == *lease
                    && player.frame.spatial.world == super::cluster_world_delta::world_id(world))
                .map(|player| player.clone() as Arc<dyn EntityBase>)
        }
        ContactIdentity::Entity { id, birth } => {
            let directory = server.cluster_canonical_entities.load();
            directory.entity(super::cluster_world_delta::world_id(world), *id)
                .filter(|entity| entity.cast_any().downcast_ref::<super::cluster_entity_registry::CanonicalEntityShell>()
                    .is_some_and(|shell| shell.body.birth == *birth))
        }
    }
}

pub(crate) fn target(server: &super::Server, world: &crate::world::World,
    entity: &dyn EntityBase) -> Option<Arc<dyn EntityBase>> {
    let living = super::cluster_attribute_commands::living(server, entity)?;
    contact_entity(server, world, living.mob?.brain.target.as_ref()?)
}

pub(crate) fn owner(server: &super::Server, world: &crate::world::World,
    entity: &dyn EntityBase) -> Option<Arc<dyn EntityBase>> {
    let living = super::cluster_attribute_commands::living(server, entity)?;
    let owner = match living.mob?.species {
        MobSpecies::Horse { owner, .. } | MobSpecies::Llama { owner, .. } => owner?,
        _ => return None,
    };
    if let Some(player) = world.get_player_by_uuid(owner) { return Some(player as Arc<dyn EntityBase>); }
    server.cluster_remote_players.load().by_uuid(owner)
        .filter(|player| player.frame.lease.key.0 == owner
            && player.frame.spatial.world == super::cluster_world_delta::world_id(world))
        .map(|player| player.clone() as Arc<dyn EntityBase>)
}
