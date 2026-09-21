use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::inventory::InventoryOp;
use pumpkin_cluster::invsee::InventorySnapshot;

#[must_use]
pub fn replicated_snapshot(
    target: GlobalPlayerId,
    owner_name: &str,
) -> Option<InventorySnapshot> {
    super::cluster_world_apply::replicated_inventory_snapshot(target, owner_name.to_owned())
}

pub fn submit_inventory_ops(operations: Vec<InventoryOp>) -> bool {
    super::cluster_world_apply::submit_local_inventory_ops(operations)
}
