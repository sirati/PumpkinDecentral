use tokio::sync::mpsc;

use pumpkin_cluster::controller::ControllerFrame;
use pumpkin_cluster::identity::PlayerSeq;
use pumpkin_cluster::inventory::InventoryApplyError;
use pumpkin_cluster::item_action::ItemActionIntent;

use super::Server;

pub(crate) struct LocalItemActionIntent {
    pub intent: ItemActionIntent,
    pub origin: Option<u64>,
    pub input_sequence: Option<PlayerSeq>,
    pub effective_frame: Option<(ControllerFrame, f64)>,
}

pub(crate) type ItemActionRoute = Option<mpsc::Sender<LocalItemActionIntent>>;

pub(crate) fn attach_item_action_owner(server: &Server) -> mpsc::Receiver<LocalItemActionIntent> {
    let (tx, rx) = mpsc::channel(256);
    let _ = server.cluster_item_action_route_tx.send_replace(Some(tx));
    rx
}

pub(crate) fn submit_item_action_with_sequence(
    server: &Server,
    intent: ItemActionIntent,
    origin: Option<u64>,
    input_sequence: Option<PlayerSeq>,
    effective_frame: Option<(ControllerFrame, f64)>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_item_action_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalItemActionIntent {
            intent,
            origin,
            input_sequence,
            effective_frame,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}
