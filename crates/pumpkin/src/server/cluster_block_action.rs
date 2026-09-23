use pumpkin_cluster::block_action::BlockActionIntent;
use tokio::sync::mpsc;

use super::Server;

pub(crate) type BlockActionRoute = Option<mpsc::Sender<BlockActionIntent>>;

pub(crate) fn attach_block_action_owner(server: &Server) -> mpsc::Receiver<BlockActionIntent> {
    let (tx, rx) = mpsc::channel(256);
    server.cluster_block_action_route_tx.send_replace(Some(tx));
    rx
}

pub(crate) fn submit_block_action(
    server: &Server,
    intent: BlockActionIntent,
) -> Result<(), BlockActionIntent> {
    let Some(route) = server.cluster_block_action_route_tx.borrow().clone() else {
        return Err(intent);
    };
    route.try_send(intent).map_err(|error| error.into_inner())
}
