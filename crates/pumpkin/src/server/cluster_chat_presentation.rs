use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_util::text::TextComponent;
use tokio::sync::mpsc;

use crate::plugin::server::server_broadcast::ServerBroadcastEvent;

use super::Server;

static CALLBACK_WARN_LAST: AtomicU64 = AtomicU64::new(0);

pub struct ChatPresentation {
    pub message: TextComponent,
    pub sender: TextComponent,
    pub chat_type: u8,
}

pub fn try_enqueue(
    server: &Arc<Server>,
    message: TextComponent,
    sender: TextComponent,
    chat_type: u8,
) -> bool {
    server
        .cluster_chat_presentation_tx
        .try_send(ChatPresentation {
            message,
            sender,
            chat_type,
        })
        .is_ok()
}

fn warn_timeout() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_secs())
        .unwrap_or(0);
    let previous = CALLBACK_WARN_LAST.load(Ordering::Relaxed);
    if now.saturating_sub(previous) >= 60
        && CALLBACK_WARN_LAST
            .compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!("cluster chat broadcast plugin callback timed out");
    }
}

pub fn spawn(server: &Arc<Server>, mut rx: mpsc::Receiver<ChatPresentation>) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        while let Some(delivery) = rx.recv().await {
            let mut event =
                ServerBroadcastEvent::new(delivery.message.clone(), delivery.sender.clone());
            if tokio::time::timeout(
                std::time::Duration::from_secs(5),
                task_server.plugin_manager.fire(&task_server, &mut event),
            )
            .await
            .is_err()
            {
                warn_timeout();
                event = ServerBroadcastEvent::new(delivery.message, delivery.sender);
            }
            if !event.cancelled {
                for world in task_server.worlds.load().iter() {
                    world.broadcast_message(
                        &event.message,
                        &event.sender,
                        delivery.chat_type,
                        None,
                    );
                }
            }
        }
    });
}
