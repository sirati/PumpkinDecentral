use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use pumpkin_protocol::StatusResponse;
use pumpkin_util::text::TextComponent;
use tokio::sync::mpsc;

use crate::plugin::server::list_ping::ServerListPingEvent;

use super::Server;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StatusHookKey {
    host: String,
    address: IpAddr,
    max_players: u32,
    online: u32,
    description: String,
}

#[derive(Clone)]
struct StatusHookView {
    published: Instant,
    motd: TextComponent,
    max_players: u32,
    online: u32,
    favicon: Option<String>,
}

pub struct StatusHookRequest {
    key: StatusHookKey,
    event: ServerListPingEvent,
}

pub struct StatusHooks {
    tx: mpsc::Sender<StatusHookRequest>,
    view: Arc<ArcSwap<HashMap<StatusHookKey, StatusHookView>>>,
}

impl StatusHooks {
    pub fn new() -> (Self, mpsc::Receiver<StatusHookRequest>) {
        let (tx, rx) = mpsc::channel(64);
        (
            Self {
                tx,
                view: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            },
            rx,
        )
    }

    pub fn apply_and_refresh(
        &self,
        hostname: String,
        address: SocketAddr,
        response: &mut StatusResponse,
    ) {
        let (max_players, online) = response
            .players
            .as_ref()
            .map_or((0, 0), |players| (players.max, players.online));
        let key = StatusHookKey {
            host: hostname.clone(),
            address: address.ip(),
            max_players,
            online,
            description: response.description.clone().to_pretty_console(),
        };
        let event = ServerListPingEvent::new(
            hostname,
            address,
            response.description.clone(),
            max_players,
            online,
            response.favicon.clone(),
        );
        let view = self.view.load();
        if let Some(cached) = view
            .get(&key)
            .filter(|cached| cached.published.elapsed() < Duration::from_secs(1))
        {
            response.description = cached.motd.clone();
            response.favicon = cached.favicon.clone();
            if let Some(players) = &mut response.players {
                players.max = cached.max_players;
                players.online = cached.online;
            }
        } else {
            let _ = self.tx.try_send(StatusHookRequest { key, event });
        }
    }
}

pub fn spawn(server: &Arc<Server>, mut rx: mpsc::Receiver<StatusHookRequest>) {
    let bank = Arc::clone(&server.cluster_status_hooks.view);
    let server = Arc::downgrade(server);
    tokio::spawn(async move {
        let mut views = HashMap::<StatusHookKey, StatusHookView>::new();
        let mut warning = None::<Instant>;
        while let Some(mut request) = rx.recv().await {
            let Some(server) = server.upgrade() else {
                break;
            };
            if views
                .get(&request.key)
                .is_some_and(|cached| cached.published.elapsed() < Duration::from_secs(1))
            {
                continue;
            }
            let baseline = request.event.clone();
            if tokio::time::timeout(
                Duration::from_secs(5),
                server.plugin_manager.fire(&server, &mut request.event),
            )
            .await
            .is_err()
            {
                let now = Instant::now();
                if warning.is_none_or(|last| now.duration_since(last) >= Duration::from_secs(300)) {
                    warning = Some(now);
                    tracing::warn!("cluster status plugin callback timed out");
                }
                request.event = baseline;
            }
            let now = Instant::now();
            views
                .retain(|_, cached| now.duration_since(cached.published) < Duration::from_secs(30));
            if views.len() >= 512
                && let Some(oldest) = views
                    .iter()
                    .min_by_key(|(_, cached)| cached.published)
                    .map(|(key, _)| key.clone())
            {
                views.remove(&oldest);
            }
            views.insert(
                request.key,
                StatusHookView {
                    published: now,
                    motd: request.event.motd,
                    max_players: request.event.max_players,
                    online: request.event.num_players,
                    favicon: request.event.favicon,
                },
            );
            bank.store(Arc::new(views.clone()));
        }
    });
}
