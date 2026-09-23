use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pumpkin_command::suggestion::suggestions::Suggestions;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{CCommandSuggestions, CommandSuggestion};
use tokio::sync::{mpsc, oneshot};

use crate::command::CommandSource;
use crate::net::ClientPlatform;

use super::Server;

tokio::task_local! {
    static SUGGESTION_TASKS: mpsc::Sender<oneshot::Receiver<Suggestions>>;
}

pub struct SuggestionRequest {
    client: Arc<ClientPlatform>,
    source: CommandSource,
    command: String,
    id: VarInt,
}

pub fn submit(
    server: &Server,
    client: Arc<ClientPlatform>,
    source: CommandSource,
    command: String,
    id: VarInt,
) -> bool {
    server
        .cluster_command_suggestions_tx
        .try_send(SuggestionRequest {
            client,
            source,
            command,
            id,
        })
        .is_ok()
}

pub fn register<F>(server: &Server, future: F) -> bool
where
    F: Future<Output = Suggestions> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let admitted = SUGGESTION_TASKS
        .try_with(|tasks| tasks.try_send(receiver).is_ok())
        .unwrap_or(false);
    if admitted {
        server.spawn_task(async move {
            let result = future.await;
            let _ = sender.send(result);
        });
    }
    admitted
}

pub fn spawn(server: &Arc<Server>, mut rx: mpsc::Receiver<SuggestionRequest>) {
    let task_server = Arc::clone(server);
    let permits = Arc::new(tokio::sync::Semaphore::new(32));
    server.spawn_task(async move {
        while let Some(request) = rx.recv().await {
            let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                break;
            };
            let server = Arc::clone(&task_server);
            task_server.spawn_task(async move {
                let _permit = permit;
                let Some(command) = request.command.strip_prefix('/') else {
                    return;
                };
                let start = command
                    .char_indices()
                    .rfind(|(_, c)| c.is_whitespace())
                    .map_or(0, |(index, c)| index + c.len_utf8());
                let (tasks, mut results) = mpsc::channel(16);
                let base = SUGGESTION_TASKS
                    .scope(tasks, async {
                        server
                            .command_dispatcher
                            .load()
                            .suggest_with_range(command, &request.source)
                    })
                    .await;
                let mut sets = vec![base];
                while let Ok(result) = results.try_recv() {
                    match tokio::time::timeout(Duration::from_secs(5), result).await {
                        Ok(Ok(suggestions)) => sets.push(suggestions),
                        Ok(Err(_)) | Err(_) => {
                            tracing::warn!("cluster plugin suggestions did not complete");
                        }
                    }
                }
                let matches = Suggestions::merge(command, &sets)
                    .suggestions
                    .into_iter()
                    .map(|suggestion| CommandSuggestion {
                        suggestion: suggestion.text.cached_text().clone(),
                        tooltip: suggestion.tooltip,
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                if let ClientPlatform::Java(client) = request.client.as_ref() {
                    client.try_send_packet(&CCommandSuggestions::new(
                        request.id,
                        ((start + 1) as i32).into(),
                        ((command.len() - start) as i32).into(),
                        matches,
                    ));
                }
            });
        }
    });
}
