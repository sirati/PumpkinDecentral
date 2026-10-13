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
    let result = server
        .cluster_command_suggestions_tx
        .try_send(SuggestionRequest {
            client,
            source,
            command,
            id,
        });
    match result {
        Ok(()) => true,
        Err(error) => {
            send_empty(error.into_inner());
            false
        }
    }
}

fn send_empty(request: SuggestionRequest) {
    if let ClientPlatform::Java(client) = request.client.as_ref() {
        client.try_send_packet(&CCommandSuggestions::new(
            request.id,
            (request.command.len() as i32).into(),
            0.into(),
            Box::new([]),
        ));
    }
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
    let (failure_tx, mut failure_rx) = mpsc::channel::<usize>(256);
    server.spawn_task(async move {
        let mut failures = std::collections::HashMap::<usize, std::time::Instant>::new();
        loop {
            let request = tokio::select! {
                request = rx.recv() => {
                    let Some(request) = request else { break };
                    request
                }
                client = failure_rx.recv() => {
                    let Some(client) = client else { break };
                    let now = std::time::Instant::now();
                    failures.retain(|_, last| now.duration_since(*last).as_secs() < 300);
                    if !failures.contains_key(&client) && failures.len() < 4096 {
                        failures.insert(client, now);
                        tracing::warn!(client, "cluster plugin suggestions did not complete");
                    }
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                send_empty(request);
                continue;
            };
            let server = Arc::clone(&task_server);
            let failure_tx = failure_tx.clone();
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
                        super::cluster_permissions::with_source(&server, &request.source, async {
                            server
                                .command_dispatcher
                                .load()
                                .suggest_with_range(command, &request.source)
                        })
                        .await
                    })
                    .await;
                let mut sets = vec![base];
                while let Ok(result) = results.try_recv() {
                    match tokio::time::timeout(Duration::from_secs(5), result).await {
                        Ok(Ok(suggestions)) => sets.push(suggestions),
                        Ok(Err(_)) | Err(_) => {
                            let _ = failure_tx.try_send(Arc::as_ptr(&request.client) as usize);
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
