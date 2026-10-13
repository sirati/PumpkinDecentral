use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use packet::{ClientboundPacket, Packet, PacketError, ServerboundPacket};
use pumpkin_config::RCONConfig;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    select,
};
use tracing::{debug, error, info, warn};

use crate::command::CommandSender;
use crate::{SHOULD_STOP, STOP_INTERRUPT, server::Server};

pub use pumpkin_protocol::rcon as packet;

mod completion;

use completion::{CompletionEvent, CompletionSink, PendingResponse};

pub struct RCONServer;

impl RCONServer {
    pub async fn run(config: &RCONConfig, server: Arc<Server>) {
        if config.password.trim().is_empty() {
            error!(
                "RCON is enabled but password is empty! Refusing to start RCON server for security."
            );
            return;
        }

        let listener = match tokio::net::TcpListener::bind(config.address).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind RCON server on {}: {e}", config.address);
                return;
            }
        };

        info!("RCON server is listening on {}", config.address);

        let password = Arc::new(config.password.clone());
        let connections = Arc::new(AtomicU32::new(0));

        while !SHOULD_STOP.load(Ordering::Relaxed) {
            let await_new_client = || async {
                let t1 = listener.accept();
                let t2 = STOP_INTERRUPT.cancelled();

                select! {
                    client = t1 => Some(client),
                    () = t2 => None,
                }
            };
            // Asynchronously wait for an inbound socket.

            let Some(Ok((connection, address))) = await_new_client().await else {
                break;
            };

            let current_conns = connections.load(Ordering::Relaxed);
            if config.max_connections != 0 && current_conns >= config.max_connections {
                warn!(
                    "RCON ({}): Connection rejected, maximum connections ({}) reached",
                    address, config.max_connections
                );
                continue;
            }

            connections.fetch_add(1, Ordering::Relaxed);
            let mut client = RCONClient::new(connection, address);

            let password = password.clone();
            let server = server.clone();
            let connections = connections.clone();
            tokio::spawn(async move {
                while !client.handle(&server, &password).await {}
                connections.fetch_sub(1, Ordering::Relaxed);
                if server.advanced_config.networking.rcon.logging.quit {
                    info!("RCON ({}): Client disconnected", address);
                }
                debug!("closed RCON connection with {}", address);
            });
        }
    }
}

pub struct RCONClient {
    connection: tokio::net::TcpStream,
    address: SocketAddr,
    logged_in: bool,
    incoming: Vec<u8>,
    closed: bool,
    responses: std::collections::VecDeque<PendingResponse>,
    completions: tokio::sync::mpsc::Receiver<CompletionEvent>,
    completion_tx: tokio::sync::mpsc::Sender<CompletionEvent>,
    next_sequence: u64,
}

impl RCONClient {
    #[must_use]
    pub fn new(connection: tokio::net::TcpStream, address: SocketAddr) -> Self {
        let (completion_tx, completions) = tokio::sync::mpsc::channel(65_536);
        Self {
            connection,
            address,
            logged_in: false,
            incoming: Vec::new(),
            closed: false,
            responses: std::collections::VecDeque::new(),
            completions,
            completion_tx,
            next_sequence: 1,
        }
    }

    /// Returns whether the client is closed or not.
    pub async fn handle(&mut self, server: &Arc<Server>, password: &str) -> bool {
        if self.closed {
            return true;
        }
        for _ in 0..65_536 {
            let Ok(event) = self.completions.try_recv() else {
                break;
            };
            self.complete_response(&event);
        }
        if let Err(error) = self.flush_responses(server).await {
            error!(?error, "RCON response write failed");
            return true;
        }
        let mut processed = false;
        while !self.closed && self.responses.len() < 16 {
            match self.receive_packet() {
                Ok(Some(packet)) => {
                    processed = true;
                    if let Err(error) = self.process_packet(server, password, packet).await {
                        error!(?error, "RCON command packet failed");
                        self.closed = true;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    error!(?error, "RCON packet decode failed");
                    self.closed = true;
                }
            }
        }
        if self.closed {
            return true;
        }
        if let Err(error) = self.flush_responses(server).await {
            error!(?error, "RCON response write failed");
            return true;
        }
        if processed && !self.incoming.is_empty() && self.responses.len() < 16 {
            return false;
        }
        let mut buffer = [0; 1460];
        select! {
            received = self.connection.read(&mut buffer) => {
                match received {
                    Ok(0) => return true,
                    Ok(length) => {
                        if self.incoming.len().saturating_add(length) > 65_536 {
                            error!("RCON pending input capacity exceeded");
                            return true;
                        }
                        self.incoming.extend_from_slice(&buffer[..length]);
                    },
                    Err(error) => { error!(?error, "RCON socket read failed"); return true; }
                }
            }
            event = self.completions.recv(), if !self.responses.is_empty() => {
                let Some(event) = event else { return true; };
                self.complete_response(&event);
            }
        }
        self.closed
    }

    fn complete_response(&mut self, event: &CompletionEvent) {
        if let Some(response) = self
            .responses
            .iter_mut()
            .find(|response| response.sequence == event.sequence)
        {
            response.complete(event);
        }
    }

    async fn flush_responses(&mut self, server: &Server) -> Result<(), PacketError> {
        while self.responses.front().is_some_and(PendingResponse::ready) {
            let response = self.responses.pop_front().unwrap();
            let mut lines = response.output.drain();
            if response.lost_receipts.load(Ordering::Acquire) != 0 {
                error!(
                    sequence = response.sequence,
                    "RCON completion receipt capacity exceeded"
                );
                lines.push("RCON command completion receipt capacity exceeded".into());
            }
            if lines.is_empty() {
                self.send(ClientboundPacket::Output, response.packet_id, "")
                    .await?;
            } else {
                for line in lines {
                    if server.advanced_config.networking.rcon.logging.commands {
                        info!(address = %self.address, %line, "RCON command output");
                    }
                    self.send(ClientboundPacket::Output, response.packet_id, &line)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn process_packet(
        &mut self,
        server: &Arc<Server>,
        password: &str,
        packet: Packet,
    ) -> Result<(), PacketError> {
        let config = &server.advanced_config.networking.rcon;
        match packet.get_type() {
            ServerboundPacket::Auth => {
                if !password.is_empty() && packet.get_body() == password {
                    self.send(ClientboundPacket::AuthResponse, packet.get_id(), "")
                        .await?;
                    if config.logging.logged_successfully {
                        info!("RCON ({}): Client logged in successfully", self.address);
                    }
                    self.logged_in = true;
                } else {
                    if config.logging.wrong_password {
                        info!("RCON ({}): Client tried the wrong password", self.address);
                    }
                    self.send(ClientboundPacket::AuthResponse, -1, "").await?;
                    self.closed = true;
                }
            }
            ServerboundPacket::ExecCommand => {
                if self.logged_in {
                    let output = Arc::new(crate::command::RconOutput::default());
                    let sequence = self.next_sequence;
                    self.next_sequence = self.next_sequence.wrapping_add(1);
                    let lost_receipts = Arc::new(std::sync::atomic::AtomicU64::new(0));
                    let callback = Arc::new(CompletionSink {
                        sequence,
                        events: self.completion_tx.clone(),
                        lost_receipts: lost_receipts.clone(),
                    });
                    let command_source = CommandSender::Rcon(output.clone())
                        .into_source(server)
                        .with_command_result_taker(pumpkin_command::source::ResultValueTaker(
                            vec![callback],
                        ));
                    let summary = server
                        .command_dispatcher
                        .load()
                        .handle_command_outcome(&command_source, packet.get_body());
                    let pending = summary
                        .map(|summary| {
                            if summary.pending.len() > 65_536 {
                                lost_receipts.fetch_add(1, Ordering::Relaxed);
                            }
                            summary.pending.into_iter().take(65_536).collect()
                        })
                        .unwrap_or_default();
                    self.responses.push_back(PendingResponse {
                        sequence,
                        packet_id: packet.get_id(),
                        output,
                        pending,
                        lost_receipts,
                    });
                    if config.logging.commands {
                        info!(address = %self.address, command = packet.get_body(), "RCON command submitted");
                    }
                } else {
                    if config.logging.wrong_password {
                        info!(
                            "RCON ({}): Unauthenticated client tried to execute command",
                            self.address
                        );
                    }
                    self.send(ClientboundPacket::AuthResponse, -1, "").await?;
                    self.closed = true;
                }
            }
        }
        Ok(())
    }

    async fn send(
        &mut self,
        packet: ClientboundPacket,
        id: i32,
        body: &str,
    ) -> Result<(), PacketError> {
        let buf = packet.write_buf(id, body);
        self.connection
            .write_all(&buf)
            .await
            .map_err(PacketError::FailedSend)?;
        Ok(())
    }

    fn receive_packet(&mut self) -> Result<Option<Packet>, PacketError> {
        Packet::deserialize(&mut self.incoming)
    }
}
