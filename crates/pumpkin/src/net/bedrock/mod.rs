pub mod nethernet;
pub mod play;
pub mod status;
use crossbeam::atomic::AtomicCell;
use std::{
    collections::HashMap,
    io::{Cursor, Error, Write},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use tracing::{debug, error, warn};

use bytes::Bytes;
use pumpkin_config::networking::compression::CompressionInfo;
use pumpkin_protocol::{
    BClientPacket, PacketDecodeError, RawPacket,
    bedrock::{
        BEDROCK_GAME_PACKET, SubClient,
        client::{
            client_cache_miss_response::{CClientCacheMissResponse, MissingBlobData},
            disconnect::CDisconnect,
        },
        packet_decoder::BedrockBatchDecoder,
        packet_encoder::BedrockBatchEncoder,
        server::{
            actor_event::SActorEvent, animate::SAnimate, block_pick_request::SBlockPickRequest,
            client_cache_blob_status::SClientCacheBlobStatus,
            client_cache_status::SClientCacheStatus, command_request::SCommandRequest,
            container_close::SContainerClose, emote::SEmote, emote_list::SEmoteList,
            interact::SInteract, inventory_transaction::SInventoryTransaction,
            loading_screen::SLoadingScreen, login::SLogin, mob_equipment::SMobEquipment,
            packet_violation_warning::SPacketViolationWarning, player_action::SPlayerAction,
            player_auth_input::SPlayerAuthInput, request_ability::SRequestAbility,
            request_chunk_radius::SRequestChunkRadius,
            request_network_settings::SRequestNetworkSettings,
            resource_pack_client_response::SResourcePackClientResponse, respawn::SRespawn,
            set_local_player_as_initialized::SSetLocalPlayerAsInitialized,
            set_player_inventory_options::SSetPlayerInventoryOptions, text::SText,
        },
    },
    packet::Packet,
    serial::{PacketRead, PacketReadSlice},
};
use tokio::{
    sync::mpsc::{Receiver, Sender, error::TryRecvError},
    sync::{Mutex, oneshot},
    task::JoinHandle,
};

use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub mod level_chunk;
pub mod login;
use self::level_chunk::CLevelChunk;
use self::nethernet::NetherNetSession;
use crate::{
    entity::player::Player,
    net::{
        DisconnectReason, MAX_PENDING_BYTES, PacketHandlerResult, PacketRateLimiter,
        decrement_pending_bytes,
    },
    plugin::api::events::world::chunk_send::ChunkSend,
    server::Server,
};
use arc_swap::ArcSwap;
use pumpkin_protocol::bedrock::server::login::ClientData;
use pumpkin_util::version::BedrockMinecraftVersion;
use pumpkin_world::level::SyncChunk;

pub struct OutgoingPacket {
    pub data: Bytes,
    pub completion: Option<oneshot::Sender<()>>,
}

struct IncomingPacketLease<'a> {
    owner: &'a crossbeam::queue::ArrayQueue<Receiver<RawPacket>>,
    receiver: Option<Receiver<RawPacket>>,
}

impl Drop for IncomingPacketLease<'_> {
    fn drop(&mut self) {
        if let Some(receiver) = self.receiver.take() {
            if self.owner.push(receiver).is_err() {
                tracing::error!("Bedrock inbound ownership returned to occupied slot");
            }
        }
    }
}

impl OutgoingPacket {
    pub const fn normal(data: Bytes) -> Self {
        Self {
            data,
            completion: None,
        }
    }

    pub const fn priority(data: Bytes, completion: oneshot::Sender<()>) -> Self {
        Self {
            data,
            completion: Some(completion),
        }
    }
}

pub struct BedrockClient {
    session: Arc<NetherNetSession>,
    /// The client's IP address.
    pub address: SocketAddr,
    pub player: ArcSwap<Option<Arc<Player>>>,
    pub version: AtomicCell<BedrockMinecraftVersion>,
    pub client_data: ArcSwap<Option<Arc<ClientData>>>,
    tasks: TaskTracker,
    rt_handle: tokio::runtime::Handle,
    outgoing_packet_queue_send: Sender<OutgoingPacket>,
    /// A queue of serialized packets to send to the network
    outgoing_packet_queue_recv: crossbeam::queue::ArrayQueue<Receiver<OutgoingPacket>>,

    outgoing_packet_priority_send: Sender<OutgoingPacket>,
    outgoing_packet_priority_recv: crossbeam::queue::ArrayQueue<Receiver<OutgoingPacket>>,

    /// Tracks total buffered payload bytes in the outgoing queues.
    pub pending_bytes: Arc<AtomicUsize>,

    /// The packet encoder for outgoing packets.
    network_writer: arc_swap::ArcSwap<BedrockBatchEncoder>,
    /// The packet decoder for incoming packets.
    network_reader: ArcSwap<BedrockBatchDecoder>,

    /// The next form ID to use for custom forms.
    pub next_form_id: AtomicU32,
    pub inventory_opened: AtomicBool,
    /// Separate from normal vitals caching so the first rejected use always gets corrected.
    last_food_rejection_tick: AtomicCell<Option<i32>>,
    pub client_cache_supported: AtomicBool,
    pub blob_cache: std::sync::Mutex<HashMap<u64, Vec<u8>>>,
    /// An notifier that is triggered when this client is closed.
    close_token: CancellationToken,
    last_seen: Arc<AtomicCell<std::time::Instant>>,
    lobby_requests: crossbeam::queue::ArrayQueue<bool>,
    lobby_notify: tokio::sync::Notify,
    incoming_game_packet_send: Sender<RawPacket>,
    incoming_game_packet_recv: crossbeam::queue::ArrayQueue<Receiver<RawPacket>>,
    /// Packet rate limiter for incoming client packets.
    pub packet_limiter: PacketRateLimiter,
}

impl BedrockClient {
    #[must_use]
    pub fn new(
        session: Arc<NetherNetSession>,
        address: SocketAddr,
        packet_limiter: PacketRateLimiter,
    ) -> Self {
        let (send, recv) = tokio::sync::mpsc::channel(4096);
        let (priority_send, priority_recv) = tokio::sync::mpsc::channel(256);
        let (incoming_send, incoming_recv) = tokio::sync::mpsc::channel(4096);
        let rt_handle = tokio::runtime::Handle::current();
        let outgoing = crossbeam::queue::ArrayQueue::new(1);
        let _ = outgoing.push(recv);
        let priority = crossbeam::queue::ArrayQueue::new(1);
        let _ = priority.push(priority_recv);
        let incoming = crossbeam::queue::ArrayQueue::new(1);
        let _ = incoming.push(incoming_recv);
        Self {
            session,
            player: ArcSwap::new(Arc::new(None)),
            address,
            version: AtomicCell::new(BedrockMinecraftVersion::Unknown),
            client_data: ArcSwap::new(Arc::new(None)),
            network_writer: arc_swap::ArcSwap::from_pointee(BedrockBatchEncoder::new()),
            network_reader: ArcSwap::from_pointee(BedrockBatchDecoder::new()),
            tasks: TaskTracker::new(),
            rt_handle,
            outgoing_packet_queue_send: send,
            outgoing_packet_queue_recv: outgoing,
            outgoing_packet_priority_send: priority_send,
            outgoing_packet_priority_recv: priority,
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            next_form_id: AtomicU32::new(0),
            inventory_opened: AtomicBool::new(false),
            last_food_rejection_tick: AtomicCell::new(None),
            client_cache_supported: AtomicBool::new(false),
            blob_cache: std::sync::Mutex::new(HashMap::new()),
            close_token: CancellationToken::new(),
            last_seen: Arc::new(AtomicCell::new(std::time::Instant::now())),
            lobby_requests: crossbeam::queue::ArrayQueue::new(1),
            lobby_notify: tokio::sync::Notify::new(),
            incoming_game_packet_send: incoming_send,
            incoming_game_packet_recv: incoming,
            packet_limiter,
        }
    }

    pub async fn get_packet(&self) -> Option<RawPacket> {
        let receiver = self.incoming_game_packet_recv.pop()?;
        let mut guard = IncomingPacketLease {
            owner: &self.incoming_game_packet_recv,
            receiver: Some(receiver),
        };
        let recv = guard.receiver.as_mut()?;
        tokio::select! {
            () = self.await_close_interrupt() => None,
            packet = recv.recv() => packet,
        }
    }

    pub fn start_outgoing_packet_task(self: &Arc<Self>) {
        const MAX_BATCH_SIZE: usize = 64;

        let client = self.clone();
        self.spawn_task(async move {
            let Some(mut packet_receiver) = client.outgoing_packet_queue_recv.pop() else {
                return;
            };
            let Some(mut priority_packet_receiver) = client.outgoing_packet_priority_recv.pop()
            else {
                return;
            };
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

            loop {
                let recv_result = tokio::select! {
                    biased;
                    res = priority_packet_receiver.recv() => res,
                    res = packet_receiver.recv() => res,
                    _ = interval.tick() => {
                        if !client.tick_connection().await {
                            break;
                        }
                        continue;
                    }
                    () = client.close_token.cancelled() => {
                        priority_packet_receiver
                            .try_recv()
                            .ok()
                            .or_else(|| packet_receiver.try_recv().ok())
                    }
                };

                let Some(packet) = recv_result else {
                    break;
                };

                let mut packet_batch = Vec::with_capacity(MAX_BATCH_SIZE);
                packet_batch.push(packet);

                while packet_batch.len() < MAX_BATCH_SIZE {
                    match priority_packet_receiver.try_recv() {
                        Ok(packet) => {
                            packet_batch.push(packet);
                            continue;
                        }
                        Err(TryRecvError::Disconnected | TryRecvError::Empty) => {}
                    }

                    match packet_receiver.try_recv() {
                        Ok(packet) => packet_batch.push(packet),
                        Err(TryRecvError::Disconnected | TryRecvError::Empty) => break,
                    }
                }

                for packet in packet_batch {
                    let packet_len = packet.data.len();
                    let data = packet.data.strip_prefix(&[BEDROCK_GAME_PACKET]);
                    let Some(data) = data else {
                        warn!("Refusing to send a non-game packet over NetherNet");
                        decrement_pending_bytes(&client.pending_bytes, packet_len);
                        continue;
                    };
                    if let Err(error) = client.session.send(Bytes::copy_from_slice(data)).await {
                        warn!(
                            "Failed to send NetherNet packet to {}: {error}",
                            client.address
                        );
                        decrement_pending_bytes(&client.pending_bytes, packet_len);
                        client.close().await;
                        return;
                    }

                    decrement_pending_bytes(&client.pending_bytes, packet_len);

                    if let Some(completion) = packet.completion {
                        let _ = completion.send(());
                    }
                }
            }
        });
    }

    async fn tick_connection(&self) -> bool {
        if self.last_seen.load().elapsed() > std::time::Duration::from_secs(10) {
            debug!("Bedrock client {} timed out", self.address);
            self.close().await;
            return false;
        }
        true
    }

    pub async fn process_nethernet_packet(self: &Arc<Self>, server: &Arc<Server>, packet: Bytes) {
        self.last_seen.store(std::time::Instant::now());
        let mut batch = Vec::with_capacity(packet.len() + 1);
        batch.push(BEDROCK_GAME_PACKET);
        batch.extend_from_slice(&packet);
        if let Err(error) = self.process_batch(server, batch).await {
            error!(
                "Failed to handle NetherNet payload for {}: {error}",
                self.address
            );
            self.kick(DisconnectReason::BadPacket, error.to_string())
                .await;
        }
    }

    pub fn nethernet_public_key(&self) -> Option<&pumpkin_auth::p384::PublicKey> {
        self.session.client_public_key()
    }

    pub async fn set_compression(&self, compression: CompressionInfo) {
        let mut decoder = self.network_reader.load().as_ref().clone();
        decoder.set_compression(compression.threshold as usize);
        self.network_reader.store(Arc::new(decoder));

        let mut encoder = self.network_writer.load().as_ref().clone();
        encoder.set_compression((compression.threshold as usize, compression.level));
        self.network_writer.store(Arc::new(encoder));
    }

    pub fn try_kick(&self, reason: DisconnectReason, message: String) {
        warn!("Closing connection for {}: {message}", self.address);
        let packet = CDisconnect::new(reason as i32, message);
        if let Ok(data) = self.serialize_packet(&packet) {
            let packet_len = data.len();
            let _ = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
            if self
                .outgoing_packet_priority_send
                .try_send(OutgoingPacket::normal(data))
                .is_err()
            {
                decrement_pending_bytes(&self.pending_bytes, packet_len);
            }
        }
        if !self.close_token.is_cancelled() {
            self.close_token.cancel();
        }
    }

    pub async fn kick(&self, reason: DisconnectReason, message: String) {
        warn!("Closing connection for {}: {message}", self.address);
        self.send_packet(&CDisconnect::new(reason as i32, message))
            .await;
        self.close().await;
    }

    pub async fn kick_explicit(
        &self,
        reason: DisconnectReason,
        message: String,
        skip_message: bool,
        filtered_message: String,
        send_packet: bool,
    ) {
        warn!("Closing connection for {}: {message}", self.address);
        if send_packet {
            self.send_packet(&CDisconnect {
                reason: pumpkin_protocol::codec::var_int::VarInt(reason as i32),
                skip_message,
                message,
                filtered_message,
            })
            .await;
        }
        self.close().await;
    }

    pub async fn send_chunks(&self, chunks: &[SyncChunk]) {
        let player = self.player.load_full();
        let Some(player) = player.as_ref() else {
            debug!(
                "send_chunks: player not set yet, dropping {} chunks",
                chunks.len()
            );
            return;
        };
        let Some(server) = player.world().server.upgrade() else {
            return;
        };

        let mut valid_chunks = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let mut event = ChunkSend::new(player.world(), chunk.clone());
            server.plugin_manager.fire(&server, &mut event).await;
            if !event.cancelled {
                valid_chunks.push(chunk.clone());
            }
        }

        if valid_chunks.is_empty() {
            return;
        }

        let bedrock_dimension =
            if player.world().dimension == pumpkin_data::dimension::Dimension::THE_NETHER {
                1
            } else if player.world().dimension == pumpkin_data::dimension::Dimension::THE_END {
                2
            } else {
                0
            };

        let cache_enabled = server.advanced_config.networking.bedrock.chunk_caching
            && self.client_cache_supported.load(Ordering::Relaxed);

        let world = player.world();
        let (tx, rx) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let mut encoded_payloads = Vec::with_capacity(valid_chunks.len());
            let mut new_blobs = Vec::new();
            for chunk in valid_chunks {
                let block_actors = world.bedrock_chunk_block_actors(&chunk);
                match CLevelChunk::encode_chunk(
                    &chunk,
                    bedrock_dimension,
                    cache_enabled,
                    &block_actors,
                ) {
                    Ok((payload, blobs)) => {
                        encoded_payloads.push(payload);
                        new_blobs.extend(blobs);
                    }
                    Err(e) => error!("Failed to serialize Bedrock chunk: {:?}", e),
                }
            }
            let _ = tx.send((encoded_payloads, new_blobs));
        });

        let Ok((encoded_payloads, new_blobs)) = rx.await else {
            return;
        };

        if !new_blobs.is_empty() {
            let mut cache = self
                .blob_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (hash, payload) in new_blobs {
                cache.insert(hash, payload);
            }
        }

        let mut packets_to_enqueue = Vec::with_capacity(encoded_payloads.len());
        {
            let encoder = self.network_writer.load();
            for payload in encoded_payloads {
                let mut packet_buf = Vec::new();
                match encoder.write_game_packet(
                    CLevelChunk::PACKET_ID as u16,
                    SubClient::Main,
                    SubClient::Main,
                    &payload,
                    &mut packet_buf,
                ) {
                    Ok(()) => packets_to_enqueue.push(packet_buf),
                    Err(err) => error!("Failed to write game packet wrapper: {err}"),
                }
            }
        }
        for packet_buf in packets_to_enqueue {
            self.enqueue_packet_data(packet_buf.into()).await;
        }
    }

    pub fn set_player(&self, player: Arc<Player>) {
        self.player.store(Arc::new(Some(player)));
    }

    #[allow(clippy::unused_async)]
    pub async fn enqueue_packet(&self, packet_data: Bytes) {
        self.try_enqueue_packet_data(packet_data);
    }

    pub(crate) fn try_enqueue_packet_burst(&self, packets: Vec<Bytes>) -> Result<(), Vec<Bytes>> {
        if self.is_closed() { return Err(packets); }
        super::try_enqueue_packet_burst(&self.outgoing_packet_queue_send, &self.pending_bytes, packets, OutgoingPacket::normal)
    }

    pub fn try_enqueue_packet(&self, packet_data: Bytes) {
        self.try_enqueue_packet_data(packet_data);
    }

    pub(crate) fn try_enqueue_owned_chunk(
        &self,
        payload: &Bytes,
    ) -> Result<oneshot::Receiver<()>, String> {
        let mut framed = Vec::new();
        self.network_writer
            .load()
            .write_game_packet(58, SubClient::Main, SubClient::Main, payload, &mut framed)
            .map_err(|error| error.to_string())?;
        let (completion, receipt) = oneshot::channel();
        if self.is_closed() {
            return Err("Bedrock transport closed".into());
        }
        let packet_data: Bytes = framed.into();
        let packet_len = packet_data.len();
        let previous = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
        if previous.saturating_add(packet_len) > MAX_PENDING_BYTES {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            self.close_token.cancel();
            return Err("Bedrock chunk transport byte capacity exhausted".into());
        }
        if self
            .outgoing_packet_queue_send
            .try_send(OutgoingPacket {
                data: packet_data,
                completion: Some(completion),
            })
            .is_err()
        {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            self.close_token.cancel();
            return Err("Bedrock chunk transport queue exhausted".into());
        }
        Ok(receipt)
    }

    /// Queues a clientbound packet to be sent to the connected client. Queued chunks are sent
    /// in-order to the client
    ///
    /// # Arguments
    ///
    /// * `packet_data`: A `Bytes` payload representing the encoded packet.
    #[allow(clippy::unused_async)]
    pub async fn enqueue_packet_data(&self, packet_data: Bytes) {
        self.try_enqueue_packet_data(packet_data);
    }

    pub fn try_enqueue_packet_data(&self, packet_data: Bytes) {
        if self.is_closed() {
            return;
        }

        let packet_len = packet_data.len();
        let prev_bytes = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
        let new_bytes = prev_bytes.saturating_add(packet_len);

        if new_bytes > MAX_PENDING_BYTES {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            if !self.is_closed() {
                warn!(
                    "Bedrock client {} outbound packet buffer overflow ({} bytes > {} bytes). Closing connection.",
                    self.address, new_bytes, MAX_PENDING_BYTES
                );
                self.close_token.cancel();
            }
            return;
        }

        if let Err(err) = self
            .outgoing_packet_queue_send
            .try_send(OutgoingPacket::normal(packet_data))
        {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            // This is expected to fail if we are closed
            if !self.is_closed() {
                error!("Failed to add packet to the outgoing packet queue for client: {err}");
                self.close_token.cancel();
            }
        }
    }

    pub fn write_raw_packet<P: BClientPacket>(
        packet: &P,
        mut writer: impl Write,
    ) -> Result<(), Error> {
        writer.write_all(&[P::PACKET_ID as u8])?;
        packet.write_packet(writer)
    }

    pub async fn write_game_packet<P: BClientPacket>(
        &self,
        packet: &P,
        write: impl Write,
    ) -> Result<(), Error> {
        let mut packet_payload = Vec::new();
        packet.write_packet(&mut packet_payload)?;

        let encoder = self.network_writer.load();
        encoder.write_game_packet(
            P::PACKET_ID as u16,
            SubClient::Main,
            SubClient::Main,
            &packet_payload,
            write,
        )
    }

    pub fn serialize_packet<P: BClientPacket>(&self, packet: &P) -> Result<Bytes, Error> {
        self.network_writer.load().serialize_packet(packet)
    }

    pub(crate) fn encoder_snapshot(&self) -> Arc<BedrockBatchEncoder> {
        self.network_writer.load_full()
    }

    pub async fn send_packet<P: BClientPacket>(&self, packet: &P) {
        let mut data = Vec::new();
        match self.write_game_packet(packet, &mut data).await {
            Ok(()) => self.send_game_packet(data.into()).await,
            Err(err) => error!("Failed to serialize Bedrock packet: {err}"),
        }
    }

    pub async fn enqueue_client_packet<P: BClientPacket>(&self, packet: &P) {
        let mut data = Vec::new();
        match self.write_game_packet(packet, &mut data).await {
            Ok(()) => self.enqueue_packet(data.into()).await,
            Err(err) => error!("Failed to serialize Bedrock packet: {err}"),
        }
    }

    pub fn try_enqueue_client_packet<P: BClientPacket>(&self, packet: &P) {
        match self.serialize_packet(packet) {
            Ok(data) => self.try_enqueue_packet(data),
            Err(err) => error!("Failed to serialize Bedrock packet: {err}"),
        }
    }

    pub async fn send_game_packet(&self, packet_data: Bytes) {
        if self.is_closed() {
            return;
        }

        let packet_len = packet_data.len();
        let prev_bytes = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
        let new_bytes = prev_bytes.saturating_add(packet_len);

        if new_bytes > MAX_PENDING_BYTES {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            if !self.is_closed() {
                warn!(
                    "Bedrock client {} outbound packet buffer overflow ({} bytes > {} bytes). Closing connection.",
                    self.address, new_bytes, MAX_PENDING_BYTES
                );
                self.close_token.cancel();
            }
            return;
        }

        let (tx, rx) = oneshot::channel();
        if let Err(err) = self
            .outgoing_packet_priority_send
            .try_send(OutgoingPacket::priority(packet_data, tx))
        {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            if !self.is_closed() {
                error!("Failed to add priority packet to the outgoing packet queue: {err}");
            }
        } else {
            let _ = rx.await;
        }
    }

    pub async fn close(&self) {
        if self.close_token.is_cancelled() {
            return;
        }
        self.close_token.cancel();
        self.session.close().await;
    }

    pub async fn await_tasks(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    pub fn is_closed(&self) -> bool {
        self.close_token.is_cancelled() || self.session.is_closed()
    }

    pub fn enqueue_spawn_packet(&self, entity: &dyn crate::entity::EntityBase) {
        entity.send_bedrock_spawn_packet(self);
    }

    async fn process_batch(
        self: &Arc<Self>,
        server: &Arc<Server>,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let decompressed_payload = self
            .get_packet_payload(payload)
            .await
            .ok_or_else(|| Error::other("Failed to decompress game packet batch"))?;
        let mut cursor = Cursor::new(decompressed_payload);

        while (cursor.position() as usize) < cursor.get_ref().len() {
            let game_packet = BedrockBatchDecoder::new()
                .get_game_packet(&mut cursor)
                .map_err(|e| Error::other(e.to_string()))?;

            if !self.packet_limiter.check_packet() {
                warn!(
                    "Bedrock client {} exceeded packet rate limit (rate: {}/s)",
                    self.address,
                    self.packet_limiter.max_rate()
                );
                self.kick(
                    DisconnectReason::Kicked,
                    server
                        .advanced_config
                        .networking
                        .bedrock
                        .packet_limiter
                        .kick_message
                        .clone(),
                )
                .await;
                return Err(Error::other("Packet rate limit exceeded"));
            }

            self.handle_game_packet(game_packet).await?;
        }

        Ok(())
    }

    async fn handle_game_packet(&self, packet: RawPacket) -> Result<(), Error> {
        if let Err(err) = self.incoming_game_packet_send.send(packet).await {
            debug!("Failed to send game packet to session task: {err}");
        }
        Ok(())
    }

    pub async fn handle_login_sequence(
        self: &Arc<Self>,
        server: &Arc<Server>,
    ) -> PacketHandlerResult {
        while let Some(packet) = self.get_packet().await {
            let payload = &mut Cursor::new(&packet.payload);
            match packet.id {
                SRequestNetworkSettings::PACKET_ID => {
                    let packet = match SRequestNetworkSettings::read(payload) {
                        Ok(p) => p,
                        Err(err) => {
                            error!("Failed to read SRequestNetworkSettings: {err}");
                            continue;
                        }
                    };
                    if !self.handle_request_network_settings(packet, server).await {
                        return PacketHandlerResult::Stop;
                    }
                }
                SLogin::PACKET_ID => {
                    let packet = match SLogin::read(payload) {
                        Ok(p) => p,
                        Err(err) => {
                            error!("Failed to read SLogin: {err}");
                            self.kick(DisconnectReason::BadPacket, err.to_string())
                                .await;
                            return PacketHandlerResult::Stop;
                        }
                    };
                    match self.handle_login(packet, server).await {
                        Ok(result) => return result,
                        Err(err) => {
                            self.kick(DisconnectReason::Unknown, err.to_string()).await;
                            return PacketHandlerResult::Stop;
                        }
                    }
                }
                _ => {
                    debug!(
                        "Received unexpected game packet {} during login sequence",
                        packet.id
                    );
                }
            }
        }
        PacketHandlerResult::Stop
    }

    pub fn clear_player(&self) {
        self.player.store(Arc::new(None));
    }

    pub(crate) fn request_cluster_lobby(&self, forced: bool) -> bool {
        if self.lobby_requests.push(forced).is_err() {
            return false;
        }
        self.lobby_notify.notify_one();
        true
    }

    pub async fn progress_player_packets(self: &Arc<Self>, player: &Arc<Player>) -> Option<bool> {
        loop {
            tokio::select! {
                _ = self.lobby_notify.notified() => {
                    if let Some(forced) = self.lobby_requests.pop() { return Some(forced); }
                }
                packet = self.get_packet() => {
                    let packet = packet?;
                    if player.inbound_packets.push(packet).is_err() {
                        self.kick(DisconnectReason::BadPacket, "Client packet queue is full".to_string()).await;
                        return None;
                    }
                }
            }
        }
    }

    pub(crate) async fn progress_lobby_packets(
        self: &Arc<Self>,
        session: &mut crate::server::cluster_lobby::LobbySession,
        server: &Arc<Server>,
    ) -> Option<crate::server::cluster_lobby::LobbyPromotion> {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.is_closed() { return None; }
                    if let Some(promotion) = session.tick(server) { return Some(promotion); }
                }
                packet = self.get_packet() => session.handle_packet(&packet?, server),
            }
        }
    }

    pub(crate) async fn progress_lobby_during_join<F: std::future::Future>(
        self: &Arc<Self>,
        session: &mut crate::server::cluster_lobby::LobbySession,
        server: &Arc<Server>,
        join: F,
    ) -> Option<F::Output> {
        tokio::pin!(join);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            session.poll_virtual_events(server);
            if self.is_closed() {
                return None;
            }
            tokio::select! {
                result = &mut join => return Some(result),
                _ = interval.tick() => {},
                packet = self.get_packet() => session.handle_packet(&packet?, server),
            }
        }
    }

    pub(crate) async fn progress_lobby_until_idle(
        self: &Arc<Self>,
        session: &mut crate::server::cluster_lobby::LobbySession,
        server: &Arc<Server>,
    ) -> bool {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            session.poll_virtual_events(server);
            if self.is_closed() {
                return false;
            }
            if session.session_work_complete() {
                return true;
            }
            tokio::select! {
                _ = interval.tick() => {},
                packet = self.get_packet() => {
                    let Some(packet) = packet else { return false; };
                    session.handle_packet(&packet, server);
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn handle_play_packet(
        self: &Arc<Self>,
        player: &Arc<Player>,
        server: &Arc<Server>,
        packet: &RawPacket,
    ) -> Result<(), Error> {
        let payload = &packet.payload[..];
        let reader = &mut &payload[..];
        match packet.id {
            SClientCacheStatus::PACKET_ID => {
                let packet = SClientCacheStatus::read(reader)?;
                self.client_cache_supported
                    .store(packet.is_cache_supported, Ordering::Relaxed);
            }
            SClientCacheBlobStatus::PACKET_ID => {
                let packet = SClientCacheBlobStatus::read(reader)?;
                if !server.advanced_config.cluster.enabled {
                    self.handle_client_cache_blob_status(packet);
                }
            }
            SResourcePackClientResponse::PACKET_ID => {
                let packet = SResourcePackClientResponse::read(reader)?;
                let client = self.clone();
                let server_c = server.clone();
                server.spawn_task(async move {
                    client.handle_resource_pack_response(packet, &server_c).await;
                });
            }
            SPlayerAuthInput::PACKET_ID => {
                let packet = SPlayerAuthInput::read(reader)?;
                self.handle_player_auth_input(player, packet, server);
            }
            SRequestChunkRadius::PACKET_ID => {
                let packet = SRequestChunkRadius::read(reader)?;
                self.handle_request_chunk_radius(player, &packet);
            }
            SInventoryTransaction::PACKET_ID => {
                let packet = SInventoryTransaction::read(reader)?;
                self.handle_inventory_action(player, packet);
            }
            pumpkin_protocol::bedrock::server::item_stack_request::SItemStackRequest::PACKET_ID => {
                let packet = pumpkin_protocol::bedrock::server::item_stack_request::SItemStackRequest::read(reader)?;
                self.handle_item_stack_request(player, packet);
            }
            SInteract::PACKET_ID => {
                let packet = SInteract::read(reader)?;
                self.handle_interaction(&packet);
            }
            SContainerClose::PACKET_ID => {
                let packet = SContainerClose::read(reader)?;
                self.handle_container_close(player, &packet);
            }
            SText::PACKET_ID => {
                let text = SText::read(reader)?;
                if server.advanced_config.cluster.enabled {
                    player.update_last_action_time();
                    if crate::server::cluster_session_ingress::stage(
                        player,
                        crate::server::cluster_session_ingress::StagedSessionInput::Bedrock {
                            body: text.message.into_owned(),
                            xuid: text.xuid.into_owned(),
                            filtered: text.filtered_message.map(std::borrow::Cow::into_owned),
                            received_at: std::time::Instant::now(),
                        },
                    )
                    .is_err()
                    {
                        player.send_system_message(&pumpkin_util::text::TextComponent::text("Cluster chat queue is full"));
                    }
                    return Ok(());
                }
                let client = self.clone();
                let player_c = player.clone();
                let server_c = server.clone();
                player.spawn_task(async move {
                    client.handle_chat_message(&server_c, &player_c, text).await;
                });
            }
            SCommandRequest::PACKET_ID => {
                let req = SCommandRequest::read(reader)?;
                if server.advanced_config.cluster.enabled {
                    player.update_last_action_time();
                    let command = req.command.strip_prefix('/').unwrap_or(&req.command);
                    if crate::server::cluster_session_ingress::stage(
                        player,
                        crate::server::cluster_session_ingress::StagedSessionInput::Command {
                            command: command.to_string(),
                            received_at: std::time::Instant::now(),
                        },
                    )
                    .is_err()
                    {
                        player.send_system_message(&pumpkin_util::text::TextComponent::text("Cluster command queue is full"));
                    }
                    return Ok(());
                }
                let client = self.clone();
                let player_c = player.clone();
                let server_c = server.clone();
                player.spawn_task(async move {
                    client.handle_chat_command(&player_c, &server_c, req).await;
                });
            }
            SSetLocalPlayerAsInitialized::PACKET_ID => {
                self.handle_set_local_player_as_initialized(
                    player,
                    &SSetLocalPlayerAsInitialized::read(reader)?,
                );
            }
            SSetPlayerInventoryOptions::PACKET_ID => {
                let _ = SSetPlayerInventoryOptions::read(reader)?;
                // Ignore for now
            }
            SPlayerAction::PACKET_ID => {
                let packet = SPlayerAction::read(reader)?;
                self.handle_player_action(player, server, packet);
            }
            SRespawn::PACKET_ID => {
                let packet = SRespawn::read(reader)?;
                self.handle_respawn(player, &packet);
            }
            SAnimate::PACKET_ID => {
                self.handle_animate(player, &SAnimate::read(reader)?);
            }
            SActorEvent::PACKET_ID => {
                self.handle_actor_event(player, &SActorEvent::read(reader)?);
            }
            SEmote::PACKET_ID => {
                self.handle_emote(player, SEmote::read_slice(reader)?);
            }
            SEmoteList::PACKET_ID => {
                self.handle_emote_list(player, &SEmoteList::read(reader)?);
            }
            pumpkin_protocol::bedrock::server::modal_form_response::SModalFormResponse::PACKET_ID => {
                let form_resp = pumpkin_protocol::bedrock::server::modal_form_response::SModalFormResponse::read(
                    reader,
                )?;
                self.handle_modal_form_response(player, server, form_resp);
            }
            SLoadingScreen::PACKET_ID => {
                // Ignore for now
            }
            SBlockPickRequest::PACKET_ID => {
                let packet = SBlockPickRequest::read(reader)?;
                self.handle_block_pick_request(player, &packet);
            }
            SRequestAbility::PACKET_ID => {
                self.handle_request_ability(player, &SRequestAbility::read(reader)?);
            }
            SMobEquipment::PACKET_ID => {
                let packet = SMobEquipment::read(reader)?;
                self.handle_mob_equipment(server, player, &packet);
            }
            SPacketViolationWarning::PACKET_ID => {
                let warning = SPacketViolationWarning::read(reader)?;
                warn!(
                    violation_type = warning.violation_type.0,
                    violation_severity = warning.violation_severity.0,
                    violation_packet_id = warning.violation_packet_id.0,
                    violation_context = %warning.violation_context,
                    "Bedrock client rejected a server packet"
                );
            }
            _ => {
                warn!("Bedrock: Received Unknown Game packet: {}", packet.id);
            }
        }
        Ok(())
    }

    pub fn handle_client_cache_blob_status(&self, packet: SClientCacheBlobStatus) {
        if packet.miss_hashes.is_empty() {
            return;
        }
        let missing_blobs = {
            let cache = self
                .blob_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut missing_blobs = Vec::with_capacity(packet.miss_hashes.len());
            for hash in packet.miss_hashes {
                if let Some(payload) = cache.get(&hash) {
                    missing_blobs.push(MissingBlobData {
                        blob_id: hash,
                        blob_data: payload.clone(),
                    });
                } else {
                    warn!("Client requested missing blob {hash:#x} not found in server cache");
                }
            }
            missing_blobs
        };
        if !missing_blobs.is_empty() {
            self.try_enqueue_client_packet(&CClientCacheMissResponse { missing_blobs });
        }
    }

    pub async fn await_close_interrupt(&self) {
        self.close_token.cancelled().await;
    }

    pub async fn get_packet_payload(&self, packet: Vec<u8>) -> Option<Vec<u8>> {
        let mut network_reader = self.network_reader.load().as_ref().clone();
        tokio::select! {
            () = self.await_close_interrupt() => {
                debug!("Canceling player packet processing");
                None
            },
            packet_result = network_reader.get_packet_payload(packet) => {
                match packet_result {
                    Ok(packet) => Some(packet),
                    Err(err) => {
                        if !matches!(err, PacketDecodeError::ConnectionClosed) {
                            debug!("Failed to decode packet from client: {err}");
                            let text = format!("Error while reading incoming packet {err}");
                            self.kick(DisconnectReason::BadPacket, text).await;
                        }
                        None
                    }
                }
            }
        }
    }

    pub fn spawn_task<F>(&self, task: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if self.close_token.is_cancelled() {
            None
        } else {
            let _guard = self.rt_handle.enter();
            Some(self.tasks.spawn(task))
        }
    }
}
