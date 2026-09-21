use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use pumpkin_cluster::codec::decode_pos;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::presence::PresenceLogin;
use pumpkin_cluster::protocol::{PosUpdate, StreamKind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_config::ClusterRole;
use pumpkin_data::dimension::Dimension;
use pumpkin_inventory::inventory::Inventory;
use pumpkin_nbt::Nbt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use crate::entity::EntityBase;
use crate::entity::player::Player;

const PLAYERDATA_MAGIC: [u8; 4] = [0x50, 0x44, 0x41, 0x54];
const PLAYERDATA_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedPlayerdata {
    pub accepted: bool,
    pub tick: TickStamp,
    pub revision: u16,
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub nbt: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CachedHandoffLocation {
    pub dimension: Dimension,
    pub pos: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
}

impl AcceptedPlayerdata {
    #[must_use]
    pub fn from_player(player: &Player, tick: TickStamp, gid: GlobalPlayerId) -> Self {
        let mut compound = pumpkin_nbt::NbtCompound::new();
        player.write_nbt(&mut compound);
        Self {
            accepted: true,
            tick,
            revision: NEXT_PLAYERDATA_REVISION.fetch_add(1, Ordering::Relaxed),
            gid,
            uuid: player.gameprofile.id.into_bytes(),
            nbt: Nbt::from(compound).write_unnamed().to_vec(),
        }
    }
}

struct PlayerdataOutbox {
    peers: Vec<ServerId>,
    outbound: mpsc::Sender<OutboundParcel>,
}

enum PlayerdataInput {
    Accepted(AcceptedPlayerdata),
    Presence(PresenceLogin),
    Flush(GlobalPlayerId),
}

static PLAYERDATA_OUTBOX: OnceLock<PlayerdataOutbox> = OnceLock::new();
static PLAYERDATA_INPUT: OnceLock<mpsc::Sender<PlayerdataInput>> = OnceLock::new();
static NEXT_PLAYERDATA_REVISION: AtomicU16 = AtomicU16::new(1);
static POSITION_DATAGRAM_DROPS: AtomicU64 = AtomicU64::new(0);
static PLAYERDATA_REPLICA: std::sync::LazyLock<ArcSwap<BTreeMap<[u8; 16], AcceptedPlayerdata>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeMap::new()));
static LATEST_POSITIONS: std::sync::LazyLock<ArcSwap<BTreeMap<GlobalPlayerId, PosUpdate>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeMap::new()));

fn tick_is_newer(candidate: TickStamp, current: TickStamp) -> bool {
    candidate.is_newer_than(current)
}

fn record_is_newer(candidate: &AcceptedPlayerdata, current: &AcceptedPlayerdata) -> bool {
    if candidate.tick != current.tick {
        return tick_is_newer(candidate.tick, current.tick);
    }
    let distance = candidate.revision.wrapping_sub(current.revision);
    distance != 0 && distance < (u16::MAX / 2)
}

fn encode(record: &AcceptedPlayerdata) -> Option<Vec<u8>> {
    if record.nbt.len() > PLAYERDATA_MAX_BYTES {
        return None;
    }
    let mut bytes = Vec::with_capacity(PLAYERDATA_MAGIC.len() + record.nbt.len());
    bytes.extend_from_slice(&PLAYERDATA_MAGIC);
    bytes.extend_from_slice(&postcard::to_allocvec(record).ok()?);
    Some(bytes)
}

fn decode(bytes: &[u8]) -> Option<AcceptedPlayerdata> {
    let body = bytes.strip_prefix(&PLAYERDATA_MAGIC)?;
    let record = postcard::from_bytes::<AcceptedPlayerdata>(body).ok()?;
    (record.nbt.len() <= PLAYERDATA_MAX_BYTES).then_some(record)
}

fn publish(record: &AcceptedPlayerdata) {
    let Some(outbox) = PLAYERDATA_OUTBOX.get() else {
        return;
    };
    let Some(bytes) = encode(record) else {
        warn!(uuid = %uuid::Uuid::from_bytes(record.uuid), "cluster playerdata record too large");
        return;
    };
    for peer in &outbox.peers {
        if outbox
            .outbound
            .try_send(OutboundParcel {
                peer: *peer,
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: bytes.clone(),
            })
            .is_err()
        {
            warn!(peer = peer.0, "cluster playerdata delivery queue full");
        }
    }
}

fn remember(record: AcceptedPlayerdata) -> bool {
    let current = PLAYERDATA_REPLICA.load();
    if let Some(existing) = current.get(&record.uuid) {
        if existing.accepted && !record.accepted {
            return false;
        }
        if existing.accepted == record.accepted && !record_is_newer(&record, existing) {
            return false;
        }
    }
    let mut next = (**current).clone();
    next.insert(record.uuid, record);
    PLAYERDATA_REPLICA.store(Arc::new(next));
    true
}

fn cached(uuid: &[u8; 16]) -> Option<AcceptedPlayerdata> {
    PLAYERDATA_REPLICA.load().get(uuid).cloned()
}

pub fn cached_handoff_location(uuid: uuid::Uuid) -> Option<CachedHandoffLocation> {
    let record = cached(&uuid.into_bytes())?;
    let mut cursor = Cursor::new(record.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let nbt = Nbt::read_unnamed(&mut reader).ok()?;
    let dimension = nbt
        .root_tag
        .get_string("Dimension")
        .and_then(Dimension::from_name)
        .cloned()?;
    let pos = nbt.root_tag.get_list("Pos")?;
    let rotation = nbt.root_tag.get_list("Rotation")?;
    Some(CachedHandoffLocation {
        dimension,
        pos: [
            pos.first()?.extract_double()?,
            pos.get(1)?.extract_double()?,
            pos.get(2)?.extract_double()?,
        ],
        yaw: rotation.first()?.extract_float()?,
        pitch: rotation.get(1)?.extract_float()?,
    })
}

fn merged_position(mut record: AcceptedPlayerdata) -> AcceptedPlayerdata {
    let Some(position) = LATEST_POSITIONS.load().get(&record.gid).copied() else {
        return record;
    };
    let mut cursor = Cursor::new(record.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let Ok(mut nbt) = Nbt::read_unnamed(&mut reader) else {
        return record;
    };
    nbt.root_tag.put_list(
        "Pos",
        position.pos.into_iter().map(Into::into).collect(),
    );
    nbt.root_tag.put_list(
        "Motion",
        position.vel.into_iter().map(Into::into).collect(),
    );
    nbt.root_tag.put_list(
        "Rotation",
        vec![position.yaw.into(), position.pitch.into()],
    );
    record.nbt = nbt.write_unnamed().to_vec();
    record
}

fn replace_cached(record: AcceptedPlayerdata) {
    let current = PLAYERDATA_REPLICA.load();
    let mut next = (**current).clone();
    next.insert(record.uuid, record);
    PLAYERDATA_REPLICA.store(Arc::new(next));
}

fn load_primary_record(server: &Server, login: PresenceLogin) -> Option<AcceptedPlayerdata> {
    let uuid = uuid::Uuid::from_bytes(login.uuid);
    let compound = server.player_data_storage.load_data(&uuid).ok().flatten()?;
    Some(AcceptedPlayerdata {
        accepted: false,
        tick: TickStamp::default(),
        revision: 0,
        gid: login.gid,
        uuid: login.uuid,
        nbt: Nbt::from(compound).write_unnamed().to_vec(),
    })
}

fn persist_primary_record(server: &Server, record: &AcceptedPlayerdata) {
    let mut cursor = Cursor::new(record.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let Ok(nbt) = Nbt::read_unnamed(&mut reader) else {
        warn!(uuid = %uuid::Uuid::from_bytes(record.uuid), "cluster playerdata NBT decode failed");
        return;
    };
    if let Err(error) = server
        .player_data_storage
        .save_data(&uuid::Uuid::from_bytes(record.uuid), nbt.root_tag)
    {
        warn!(uuid = %uuid::Uuid::from_bytes(record.uuid), %error, "cluster playerdata persistence failed");
    }
}

async fn playerdata_task(server: Arc<Server>, mut input: mpsc::Receiver<PlayerdataInput>) {
    while let Some(event) = input.recv().await {
        match event {
            PlayerdataInput::Accepted(record) => {
                let record = if matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
                    merged_position(record)
                } else {
                    record
                };
                if !remember(record.clone()) {
                    continue;
                }
                if matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
                    persist_primary_record(&server, &record);
                }
            }
            PlayerdataInput::Presence(login) => {
                if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
                    continue;
                }
                if let Some(record) = cached(&login.uuid) {
                    publish(&record);
                    continue;
                }
                let Some(record) = load_primary_record(&server, login) else {
                    continue;
                };
                if remember(record.clone()) {
                    publish(&record);
                }
            }
            PlayerdataInput::Flush(gid) => {
                let _ = flush_primary_playerdata(&server, gid);
            }
        }
    }
    debug!("cluster playerdata task stopped");
}

pub fn observe_infallible_position(
    server: &Server,
    sender: ServerId,
    update: &PosUpdate,
) -> bool {
    if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
        || update.gid.server != sender
        || super::cluster_presence::remote_presence_entry(update.gid)
            .is_none_or(|entry| entry.in_lobby)
    {
        return false;
    }
    let current = LATEST_POSITIONS.load();
    if current
        .get(&update.gid)
        .is_some_and(|existing| !update.seq.is_newer_than(existing.seq))
    {
        return false;
    }
    let mut next = (**current).clone();
    next.insert(update.gid, *update);
    LATEST_POSITIONS.store(Arc::new(next));
    true
}

fn flush_primary_playerdata(server: &Server, gid: GlobalPlayerId) -> bool {
    if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
        return false;
    }
    let Some(record) = PLAYERDATA_REPLICA
        .load()
        .values()
        .find(|record| record.gid == gid)
        .cloned()
    else {
        return false;
    };
    let record = merged_position(record);
    replace_cached(record.clone());
    persist_primary_record(server, &record);
    true
}

pub fn install_playerdata(
    server: &Arc<Server>,
    peers: Vec<ServerId>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = PLAYERDATA_OUTBOX.set(PlayerdataOutbox { peers, outbound });
    let (input_tx, input_rx) = mpsc::channel(1024);
    if PLAYERDATA_INPUT.set(input_tx).is_ok() {
        let task_server = Arc::clone(server);
        server.spawn_task(playerdata_task(task_server, input_rx));
    }
}

pub fn spawn_position_datagram_demux(
    server: &Arc<Server>,
    datagrams: mpsc::Receiver<(ServerId, Vec<u8>)>,
) {
    let (entity_tx, entity_rx) = mpsc::channel(4096);
    super::cluster_entity_apply::spawn_entity_datagram_apply(server, entity_rx);
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut datagrams = datagrams;
        while let Some((sender, bytes)) = datagrams.recv().await {
            match decode_pos(&bytes) {
                Ok(datagram) if datagram.is_consistent() && !datagram.is_empty() => {
                    for update in &datagram.updates {
                        observe_infallible_position(&task_server, sender, update);
                        super::cluster_movement_sample::observe_remote_pos(
                            &task_server,
                            sender,
                            update,
                        );
                    }
                }
                Ok(_) => {
                    let drops = POSITION_DATAGRAM_DROPS.fetch_add(1, Ordering::Relaxed) + 1;
                    if drops == 1 || drops % 1024 == 0 {
                        warn!(sender = sender.0, drops, "cluster position datagram rejected");
                    }
                }
                Err(_) => {
                    let _ = entity_tx.try_send((sender, bytes));
                }
            }
        }
    });
}

pub fn submit_playerdata_control(parcel: InboundParcel) -> bool {
    let Some(record) = decode(&parcel.bytes) else {
        return false;
    };
    let Some(input) = PLAYERDATA_INPUT.get() else {
        return false;
    };
    if input.try_send(PlayerdataInput::Accepted(record)).is_err() {
        warn!(peer = parcel.peer.0, "cluster playerdata input queue full");
    }
    true
}

async fn playerdata_control_task(mut control: mpsc::Receiver<InboundParcel>) {
    while let Some(parcel) = control.recv().await {
        submit_playerdata_control(parcel);
    }
    debug!("cluster playerdata control task stopped");
}

pub fn spawn_playerdata_control(server: &Arc<Server>, control: mpsc::Receiver<InboundParcel>) {
    server.spawn_task(playerdata_control_task(control));
}

pub fn note_presence(login: PresenceLogin) {
    let Some(input) = PLAYERDATA_INPUT.get() else {
        return;
    };
    if input.try_send(PlayerdataInput::Presence(login)).is_err() {
        warn!("cluster playerdata presence queue full");
    }
}

pub fn note_presence_logout(gid: GlobalPlayerId) {
    let Some(input) = PLAYERDATA_INPUT.get() else {
        return;
    };
    if input.try_send(PlayerdataInput::Flush(gid)).is_err() {
        warn!(server = gid.server.0, player = gid.player.0, "cluster playerdata flush queue full");
    }
}

pub fn stage_accepted_playerdata(server: &Server, gid: GlobalPlayerId, tick: TickStamp) -> bool {
    let Some(player) = server
        .get_all_players()
        .into_iter()
        .find(|player| player.cluster_gid() == Some(gid))
    else {
        return false;
    };
    stage_accepted_playerdata_for_player(&player, tick)
}

pub fn stage_accepted_playerdata_for_player(player: &Player, tick: TickStamp) -> bool {
    let Some(gid) = player.cluster_gid() else {
        return false;
    };
    let record = AcceptedPlayerdata::from_player(player, tick, gid);
    let Some(input) = PLAYERDATA_INPUT.get() else {
        return false;
    };
    if input.try_send(PlayerdataInput::Accepted(record.clone())).is_err() {
        warn!(uuid = %player.gameprofile.id, "cluster playerdata input queue full");
        return false;
    }
    publish(&record);
    true
}

pub fn apply_replicated_playerdata(player: &Player) -> bool {
    let uuid = player.gameprofile.id.into_bytes();
    let Some(record) = cached(&uuid) else {
        return false;
    };
    let mut cursor = Cursor::new(record.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let Ok(nbt) = Nbt::read_unnamed(&mut reader) else {
        warn!(uuid = %player.gameprofile.id, "cluster playerdata handoff NBT decode failed");
        return false;
    };
    player.read_nbt_non_mut(&nbt.root_tag);
    let _ = seed_replicated_inventory_from_player(player);
    true
}

pub fn seed_replicated_inventory_from_player(player: &Player) -> bool {
    let Some(gid) = player.cluster_gid() else {
        return false;
    };
    let mut slots = Vec::with_capacity(41);
    let mut equipment = Vec::new();
    for index in 0..41 {
        let stack = player.inventory.get_stack(index);
        if let Some(slot) = player.inventory.equipment_slots.get(&index) {
            equipment.push((slot.clone(), stack.clone()));
        }
        if stack.is_empty() {
            slots.push(pumpkin_cluster::invsee::InvseeSlot::new(
                index as u16,
                0,
                0,
                Vec::new(),
            ));
            continue;
        }
        let mut compound = pumpkin_nbt::NbtCompound::new();
        stack.write_item_stack(&mut compound);
        slots.push(pumpkin_cluster::invsee::InvseeSlot::new(
            index as u16,
            stack.item.id,
            stack.item_count,
            Nbt::from(compound).write_unnamed().to_vec(),
        ));
    }
    let selected = player.inventory.get_selected_slot();
    let held_item = slots
        .iter()
        .find(|slot| slot.index == u16::from(selected))
        .map(|slot| pumpkin_cluster::inventory::InventoryStack {
            item: slot.item_id,
            count: slot.count,
            nbt: slot.nbt.clone(),
        })
        .unwrap_or_else(pumpkin_cluster::inventory::InventoryStack::empty);
    let _ = player.cluster_seed_attack_held_slot(selected, held_item);
    player
        .living_entity
        .update_cluster_combat_equipment(&equipment);
    super::cluster_world_apply::seed_replicated_inventory(gid, selected, slots);
    true
}
