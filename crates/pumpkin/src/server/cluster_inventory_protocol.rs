use std::sync::Arc;
use std::sync::atomic::Ordering;

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::inventory::{
    INV_CURSOR, INV_MAIN, INV_OFFHAND, InvLoc, InventoryApplyError, InventoryGestureEffect,
    InventoryRemovalKind, InventoryResolution, InventorySlotRef, InventoryTarget,
};
use pumpkin_data::item_stack::ItemStack;
use pumpkin_protocol::bedrock::client::container_open::CContainerOpen;
use pumpkin_protocol::bedrock::client::item_stack_response::{
    CItemStackResponse, ItemStackResponseContainerInfo, ItemStackResponseInfo,
    ItemStackResponseSlotInfo,
};
use pumpkin_protocol::bedrock::server::container_close::SContainerClose;
use pumpkin_protocol::bedrock::server::item_stack_request::ItemStackRequestSlotInfo;
use pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::codec::var_long::VarLong;
use pumpkin_protocol::java::client::play::{
    CCloseContainer, CMerchantOffers, COpenScreen, CSetContainerContent, CSetContainerProperty,
    CSetContainerSlot, CSetCursorItem, MerchantOffer,
};
use pumpkin_util::math::position::BlockPos;

use crate::entity::EntityBase;

use super::{Server, cluster_invsee::InventoryPublication};

pub(crate) type InventoryScreenMap = std::collections::HashMap<
    pumpkin_cluster::inventory::InventoryScreenSession,
    pumpkin_cluster::inventory::InventoryScreenSnapshot,
>;

pub(crate) struct LocalInventoryMenuIntent {
    pub intent: pumpkin_cluster::inventory::InventoryMenuIntent,
    pub origin: Option<u64>,
    pub input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
}

pub(crate) type InventoryMenuRoute = Option<tokio::sync::mpsc::Sender<LocalInventoryMenuIntent>>;

pub(crate) fn attach_inventory_menu_owner(
    server: &Server,
) -> tokio::sync::mpsc::Receiver<LocalInventoryMenuIntent> {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    server
        .cluster_inventory_menu_route_tx
        .send_replace(Some(tx));
    rx
}

fn submit_inventory_menu(
    server: &Server,
    intent: pumpkin_cluster::inventory::InventoryMenuIntent,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_menu_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryMenuIntent {
            intent,
            origin,
            input_sequence,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

const BEDROCK_STACK_REQUEST_TAG: u64 = 0x4252_0000_0000_0000;
const JAVA_CLICK_TAG: u64 = 0x4a56_0000_0000_0000;
const JAVA_ITEM_ACTION_TAG: u64 = 0x4a49_0000_0000_0000;
const BEDROCK_ITEM_ACTION_TAG: u64 = 0x4249_0000_0000_0000;
const JAVA_HELD_TAG: u64 = 0x4a48_0000_0000_0000;
const BEDROCK_HELD_TAG: u64 = 0x4248_0000_0000_0000;
const ORIGIN_TAG_MASK: u64 = 0xffff_0000_0000_0000;

pub(crate) enum StagedInventoryInput {
    Close {
        session: u64,
        token: u64,
    },
    BedrockStackRequests(
        Vec<pumpkin_protocol::bedrock::server::item_stack_request::ItemStackRequest>,
    ),
    BedrockTransaction(pumpkin_protocol::bedrock::server::SInventoryTransaction),
    BedrockAuthInteraction(
        pumpkin_protocol::bedrock::server::player_auth_input::PlayerInventoryAction,
    ),
    BedrockBlockActions(
        Vec<pumpkin_protocol::bedrock::server::player_auth_input::PlayerBlockAction>,
    ),
    JavaClick(pumpkin_protocol::java::server::play::SClickSlot),
    JavaCreative(pumpkin_protocol::java::server::play::SSetCreativeSlot),
    SelectHotbar {
        slot: u8,
        origin: u64,
    },
    PickBlock {
        position: pumpkin_util::math::position::BlockPos,
        target_hotbar: Option<u8>,
        bedrock: bool,
    },
    PickEntity(i32),
    DropHeld {
        whole_stack: bool,
    },
    SwapHands,
    EditBook {
        pages: Vec<String>,
        title: Option<String>,
    },
    JavaPlaceRecipe {
        display_id: i32,
        use_max: bool,
    },
    JavaMenu(JavaMenuInput),
    ItemAction {
        hand: u8,
        kind: StagedItemActionKind,
        origin: Option<u64>,
    },
}

pub(crate) struct BookPreflight {
    player: Arc<crate::entity::player::Player>,
    slot: u16,
    session: u64,
    result: tokio::sync::oneshot::Receiver<BookPolicyResult>,
}

enum BookPolicyResult {
    Approved {
        pages: Vec<String>,
        title: Option<String>,
    },
    Cancelled,
    TimedOut,
    CreativeApproved {
        slot: i16,
        stack: ItemStack,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct StagedClose {
    pub session: u64,
    pub token: u64,
    pub any: bool,
}

pub(crate) enum JavaMenuInput {
    Button {
        sync_id: i32,
        button: i32,
    },
    Rename(String),
    SelectTrade(i32),
    SetBeacon {
        primary: Option<i32>,
        secondary: Option<i32>,
    },
}

pub(crate) enum StagedItemActionKind {
    Use {
        claimed_yaw: f32,
        claimed_pitch: f32,
    },
    UseOnBlock {
        block: pumpkin_util::math::position::BlockPos,
        hit: pumpkin_cluster::item_action::ClaimedHit,
    },
    UseOnActor {
        target: pumpkin_cluster::actor::ActorId,
        target_chunk: pumpkin_cluster::protocol::ChunkKey,
        hit: Option<[f32; 3]>,
        interaction: pumpkin_cluster::item_action::ActorItemInteraction,
    },
    Release,
}

#[derive(Default)]
struct StagedItemClientClaim {
    changes: Vec<pumpkin_cluster::item_action::ClaimedInventoryChange>,
    item_in_hand: Option<ItemStack>,
    hotbar_slot: Option<u8>,
    clicked_block_state: Option<u32>,
}

pub(crate) fn stage_inventory_input(
    player: &crate::entity::player::Player,
    input: StagedInventoryInput,
) -> bool {
    if !player.cluster_inventory_pending_close.is_empty()
        || !player.cluster_inventory_departure_close.is_empty()
    {
        resync_inventory_screen(player);
        return false;
    }
    if player.cluster_inventory_inputs.push(input).is_ok() {
        true
    } else {
        player
            .cluster_inventory_dropped
            .fetch_add(1, Ordering::Relaxed);
        false
    }
}

pub(crate) fn stage_bedrock_stack_requests(
    player: &crate::entity::player::Player,
    requests: Vec<pumpkin_protocol::bedrock::server::item_stack_request::ItemStackRequest>,
) -> bool {
    let request_ids: Vec<_> = requests.iter().map(|request| request.request_id).collect();
    if stage_inventory_input(player, StagedInventoryInput::BedrockStackRequests(requests)) {
        return true;
    }
    if let Some(client) = player.client.bedrock() {
        client.try_enqueue_client_packet(&CItemStackResponse {
            responses: request_ids
                .into_iter()
                .map(|client_request_id| ItemStackResponseInfo {
                    result: 1,
                    client_request_id,
                    containers: Vec::new(),
                })
                .collect(),
        });
    }
    resync_inventory_screen(player);
    false
}

pub(crate) fn stage_bedrock_transaction(
    player: &crate::entity::player::Player,
    packet: pumpkin_protocol::bedrock::server::SInventoryTransaction,
) -> bool {
    let request_id = packet.legacy_request_id;
    let rejected_block = match &packet.transaction_data {
        pumpkin_protocol::bedrock::server::inventory_transaction::TransactionData::UseItem(
            data,
        ) if data.action_type.0 == 0 => Some((data.block_position, data.block_face)),
        _ => None,
    };
    if stage_inventory_input(player, StagedInventoryInput::BedrockTransaction(packet)) {
        return true;
    }
    if let Some((block, face)) = rejected_block {
        send_authoritative_item_block(player, block, Some(face));
    }
    if request_id.0 != 0
        && let Some(client) = player.client.bedrock()
    {
        client.try_enqueue_client_packet(&CItemStackResponse {
            responses: vec![ItemStackResponseInfo {
                result: 1,
                client_request_id: request_id,
                containers: Vec::new(),
            }],
        });
    }
    resync_inventory_screen(player);
    false
}

pub(crate) fn drain_inventory_inputs(
    server: &Arc<Server>,
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    sequence: &mut u16,
    effective_frame: Option<(pumpkin_cluster::controller::ControllerFrame, f64)>,
    preflight: &mut Option<BookPreflight>,
) {
    if let Some(pending) = preflight.as_mut() {
        if !Arc::ptr_eq(&pending.player, player) {
            *preflight = None;
        } else if pending.session
            != player
                .cluster_current_inventory_session
                .load(Ordering::Acquire)
        {
            *preflight = None;
            resync_inventory_screen(player);
        } else {
            match pending.result.try_recv() {
                Ok(BookPolicyResult::Approved { pages, title }) => {
                    let slot = pending.slot;
                    *preflight = None;
                    if process_book_edit(
                        player,
                        tick,
                        pages,
                        title,
                        next_input_sequence(sequence),
                        Some(slot),
                    )
                    .is_err()
                    {
                        resync_inventory_screen(player);
                    }
                }
                Ok(BookPolicyResult::CreativeApproved { slot, stack }) => {
                    *preflight = None;
                    if process_java_creative_stack(
                        player,
                        slot,
                        stack,
                        tick,
                        next_input_sequence(sequence),
                    )
                    .is_err()
                    {
                        resync_inventory_screen(player);
                    }
                }
                Ok(BookPolicyResult::Cancelled) => {
                    *preflight = None;
                    resync_inventory_screen(player);
                }
                Ok(BookPolicyResult::TimedOut)
                | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    *preflight = None;
                    player.client.try_kick(
                        crate::net::DisconnectReason::Unknown,
                        &pumpkin_util::text::TextComponent::text("Inventory policy timed out"),
                    );
                    return;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => return,
            }
        }
    }
    if player.cluster_inventory_dropped.load(Ordering::Relaxed) != 0 {
        let previous = player
            .cluster_inventory_last_overload_warn
            .load(Ordering::Relaxed);
        if (previous == 0 || tick.0.wrapping_sub((previous - 1) as u16) >= 6_000)
            && player
                .cluster_inventory_last_overload_warn
                .compare_exchange(
                    previous,
                    u32::from(tick.0) + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            let dropped = player.cluster_inventory_dropped.swap(0, Ordering::Relaxed);
            tracing::warn!(
                actor = player.cluster_gid().map(GlobalPlayerId::pack_u32),
                dropped,
                "cluster inventory input queue saturated"
            );
        }
    }
    while let Some(input) = player.cluster_inventory_inputs.pop() {
        match input {
            StagedInventoryInput::Close { session, token } => {
                let input_sequence = pumpkin_cluster::identity::PlayerSeq(*sequence);
                if submit_screen_close(player, tick, session, token, input_sequence).is_err() {
                    let _ = player.cluster_inventory_pending_close.push(StagedClose {
                        session,
                        token,
                        any: false,
                    });
                } else {
                    *sequence = sequence.wrapping_add(1);
                }
            }
            StagedInventoryInput::BedrockStackRequests(requests) => {
                crate::net::bedrock::play::item_stack_request::process_cluster_stack_requests(
                    player, requests, tick, sequence,
                );
            }
            StagedInventoryInput::BedrockTransaction(packet) => {
                let origin = bedrock_item_action_origin(packet.legacy_request_id.0);
                let rejected_block = match &packet.transaction_data {
                    pumpkin_protocol::bedrock::server::inventory_transaction::TransactionData::UseItem(data)
                        if data.action_type.0 == 0 => Some((data.block_position, data.block_face)),
                    _ => None,
                };
                if process_bedrock_transaction(
                    player,
                    tick,
                    packet,
                    next_input_sequence(sequence),
                    effective_frame,
                )
                .is_err()
                {
                    if let Some((block, face)) = rejected_block {
                        send_authoritative_item_block(player, block, Some(face));
                    }
                    resync_inventory_screen(player);
                    send_item_action_response(player, origin, false);
                }
            }
            StagedInventoryInput::BedrockAuthInteraction(interaction) => {
                let origin = bedrock_item_action_origin(interaction.legacy_request_id.0);
                let rejected_block = (interaction.transaction.action_type.0 == 0).then_some((
                    interaction.transaction.block_position,
                    interaction.transaction.block_face,
                ));
                if process_bedrock_auth_interaction(
                    player,
                    tick,
                    interaction,
                    next_input_sequence(sequence),
                    effective_frame,
                )
                .is_err()
                {
                    if let Some((block, face)) = rejected_block {
                        send_authoritative_item_block(player, block, Some(face));
                    }
                    resync_inventory_screen(player);
                    send_item_action_response(player, origin, false);
                }
            }
            StagedInventoryInput::BedrockBlockActions(actions) => {
                process_bedrock_block_actions(player, tick, sequence, actions);
            }
            StagedInventoryInput::JavaClick(packet) => {
                if process_java_click(player, packet, tick, next_input_sequence(sequence)).is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::JavaCreative(packet) => {
                if server
                    .plugin_manager
                    .has_handlers::<crate::plugin::api::events::inventory::inventory_creative::InventoryCreativeEvent>()
                {
                    match start_creative_preflight(server, player, packet) {
                        Ok(pending) => *preflight = Some(pending),
                        Err(_) => resync_inventory_screen(player),
                    }
                    return;
                }
                if process_java_creative(player, packet, tick, next_input_sequence(sequence))
                    .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::DropHeld { whole_stack } => {
                if process_held_inventory_action(
                    player,
                    tick,
                    Some(whole_stack),
                    next_input_sequence(sequence),
                )
                .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::SwapHands => {
                if process_held_inventory_action(player, tick, None, next_input_sequence(sequence))
                    .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::EditBook { pages, title } => {
                if server
                    .plugin_manager
                    .has_handlers::<crate::plugin::api::events::player::player_edit_book::PlayerEditBookEvent>()
                {
                    match start_book_preflight(server, player, pages, title) {
                        Ok(pending) => *preflight = Some(pending),
                        Err(_) => resync_inventory_screen(player),
                    }
                    return;
                }
                if process_book_edit(
                    player,
                    tick,
                    pages,
                    title,
                    next_input_sequence(sequence),
                    None,
                )
                .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::SelectHotbar { slot, origin } => {
                let Some(server) = player.world().server.upgrade() else {
                    continue;
                };
                let Some(actor) = player.cluster_gid() else {
                    continue;
                };
                let chunk = player_chunk(player);
                if super::cluster_invsee::submit_selected_slot_mutation_with_sequence(
                    &server,
                    actor,
                    tick,
                    slot,
                    vec![chunk],
                    Some(origin),
                    Some(next_input_sequence(sequence)),
                )
                .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::PickBlock {
                position,
                target_hotbar,
                bedrock,
            } => {
                if process_pick_block(
                    player,
                    tick,
                    position,
                    target_hotbar,
                    bedrock,
                    next_input_sequence(sequence),
                )
                .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::PickEntity(entity_id) => {
                if process_pick_entity(player, tick, entity_id, next_input_sequence(sequence))
                    .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::JavaPlaceRecipe {
                display_id,
                use_max,
            } => {
                if crate::net::java::play::place_recipe::process_cluster_place_recipe(
                    player,
                    tick,
                    display_id,
                    use_max,
                    next_input_sequence(sequence),
                )
                .is_err()
                {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::JavaMenu(input) => {
                if process_java_menu(player, tick, input, next_input_sequence(sequence)).is_err() {
                    resync_inventory_screen(player);
                }
            }
            StagedInventoryInput::ItemAction { hand, kind, origin } => {
                let rejected_block = match &kind {
                    StagedItemActionKind::UseOnBlock { block, hit } => Some((*block, hit.face)),
                    _ => None,
                };
                if submit_staged_item_action(
                    player,
                    tick,
                    hand,
                    kind,
                    origin,
                    Some(next_input_sequence(sequence)),
                    StagedItemClientClaim::default(),
                    effective_frame,
                )
                .is_err()
                {
                    if let Some((block, face)) = rejected_block {
                        send_authoritative_item_block(player, block, Some(face));
                    }
                    resync_inventory_screen(player);
                    send_item_action_response(player, origin, false);
                }
            }
        }
    }
    drain_pending_screen_close(player, tick, sequence);
    drain_departure_screen_close(player, tick, sequence);
}

pub(crate) fn next_input_sequence(sequence: &mut u16) -> pumpkin_cluster::identity::PlayerSeq {
    let next = pumpkin_cluster::identity::PlayerSeq(*sequence);
    *sequence = sequence.wrapping_add(1);
    next
}

pub(crate) fn stage_bedrock_block_actions(
    player: &crate::entity::player::Player,
    actions: Vec<pumpkin_protocol::bedrock::server::player_auth_input::PlayerBlockAction>,
) -> bool {
    let blocks: Vec<_> = actions.iter().map(|action| action.block_pos).collect();
    let staged = stage_inventory_input(player, StagedInventoryInput::BedrockBlockActions(actions));
    if !staged {
        for block in blocks {
            send_authoritative_item_block(player, block, None);
        }
    }
    staged
}

fn process_bedrock_block_actions(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    sequence: &mut u16,
    actions: Vec<pumpkin_protocol::bedrock::server::player_auth_input::PlayerBlockAction>,
) {
    use pumpkin_cluster::block_action::{BlockActionIntent, BlockActionKind};
    use pumpkin_protocol::bedrock::server::player_action::PlayerActionType;

    let Some(server) = player.world().server.upgrade() else {
        return;
    };
    let Some(actor) = player.cluster_gid() else {
        return;
    };
    let actor_chunk = player_chunk(player);
    let world = player.world();
    let world_id = super::cluster_world_delta::world_id(&world);
    for action in actions {
        let kind = match PlayerActionType::try_from(action.action.0) {
            Ok(PlayerActionType::StartDestroyBlock) => BlockActionKind::StartBreak,
            Ok(PlayerActionType::AbortDestroyBlock) => BlockActionKind::AbortBreak,
            Ok(PlayerActionType::StopDestroyBlock) => BlockActionKind::StopBreak,
            Ok(PlayerActionType::CrackBlock | PlayerActionType::ContinueDestroyBlock) => {
                BlockActionKind::ContinueBreak
            }
            Ok(PlayerActionType::PredictDestroyBlock) => BlockActionKind::PredictBreak,
            Ok(PlayerActionType::CreativeDestroyBlock) => BlockActionKind::CreativeBreak,
            _ => continue,
        };
        let position = pumpkin_cluster::protocol::BlockPos {
            x: action.block_pos.0.x,
            y: action.block_pos.0.y,
            z: action.block_pos.0.z,
        };
        let target_chunk =
            pumpkin_cluster::world_delta::chunk_of_block(world_id, position.x, position.z);
        let Ok(face) = u8::try_from(action.face.0) else {
            continue;
        };
        let intent = BlockActionIntent {
            actor: actor.into(),
            tick,
            actor_chunk,
            target_chunk,
            position,
            face,
            expected_block_state: None,
            kind,
            input_sequence: Some(next_input_sequence(sequence)),
            client_sequence: None,
        };
        if super::cluster_block_action::submit_block_action(&server, intent).is_err() {
            player
                .cluster_inventory_dropped
                .fetch_add(1, Ordering::Relaxed);
            let _ = publish_block_action_resolution(&server, actor, position, None, None, false);
        }
    }
}

pub(crate) fn stage_bedrock_auth_interaction(
    player: &crate::entity::player::Player,
    interaction: pumpkin_protocol::bedrock::server::player_auth_input::PlayerInventoryAction,
) -> bool {
    let origin = bedrock_item_action_origin(interaction.legacy_request_id.0);
    let rejected_block = (interaction.transaction.action_type.0 == 0).then_some((
        interaction.transaction.block_position,
        interaction.transaction.block_face,
    ));
    let staged = stage_inventory_input(
        player,
        StagedInventoryInput::BedrockAuthInteraction(interaction),
    );
    if !staged {
        if let Some((block, face)) = rejected_block {
            send_authoritative_item_block(player, block, Some(face));
        }
        resync_inventory_screen(player);
        send_item_action_response(player, origin, false);
    }
    staged
}

fn process_bedrock_auth_interaction(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    interaction: pumpkin_protocol::bedrock::server::player_auth_input::PlayerInventoryAction,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
    effective_frame: Option<(pumpkin_cluster::controller::ControllerFrame, f64)>,
) -> Result<(), InventoryApplyError> {
    let origin = bedrock_item_action_origin(interaction.legacy_request_id.0);
    let data = interaction.transaction;
    let claim = bedrock_item_client_claim(
        player,
        &interaction.actions,
        &data.item_in_hand,
        data.hot_bar_slot.0,
        (data.action_type.0 == 0).then_some(data.block_runtime_id.0),
    )?;
    match data.action_type.0 {
        0 => submit_staged_item_action(
            player,
            tick,
            0,
            StagedItemActionKind::UseOnBlock {
                block: data.block_position,
                hit: pumpkin_cluster::item_action::ClaimedHit {
                    position: [
                        data.click_position.x,
                        data.click_position.y,
                        data.click_position.z,
                    ],
                    face: data.block_face,
                },
            },
            origin,
            Some(input_sequence),
            claim,
            effective_frame,
        ),
        1 => submit_staged_item_action(
            player,
            tick,
            0,
            StagedItemActionKind::Use {
                claimed_yaw: player.rotation().0,
                claimed_pitch: player.rotation().1,
            },
            origin,
            Some(input_sequence),
            claim,
            effective_frame,
        ),
        _ => Err(InventoryApplyError::Rejected),
    }
}

fn bedrock_window_slot_state(
    player: &crate::entity::player::Player,
    window_id: i32,
    slot: u32,
) -> Result<(InventorySlotRef, pumpkin_cluster::inventory::InventoryCell), InventoryApplyError> {
    let screen = current_screen_snapshot(player)?;
    let screen_slot = match window_id {
        0 => match slot {
            0..=8 => usize::try_from(slot + 36).map_err(|_| InventoryApplyError::Rejected)?,
            9..=35 => usize::try_from(slot).map_err(|_| InventoryApplyError::Rejected)?,
            _ => return Err(InventoryApplyError::Rejected),
        },
        120 if slot < 4 => usize::try_from(slot + 5).map_err(|_| InventoryApplyError::Rejected)?,
        119 if slot == 0 => 45,
        119 | 120 => return Err(InventoryApplyError::Rejected),
        _ => usize::try_from(slot).map_err(|_| InventoryApplyError::Rejected)?,
    };
    let entry = screen
        .slots
        .iter()
        .find(|entry| usize::from(entry.screen_slot) == screen_slot)
        .ok_or(InventoryApplyError::Rejected)?;
    Ok((entry.slot, entry.cell.clone()))
}

fn decode_bedrock_item_changes(
    player: &crate::entity::player::Player,
    actions: &[pumpkin_protocol::bedrock::server::inventory_transaction::InventoryAction],
) -> Result<Vec<pumpkin_cluster::item_action::ClaimedInventoryChange>, InventoryApplyError> {
    use pumpkin_cluster::item_action::{ClaimedInventoryChange, ClaimedInventoryTarget};
    use pumpkin_protocol::bedrock::server::inventory_transaction::InventoryActionSource;

    let mut changes = Vec::with_capacity(actions.len());
    for action in actions {
        let target = match InventoryActionSource::from(action.source_type) {
            InventoryActionSource::Container => {
                let window = action.window_id.ok_or(InventoryApplyError::Rejected)?;
                let (slot, _) = bedrock_window_slot_state(player, window, action.inventory_slot)?;
                ClaimedInventoryTarget::Container(slot)
            }
            InventoryActionSource::World => ClaimedInventoryTarget::World,
            InventoryActionSource::Creative => ClaimedInventoryTarget::Creative,
            InventoryActionSource::Todo | InventoryActionSource::Unknown(_) => {
                return Err(InventoryApplyError::Rejected);
            }
        };
        let loc = match target {
            ClaimedInventoryTarget::Container(slot) => slot.loc,
            ClaimedInventoryTarget::World | ClaimedInventoryTarget::Creative => {
                InvLoc::new(INV_CURSOR, 0)
            }
        };
        let before_stack = crate::net::bedrock::play::descriptor_to_stack(&action.old_item);
        let after_stack = crate::net::bedrock::play::descriptor_to_stack(&action.new_item);
        changes.push(ClaimedInventoryChange {
            target,
            before: super::cluster_invsee::inventory_cell_from_item_stack(loc, &before_stack)?,
            after: super::cluster_invsee::inventory_cell_from_item_stack(loc, &after_stack)?,
        });
    }
    Ok(changes)
}

fn bedrock_item_client_claim(
    player: &crate::entity::player::Player,
    actions: &[pumpkin_protocol::bedrock::server::inventory_transaction::InventoryAction],
    item_in_hand: &pumpkin_protocol::bedrock::network_item::NetworkItemDescriptor,
    hotbar_slot: i32,
    block: Option<u32>,
) -> Result<StagedItemClientClaim, InventoryApplyError> {
    Ok(StagedItemClientClaim {
        changes: decode_bedrock_item_changes(player, actions)?,
        item_in_hand: Some(crate::net::bedrock::play::descriptor_to_stack(item_in_hand)),
        hotbar_slot: Some(u8::try_from(hotbar_slot).map_err(|_| InventoryApplyError::Rejected)?),
        clicked_block_state: block,
    })
}

pub(crate) fn actor_for_entity_id(
    player: &crate::entity::player::Player,
    entity_id: i32,
) -> Option<(
    pumpkin_cluster::actor::ActorId,
    pumpkin_cluster::protocol::ChunkKey,
)> {
    let world = player.world();
    if let Some(target) = world.get_player_by_id(entity_id)
        && let Some(gid) = target.cluster_gid()
    {
        return Some((
            pumpkin_cluster::actor::ActorId::Player(gid),
            player_chunk(&target),
        ));
    }
    let server = world.server.upgrade()?;
    let actor = super::cluster_entity_emit::actor_for_entity_id(&server, &world, entity_id)?;
    let pumpkin_cluster::actor::ActorId::Entity(_) = actor else {
        return None;
    };
    let entity = world.get_entity_by_id(entity_id)?;
    let position = entity.get_entity().pos.load();
    Some((
        actor,
        pumpkin_cluster::world_delta::chunk_of_block(
            super::cluster_world_delta::world_id(&world),
            position.x.floor() as i32,
            position.z.floor() as i32,
        ),
    ))
}

fn process_bedrock_transaction(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    packet: pumpkin_protocol::bedrock::server::SInventoryTransaction,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
    effective_frame: Option<(pumpkin_cluster::controller::ControllerFrame, f64)>,
) -> Result<(), InventoryApplyError> {
    use pumpkin_protocol::bedrock::server::inventory_transaction::{
        InventoryActionSource, TransactionData,
    };

    let origin = bedrock_item_action_origin(packet.legacy_request_id.0);
    match packet.transaction_data {
        TransactionData::Normal(_) => {
            let mut gestures = Vec::new();
            let mut conservation = std::collections::HashMap::<(u16, Vec<u8>), i32>::new();
            for action in packet.actions {
                if InventoryActionSource::from(action.source_type)
                    != InventoryActionSource::Container
                {
                    return Err(InventoryApplyError::Rejected);
                }
                let window_id = action.window_id.ok_or(InventoryApplyError::Rejected)?;
                let (destination, actual) =
                    bedrock_window_slot_state(player, window_id, action.inventory_slot)?;
                let before_stack = crate::net::bedrock::play::descriptor_to_stack(&action.old_item);
                let after_stack = crate::net::bedrock::play::descriptor_to_stack(&action.new_item);
                let before = super::cluster_invsee::inventory_cell_from_item_stack(
                    destination.loc,
                    &before_stack,
                )?;
                if before != actual {
                    return Err(InventoryApplyError::Rejected);
                }
                let after = super::cluster_invsee::inventory_cell_from_item_stack(
                    destination.loc,
                    &after_stack,
                )?;
                if !before.is_empty() {
                    *conservation
                        .entry((before.item, before.nbt.clone()))
                        .or_default() += i32::from(before.count);
                }
                if !after.is_empty() {
                    *conservation
                        .entry((after.item, after.nbt.clone()))
                        .or_default() -= i32::from(after.count);
                }
                gestures.push(pumpkin_cluster::inventory::InventoryGesture::ExactSet {
                    destination,
                    before,
                    after,
                });
            }
            if conservation.values().any(|count| *count != 0) {
                return Err(InventoryApplyError::Rejected);
            }
            submit_gestures(player, tick, gestures, None, Some(input_sequence))
        }
        TransactionData::Mismatch(_) => Err(InventoryApplyError::Rejected),
        TransactionData::UseItem(data) => {
            let claim = bedrock_item_client_claim(
                player,
                &packet.actions,
                &data.item_in_hand,
                data.hot_bar_slot.0,
                (data.action_type.0 == 0).then_some(data.block_runtime_id.0),
            )?;
            match data.action_type.0 {
                0 => submit_staged_item_action(
                    player,
                    tick,
                    0,
                    StagedItemActionKind::UseOnBlock {
                        block: data.block_position,
                        hit: pumpkin_cluster::item_action::ClaimedHit {
                            position: [
                                data.click_position.x,
                                data.click_position.y,
                                data.click_position.z,
                            ],
                            face: data.block_face,
                        },
                    },
                    origin,
                    Some(input_sequence),
                    claim,
                    effective_frame,
                ),
                1 => submit_staged_item_action(
                    player,
                    tick,
                    0,
                    StagedItemActionKind::Use {
                        claimed_yaw: player.rotation().0,
                        claimed_pitch: player.rotation().1,
                    },
                    origin,
                    Some(input_sequence),
                    claim,
                    effective_frame,
                ),
                _ => Err(InventoryApplyError::Rejected),
            }
        }
        TransactionData::UseItemOnEntity(data) => {
            let claim = bedrock_item_client_claim(
                player,
                &packet.actions,
                &data.item_in_hand,
                data.hot_bar_slot.0,
                None,
            )?;
            let entity_id = i32::try_from(data.target_entity_runtime_id.0)
                .map_err(|_| InventoryApplyError::Rejected)?;
            let (target, target_chunk) = actor_for_entity_id(player, entity_id)
                .ok_or(InventoryApplyError::TargetUnavailable)?;
            submit_staged_item_action(
                player,
                tick,
                0,
                StagedItemActionKind::UseOnActor {
                    target,
                    target_chunk,
                    hit: Some([
                        data.click_position.x,
                        data.click_position.y,
                        data.click_position.z,
                    ]),
                    interaction: match data.action_type.0 {
                        0 | 2 => pumpkin_cluster::item_action::ActorItemInteraction::Interact,
                        1 => pumpkin_cluster::item_action::ActorItemInteraction::Attack,
                        _ => return Err(InventoryApplyError::Rejected),
                    },
                },
                origin,
                Some(input_sequence),
                claim,
                effective_frame,
            )
        }
        TransactionData::ReleaseItem(data) => {
            let claim = bedrock_item_client_claim(
                player,
                &packet.actions,
                &data.item_in_hand,
                data.hot_bar_slot.0,
                None,
            )?;
            submit_staged_item_action(
                player,
                tick,
                0,
                StagedItemActionKind::Release,
                origin,
                Some(input_sequence),
                claim,
                effective_frame,
            )
        }
    }
}

pub(crate) fn stage_item_action(
    player: &crate::entity::player::Player,
    hand: u8,
    kind: StagedItemActionKind,
    origin: Option<u64>,
) -> bool {
    let rejected_block = match &kind {
        StagedItemActionKind::UseOnBlock { block, hit } => Some((*block, hit.face)),
        _ => None,
    };
    let staged = stage_inventory_input(
        player,
        StagedInventoryInput::ItemAction { hand, kind, origin },
    );
    if !staged {
        if let Some((block, face)) = rejected_block {
            send_authoritative_item_block(player, block, Some(face));
        }
        resync_inventory_screen(player);
        send_item_action_response(player, origin, false);
    }
    staged
}

fn submit_staged_item_action(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    hand: u8,
    kind: StagedItemActionKind,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
    client_claim: StagedItemClientClaim,
    effective_frame: Option<(pumpkin_cluster::controller::ControllerFrame, f64)>,
) -> Result<(), InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let gid = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let target = InventoryTarget::Player(gid);
    let snapshot = super::cluster_invsee::inventory_snapshot(&server, target)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let loc = match hand {
        0 => {
            let selected = snapshot
                .cells
                .iter()
                .find(|cell| {
                    cell.loc.inv == pumpkin_cluster::inventory::INV_SELECTED && cell.loc.slot == 0
                })
                .and_then(|cell| cell.item.checked_sub(1))
                .and_then(|slot| u16::try_from(slot).ok())
                .filter(|slot| *slot < 9)
                .ok_or(InventoryApplyError::Rejected)?;
            InvLoc::new(INV_MAIN, selected)
        }
        1 => InvLoc::new(INV_OFFHAND, 0),
        _ => return Err(InventoryApplyError::Rejected),
    };
    let expected_item = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc == loc)
        .cloned()
        .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc));
    let item_in_hand = client_claim
        .item_in_hand
        .as_ref()
        .map(|stack| super::cluster_invsee::inventory_cell_from_item_stack(loc, stack))
        .transpose()?;
    let position = player.position();
    let actor_chunk = pumpkin_cluster::world_delta::chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    );
    let kind = match kind {
        StagedItemActionKind::Use {
            claimed_yaw,
            claimed_pitch,
        } => pumpkin_cluster::item_action::ItemActionKind::Use {
            claimed_yaw,
            claimed_pitch,
        },
        StagedItemActionKind::UseOnBlock { block, hit } => {
            let world = player.world();
            let world_id = super::cluster_world_delta::world_id(&world);
            let position = pumpkin_cluster::protocol::BlockPos {
                x: block.0.x,
                y: block.0.y,
                z: block.0.z,
            };
            let chunk =
                pumpkin_cluster::world_delta::chunk_of_block(world_id, position.x, position.z);
            let expected_block_state =
                super::cluster_chunk_state::published_block(&server, chunk, position)
                    .map_err(|_| InventoryApplyError::TargetUnavailable)?
                    .as_u16();
            pumpkin_cluster::item_action::ItemActionKind::UseOnBlock {
                world: world_id,
                block: position,
                hit,
                expected_block_state,
            }
        }
        StagedItemActionKind::UseOnActor {
            target,
            target_chunk,
            hit,
            interaction,
        } => pumpkin_cluster::item_action::ItemActionKind::UseOnActor {
            target,
            target_chunk,
            hit,
            interaction,
        },
        StagedItemActionKind::Release => {
            let (frame, eye_height) = effective_frame.ok_or(InventoryApplyError::Rejected)?;
            pumpkin_cluster::item_action::ItemActionKind::Release {
                claimed_head_position: [
                    frame.claimed.position[0] as f32,
                    (frame.claimed.position[1] + eye_height) as f32,
                    frame.claimed.position[2] as f32,
                ],
            }
        }
    };
    super::cluster_item_action::submit_item_action_with_sequence(
        &server,
        pumpkin_cluster::item_action::ItemActionIntent {
            actor: pumpkin_cluster::actor::ActorId::Player(gid),
            tick,
            source: InventorySlotRef::new(target, loc),
            expected_inventory_revision: snapshot.revision,
            expected_item,
            actor_chunk,
            affected: Vec::new(),
            kind,
            client_claim: pumpkin_cluster::item_action::ItemActionClientClaim {
                changes: client_claim.changes,
                item_in_hand,
                hotbar_slot: client_claim.hotbar_slot,
                clicked_block_state: client_claim.clicked_block_state,
            },
        }
        .normalized(),
        origin,
        input_sequence,
        effective_frame,
    )
}

pub(crate) fn stage_screen_close(
    player: &crate::entity::player::Player,
    session: u64,
    token: u64,
    reserve_departure: bool,
) -> Result<(), InventoryApplyError> {
    if player
        .cluster_inventory_inputs
        .push(StagedInventoryInput::Close { session, token })
        .is_err()
    {
        let close = StagedClose {
            session,
            token,
            any: false,
        };
        if let Err(close) = player.cluster_inventory_pending_close.push(close) {
            if reserve_departure {
                player
                    .cluster_inventory_departure_close
                    .push(close)
                    .map_err(|_| InventoryApplyError::QueueFull)?;
            } else {
                return Err(InventoryApplyError::QueueFull);
            }
        }
    }
    Ok(())
}

pub(crate) fn stage_departure_screen_close_any(
    player: &crate::entity::player::Player,
    token: u64,
) -> Result<(), InventoryApplyError> {
    player
        .cluster_inventory_departure_close
        .push(StagedClose {
            session: 0,
            token,
            any: true,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

fn drain_pending_screen_close(
    player: &crate::entity::player::Player,
    tick: pumpkin_cluster::time::TickStamp,
    sequence: &mut u16,
) {
    let Some(close) = player.cluster_inventory_pending_close.pop() else {
        return;
    };
    let input_sequence = pumpkin_cluster::identity::PlayerSeq(*sequence);
    let result = if close.any {
        submit_screen_close_any(player, close.token, input_sequence)
    } else {
        submit_screen_close(player, tick, close.session, close.token, input_sequence)
    };
    if result.is_err() {
        let _ = player.cluster_inventory_pending_close.push(close);
    } else {
        *sequence = sequence.wrapping_add(1);
    }
}

fn drain_departure_screen_close(
    player: &crate::entity::player::Player,
    tick: pumpkin_cluster::time::TickStamp,
    sequence: &mut u16,
) {
    if !player.cluster_inventory_pending_close.is_empty() {
        return;
    }
    let Some(close) = player.cluster_inventory_departure_close.pop() else {
        return;
    };
    let input_sequence = pumpkin_cluster::identity::PlayerSeq(*sequence);
    let result = if close.any {
        submit_screen_close_any(player, close.token, input_sequence)
    } else {
        submit_screen_close(player, tick, close.session, close.token, input_sequence)
    };
    if result.is_err() {
        let _ = player.cluster_inventory_departure_close.push(close);
    } else {
        *sequence = sequence.wrapping_add(1);
    }
}

fn submit_screen_close_any(
    player: &crate::entity::player::Player,
    token: u64,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    super::cluster_invsee::begin_departure_inventory_close(&server, actor, token, input_sequence)
}

fn submit_screen_close(
    player: &crate::entity::player::Player,
    tick: pumpkin_cluster::time::TickStamp,
    session: u64,
    token: u64,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let position = player.position();
    let mut affected = vec![pumpkin_cluster::world_delta::chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    )];
    if let Some(screen) = super::cluster_invsee::inventory_screen_snapshot(
        &server,
        pumpkin_cluster::inventory::InventoryScreenSession { actor, session },
    ) && screen.slots.iter().any(
        |slot| matches!(slot.slot.target, InventoryTarget::Virtual { kind, .. } if kind != 0x100),
    ) {
        affected.extend(screen.affected);
    }
    affected.sort_unstable();
    affected.dedup();
    super::cluster_invsee::submit_inventory_screen_command_with_sequence_and_close_token(
        &server,
        pumpkin_cluster::inventory::InventoryScreenCommand::Close {
            session: pumpkin_cluster::inventory::InventoryScreenSession { actor, session },
        },
        tick,
        affected,
        None,
        Some(input_sequence),
        Some(token),
    )
}

pub(crate) fn stage_selected_hotbar(
    player: &crate::entity::player::Player,
    slot: u8,
    bedrock: bool,
) -> bool {
    stage_inventory_input(
        player,
        StagedInventoryInput::SelectHotbar {
            slot,
            origin: if bedrock {
                BEDROCK_HELD_TAG | u64::from(slot)
            } else {
                JAVA_HELD_TAG | u64::from(slot)
            },
        },
    )
}

pub(crate) fn stage_held_drop(player: &crate::entity::player::Player, whole_stack: bool) -> bool {
    stage_inventory_input(player, StagedInventoryInput::DropHeld { whole_stack })
}

pub(crate) fn stage_hand_swap(player: &crate::entity::player::Player) -> bool {
    stage_inventory_input(player, StagedInventoryInput::SwapHands)
}

pub(crate) fn stage_book_edit(
    player: &crate::entity::player::Player,
    pages: Vec<String>,
    title: Option<String>,
) -> bool {
    stage_inventory_input(player, StagedInventoryInput::EditBook { pages, title })
}

fn start_book_preflight(
    server: &Arc<Server>,
    player: &Arc<crate::entity::player::Player>,
    pages: Vec<String>,
    title: Option<String>,
) -> Result<BookPreflight, InventoryApplyError> {
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let snapshot =
        super::cluster_invsee::inventory_snapshot(server, InventoryTarget::Player(actor))
            .ok_or(InventoryApplyError::TargetUnavailable)?;
    let slot = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc == InvLoc::new(pumpkin_cluster::inventory::INV_SELECTED, 0))
        .and_then(|cell| cell.item.checked_sub(1))
        .filter(|slot| *slot < 9)
        .ok_or(InventoryApplyError::Rejected)?;
    let (send, result) = tokio::sync::oneshot::channel();
    let server = server.clone();
    let policy_player = player.clone();
    let spawn_server = server.clone();
    spawn_server.spawn_task(async move {
        let mut event = crate::plugin::api::events::player::player_edit_book::PlayerEditBookEvent {
            player: policy_player,
            slot: u32::from(slot),
            signing: title.is_some(),
            pages,
            title,
            cancelled: false,
        };
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.plugin_manager.fire(&server, &mut event),
        )
        .await;
        let result = if outcome.is_err() {
            BookPolicyResult::TimedOut
        } else if event.cancelled {
            BookPolicyResult::Cancelled
        } else {
            BookPolicyResult::Approved {
                pages: event.pages,
                title: event.title,
            }
        };
        let _ = send.send(result);
    });
    Ok(BookPreflight {
        player: player.clone(),
        slot,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
        result,
    })
}

fn start_creative_preflight(
    server: &Arc<Server>,
    player: &Arc<crate::entity::player::Player>,
    packet: pumpkin_protocol::java::server::play::SSetCreativeSlot,
) -> Result<BookPreflight, InventoryApplyError> {
    if player.gamemode.load() != pumpkin_util::GameMode::Creative {
        return Err(InventoryApplyError::Rejected);
    }
    let stack = packet
        .clicked_item
        .to_stack_for_version(&player.client.java_version());
    if !stack.is_empty() && stack.item_count > stack.get_max_stack_size() {
        return Err(InventoryApplyError::Rejected);
    }
    let (send, result) = tokio::sync::oneshot::channel();
    let server = server.clone();
    let policy_player = player.clone();
    let spawn_server = server.clone();
    let slot = packet.slot;
    spawn_server.spawn_task(async move {
        let mut event =
            crate::plugin::api::events::inventory::inventory_creative::InventoryCreativeEvent::new(
                policy_player,
                slot,
                stack.item.registry_key.to_string(),
                stack.item_count,
            );
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.plugin_manager.fire(&server, &mut event),
        )
        .await;
        let result = if outcome.is_err() {
            BookPolicyResult::TimedOut
        } else if event.cancelled {
            BookPolicyResult::Cancelled
        } else {
            let changed_item = event.item_id != stack.item.registry_key;
            let approved = if changed_item {
                pumpkin_data::item::Item::from_registry_key(
                    event
                        .item_id
                        .strip_prefix("minecraft:")
                        .unwrap_or(&event.item_id),
                )
                .map(|item| ItemStack::new(event.item_count, item))
            } else {
                let mut stack = stack;
                stack.item_count = event.item_count;
                Some(stack)
            };
            approved.map_or(BookPolicyResult::Cancelled, |stack| {
                BookPolicyResult::CreativeApproved {
                    slot: event.slot,
                    stack,
                }
            })
        };
        let _ = send.send(result);
    });
    Ok(BookPreflight {
        player: player.clone(),
        slot: 0,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
        result,
    })
}

fn process_book_edit(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    pages: Vec<String>,
    title: Option<String>,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
    expected_slot: Option<u16>,
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::InventoryGesture;
    use pumpkin_data::data_component::DataComponent;
    use pumpkin_data::data_component_impl::{
        DataComponentImpl, WritableBookContentImpl, WrittenBookContentImpl,
    };
    use pumpkin_data::item::Item;

    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let target = InventoryTarget::Player(actor);
    let snapshot = super::cluster_invsee::inventory_snapshot(&server, target)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let selected = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc == InvLoc::new(pumpkin_cluster::inventory::INV_SELECTED, 0))
        .and_then(|cell| cell.item.checked_sub(1))
        .and_then(|slot| u16::try_from(slot).ok())
        .filter(|slot| *slot < 9)
        .ok_or(InventoryApplyError::Rejected)?;
    if expected_slot.is_some_and(|slot| slot != selected) {
        return Err(InventoryApplyError::Rejected);
    }
    let loc = InvLoc::new(INV_MAIN, selected);
    let before = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc == loc)
        .cloned()
        .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc));
    let mut stack = stack_from_effect(&before)?;
    if stack.item.id != Item::WRITABLE_BOOK.id || stack.item_count != 1 {
        return Err(InventoryApplyError::Rejected);
    }
    if let Some(title) = title {
        let mut signed = ItemStack::new(1, &Item::WRITTEN_BOOK);
        signed.patch.push((
            DataComponent::WrittenBookContent,
            Some(
                WrittenBookContentImpl {
                    title,
                    author: player.gameprofile.name.clone(),
                    pages,
                }
                .to_dyn(),
            ),
        ));
        stack = signed;
    } else {
        stack
            .patch
            .retain(|(component, _)| *component != DataComponent::WritableBookContent);
        stack.patch.push((
            DataComponent::WritableBookContent,
            Some(WritableBookContentImpl { pages }.to_dyn()),
        ));
    }
    let after = super::cluster_invsee::inventory_cell_from_item_stack(loc, &stack)?;
    submit_gestures(
        player,
        tick,
        vec![InventoryGesture::ExactSet {
            destination: InventorySlotRef::new(target, loc),
            before,
            after,
        }],
        None,
        Some(input_sequence),
    )
}

fn process_held_inventory_action(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    drop_whole_stack: Option<bool>,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::{InventoryGesture, InventoryRemovalKind};

    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let target = InventoryTarget::Player(actor);
    let snapshot = super::cluster_invsee::inventory_snapshot(&server, target)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let selected = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc == InvLoc::new(pumpkin_cluster::inventory::INV_SELECTED, 0))
        .and_then(|cell| cell.item.checked_sub(1))
        .and_then(|slot| u16::try_from(slot).ok())
        .filter(|slot| *slot < 9)
        .ok_or(InventoryApplyError::Rejected)?;
    let held = InventorySlotRef::new(target, InvLoc::new(INV_MAIN, selected));
    let gesture = if let Some(whole_stack) = drop_whole_stack {
        let cell = snapshot
            .cells
            .iter()
            .find(|cell| cell.loc == held.loc)
            .cloned()
            .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(held.loc));
        if cell.is_empty() {
            return Ok(());
        }
        InventoryGesture::Remove {
            source: held,
            count: if whole_stack { cell.count } else { 1 },
            kind: InventoryRemovalKind::Drop,
        }
    } else {
        InventoryGesture::Swap {
            first: held,
            second: InventorySlotRef::new(target, InvLoc::new(INV_OFFHAND, 0)),
            first_limit: 64,
            second_limit: 64,
        }
    };
    submit_gestures(player, tick, vec![gesture], None, Some(input_sequence))
}

pub(crate) fn stage_pick_block(
    player: &crate::entity::player::Player,
    position: pumpkin_util::math::position::BlockPos,
    target_hotbar: Option<u8>,
    bedrock: bool,
) -> bool {
    stage_inventory_input(
        player,
        StagedInventoryInput::PickBlock {
            position,
            target_hotbar,
            bedrock,
        },
    )
}

pub(crate) fn stage_pick_entity(player: &crate::entity::player::Player, entity_id: i32) -> bool {
    stage_inventory_input(player, StagedInventoryInput::PickEntity(entity_id))
}

pub(crate) fn stage_java_place_recipe(
    player: &crate::entity::player::Player,
    display_id: i32,
    use_max: bool,
) -> bool {
    stage_inventory_input(
        player,
        StagedInventoryInput::JavaPlaceRecipe {
            display_id,
            use_max,
        },
    )
}

pub(crate) fn stage_java_menu(
    player: &crate::entity::player::Player,
    input: JavaMenuInput,
) -> bool {
    stage_inventory_input(player, StagedInventoryInput::JavaMenu(input))
}

fn process_java_menu(
    player: &crate::entity::player::Player,
    tick: pumpkin_cluster::time::TickStamp,
    input: JavaMenuInput,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::{InventoryContainerButton, InventoryMenuKind};
    use pumpkin_data::screen::WindowType;

    let screen = current_screen_snapshot(player)?;
    let kind = match input {
        JavaMenuInput::Rename(name) if screen.window_kind == WindowType::Anvil as u16 => {
            InventoryMenuKind::RenameAnvil { name }
        }
        JavaMenuInput::SelectTrade(offer)
            if screen.window_kind == WindowType::Merchant as u16 && offer >= 0 =>
        {
            InventoryMenuKind::SelectTrade {
                offer: offer as u32,
            }
        }
        JavaMenuInput::SetBeacon { primary, secondary }
            if screen.window_kind == WindowType::Beacon as u16 =>
        {
            InventoryMenuKind::SetBeacon {
                primary_effect: u16::try_from(primary.ok_or(InventoryApplyError::Rejected)?)
                    .map_err(|_| InventoryApplyError::Rejected)?,
                secondary_effect: secondary
                    .map(u16::try_from)
                    .transpose()
                    .map_err(|_| InventoryApplyError::Rejected)?,
            }
        }
        JavaMenuInput::Button { sync_id, button }
            if sync_id == i32::from(screen.sync_id) && button >= 0 =>
        {
            match screen.window_kind {
                kind if kind == WindowType::Enchantment as u16 => {
                    InventoryMenuKind::ContainerButton(InventoryContainerButton::Enchant {
                        option: u8::try_from(button).map_err(|_| InventoryApplyError::Rejected)?,
                    })
                }
                kind if kind == WindowType::Stonecutter as u16 => {
                    InventoryMenuKind::ContainerButton(
                        InventoryContainerButton::StonecutterRecipe {
                            recipe: button as u32,
                        },
                    )
                }
                kind if kind == WindowType::Loom as u16 => InventoryMenuKind::ApplyLoomPattern {
                    pattern: pumpkin_cluster::inventory::InventoryPatternSelection::MenuIndex(
                        u16::try_from(button).map_err(|_| InventoryApplyError::Rejected)?,
                    ),
                },
                kind if kind == WindowType::Lectern as u16 => {
                    InventoryMenuKind::ContainerButton(InventoryContainerButton::LecternPage {
                        page: u16::try_from(button).map_err(|_| InventoryApplyError::Rejected)?,
                    })
                }
                _ => return Err(InventoryApplyError::Rejected),
            }
        }
        _ => return Err(InventoryApplyError::Rejected),
    };
    submit_menu_kind_with_companions(player, tick, kind, None, None, Some(input_sequence))
}

pub(crate) fn submit_menu_kind_with_companions(
    player: &crate::entity::player::Player,
    tick: pumpkin_cluster::time::TickStamp,
    kind: pumpkin_cluster::inventory::InventoryMenuKind,
    companion_gestures: Option<Vec<pumpkin_cluster::inventory::InventoryGesture>>,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::{InventoryMenuIntent, InventoryScreenSession};

    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let gid = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let session = InventoryScreenSession {
        actor: gid,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
    };
    let screen = super::cluster_invsee::inventory_screen_snapshot(&server, session)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let snapshots = server.cluster_inventory_snapshots.load();
    let mut targets: Vec<_> = screen.slots.iter().map(|slot| slot.slot.target).collect();
    targets.push(InventoryTarget::Player(gid));
    targets.sort_unstable();
    targets.dedup();
    let expected_revisions = targets
        .into_iter()
        .map(|target| {
            snapshots
                .get(&target)
                .map(|snapshot| (target, snapshot.revision))
                .ok_or(InventoryApplyError::TargetUnavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    submit_inventory_menu(
        &server,
        InventoryMenuIntent {
            actor: gid.into(),
            tick,
            session,
            sync_id: screen.sync_id,
            window_kind: screen.window_kind,
            expected_revisions,
            actor_chunk: player_chunk(player),
            affected: screen.affected,
            kind,
            companion_gestures,
        },
        origin,
        input_sequence,
    )
}

fn process_pick_block(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    position: pumpkin_util::math::position::BlockPos,
    target_hotbar: Option<u8>,
    bedrock: bool,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    if !player.can_interact_with_block_at(&position, 1.0) {
        return Err(InventoryApplyError::Rejected);
    }
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let world = player.world();
    let world_id = super::cluster_world_delta::world_id(&world);
    let block = pumpkin_cluster::protocol::BlockPos {
        x: position.0.x,
        y: position.0.y,
        z: position.0.z,
    };
    let chunk = pumpkin_cluster::world_delta::chunk_of_block(world_id, block.x, block.z);
    let state = super::cluster_chunk_state::published_block(&server, chunk, block)
        .map_err(|_| InventoryApplyError::TargetUnavailable)?;
    let item = pumpkin_data::Block::from_state_id(state).item_id;
    if item == 0 {
        return Err(InventoryApplyError::Rejected);
    }
    process_pick_item(player, tick, item, target_hotbar, bedrock, input_sequence)
}

fn process_pick_entity(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    entity_id: i32,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    use pumpkin_data::entity::{entity_from_egg, spawn_egg_ids};

    let target = player
        .world()
        .get_entity_by_id(entity_id)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor_eye = player.get_entity().get_eye_pos();
    let target_eye = target.get_eye_pos();
    let dx = actor_eye.x - target_eye.x;
    let dy = actor_eye.y - target_eye.y;
    let dz = actor_eye.z - target_eye.z;
    if dx * dx + dy * dy + dz * dz > 64.0 {
        return Err(InventoryApplyError::Rejected);
    }
    let entity_type = target.get_entity().entity_type.id;
    let item = spawn_egg_ids()
        .into_iter()
        .find(|item| entity_from_egg(*item).is_some_and(|kind| kind.id == entity_type))
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    process_pick_item(player, tick, item, None, false, input_sequence)
}

fn process_pick_item(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    item: u16,
    target_hotbar: Option<u8>,
    bedrock: bool,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let target = InventoryTarget::Player(actor);
    let snapshot = super::cluster_invsee::inventory_snapshot(&server, target)
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let selected_cell = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc.inv == pumpkin_cluster::inventory::INV_SELECTED && cell.loc.slot == 0)
        .cloned()
        .ok_or(InventoryApplyError::Rejected)?;
    let selected = selected_cell
        .item
        .checked_sub(1)
        .and_then(|slot| u8::try_from(slot).ok())
        .filter(|slot| *slot < 9)
        .ok_or(InventoryApplyError::Rejected)?;
    let found = snapshot.cells.iter().find(|cell| {
        cell.loc.inv == INV_MAIN && cell.loc.slot < 36 && !cell.is_empty() && cell.item == item
    });
    let hotbar = if let Some(slot) = target_hotbar {
        if slot >= 9 {
            return Err(InventoryApplyError::Rejected);
        }
        slot
    } else if let Some(cell) = found
        && cell.loc.slot < 9
    {
        cell.loc.slot as u8
    } else {
        (0..9)
            .map(|offset| (selected + offset) % 9)
            .find(|slot| {
                snapshot.cells.iter().all(|cell| {
                    cell.loc != InvLoc::new(INV_MAIN, u16::from(*slot)) || cell.is_empty()
                })
            })
            .unwrap_or(selected)
    };
    let mut gestures = Vec::new();
    let destination = InventorySlotRef::new(target, InvLoc::new(INV_MAIN, u16::from(hotbar)));
    if let Some(source) = found {
        if source.loc != destination.loc {
            gestures.push(pumpkin_cluster::inventory::InventoryGesture::Swap {
                first: InventorySlotRef::new(target, source.loc),
                second: destination,
                first_limit: 64,
                second_limit: 64,
            });
        }
    } else if player.gamemode.load() == pumpkin_util::GameMode::Creative {
        let stack = ItemStack::new(
            1,
            pumpkin_data::item::Item::from_id(item).ok_or(InventoryApplyError::Rejected)?,
        );
        gestures.push(pumpkin_cluster::inventory::InventoryGesture::CreativeSet {
            destination,
            stack: super::cluster_invsee::inventory_cell_from_item_stack(destination.loc, &stack)?,
            destination_limit: stack.get_max_stack_size(),
        });
    } else {
        return Err(InventoryApplyError::Rejected);
    }
    let selected_loc = InvLoc::new(pumpkin_cluster::inventory::INV_SELECTED, 0);
    gestures.push(pumpkin_cluster::inventory::InventoryGesture::ExactSet {
        destination: InventorySlotRef::new(target, selected_loc),
        before: selected_cell,
        after: pumpkin_cluster::inventory::InventoryCell {
            loc: selected_loc,
            item: u16::from(hotbar) + 1,
            count: 1,
            nbt: Vec::new(),
        },
    });
    submit_gestures(
        player,
        tick,
        gestures,
        Some(if bedrock {
            BEDROCK_HELD_TAG | u64::from(hotbar)
        } else {
            JAVA_HELD_TAG | u64::from(hotbar)
        }),
        Some(input_sequence),
    )
}

#[must_use]
pub const fn bedrock_stack_request_origin(request_id: i32) -> u64 {
    BEDROCK_STACK_REQUEST_TAG | request_id as u32 as u64
}

const fn bedrock_stack_request_id(origin: u64) -> Option<i32> {
    if origin & ORIGIN_TAG_MASK == BEDROCK_STACK_REQUEST_TAG {
        Some(origin as u32 as i32)
    } else {
        None
    }
}

const fn java_click_origin(sync_id: i32, revision: i32) -> u64 {
    JAVA_CLICK_TAG | ((sync_id as u32 as u64) << 16) | (revision as u16 as u64)
}

const fn is_java_click_origin(origin: u64) -> bool {
    origin & ORIGIN_TAG_MASK == JAVA_CLICK_TAG
}

#[must_use]
pub(crate) const fn java_item_action_origin(sequence: i32) -> u64 {
    JAVA_ITEM_ACTION_TAG | sequence as u32 as u64
}

const fn bedrock_item_action_origin(request_id: i32) -> Option<u64> {
    if request_id == 0 {
        None
    } else {
        Some(BEDROCK_ITEM_ACTION_TAG | request_id as u32 as u64)
    }
}

pub(crate) fn publish_item_action_origin(
    server: &Server,
    actor: GlobalPlayerId,
    kind: &pumpkin_cluster::item_action::ItemActionKind,
    origin: Option<u64>,
    accepted: bool,
) {
    let Some(player) = find_player(server, actor) else {
        return;
    };
    if !accepted {
        if let pumpkin_cluster::item_action::ItemActionKind::UseOnBlock { block, hit, .. } = kind {
            send_authoritative_item_block(
                &player,
                pumpkin_util::math::position::BlockPos::new(block.x, block.y, block.z),
                Some(hit.face),
            );
        }
        resync_inventory_screen(&player);
    }
    send_item_action_response(&player, origin, accepted);
}

pub(crate) fn publish_block_action_resolution(
    server: &Server,
    actor: GlobalPlayerId,
    position: pumpkin_cluster::protocol::BlockPos,
    authoritative_state: Option<pumpkin_data::block_state::BlockStateId>,
    client_sequence: Option<i32>,
    accepted: bool,
) -> Result<(), InventoryApplyError> {
    let player = find_player(server, actor).ok_or(InventoryApplyError::TargetUnavailable)?;
    if accepted && authoritative_state.is_none() {
        if let (crate::net::ClientPlatform::Java(_), Some(sequence)) =
            (player.client.as_ref(), client_sequence)
        {
            player.try_send_client_packet(
                &pumpkin_protocol::java::client::play::CAcknowledgeBlockChange::new(VarInt(
                    sequence,
                )),
            );
        }
        return Ok(());
    }
    let Some(state) = authoritative_state else {
        player.client.try_kick(
            crate::net::DisconnectReason::Unknown,
            &pumpkin_util::text::TextComponent::text("Authoritative block correction unavailable"),
        );
        return Err(InventoryApplyError::TargetUnavailable);
    };
    let position = pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z);
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(_) => {
            player.try_send_client_packet(
                &pumpkin_protocol::java::client::play::CBlockUpdate::new(
                    position,
                    VarInt(i32::from(state.as_u16())),
                ),
            );
            if let Some(sequence) = client_sequence {
                player.try_send_client_packet(
                    &pumpkin_protocol::java::client::play::CAcknowledgeBlockChange::new(VarInt(
                        sequence,
                    )),
                );
            }
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            client.try_enqueue_client_packet(
                &pumpkin_protocol::bedrock::client::CUpdateBlock::new(
                    position,
                    u32::from(pumpkin_data::BlockState::to_be_network_id(state)),
                ),
            );
        }
    }
    Ok(())
}

fn send_authoritative_item_block(
    player: &crate::entity::player::Player,
    _position: pumpkin_util::math::position::BlockPos,
    _face: Option<u8>,
) {
    player.client.try_kick(
        crate::net::DisconnectReason::Unknown,
        &pumpkin_util::text::TextComponent::text("Authoritative block correction unavailable"),
    );
}

fn send_item_action_response(
    player: &crate::entity::player::Player,
    origin: Option<u64>,
    accepted: bool,
) {
    let Some(origin) = origin else {
        return;
    };
    if origin & ORIGIN_TAG_MASK == JAVA_ITEM_ACTION_TAG {
        if let crate::net::ClientPlatform::Java(_) = player.client.as_ref() {
            player.try_send_client_packet(
                &pumpkin_protocol::java::client::play::CAcknowledgeBlockChange::new(VarInt(
                    origin as u32 as i32,
                )),
            );
        }
    } else if origin & ORIGIN_TAG_MASK == BEDROCK_ITEM_ACTION_TAG
        && let Some(client) = player.client.bedrock()
    {
        client.try_enqueue_client_packet(&CItemStackResponse {
            responses: vec![ItemStackResponseInfo {
                result: u8::from(!accepted),
                client_request_id: VarInt(origin as u32 as i32),
                containers: Vec::new(),
            }],
        });
    }
}

pub(crate) fn current_screen_snapshot(
    player: &crate::entity::player::Player,
) -> Result<pumpkin_cluster::inventory::InventoryScreenSnapshot, InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let session = pumpkin_cluster::inventory::InventoryScreenSession {
        actor,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
    };
    super::cluster_invsee::inventory_screen_snapshot(&server, session)
        .ok_or(InventoryApplyError::TargetUnavailable)
}

fn bedrock_screen_index(
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    container: pumpkin_protocol::bedrock::network_item::ContainerName,
    slot: u8,
) -> Option<usize> {
    use pumpkin_protocol::bedrock::network_item::ContainerName;

    let container_slots = usize::from(screen.container_slots);
    let player_screen = screen.session.session == 0;
    use pumpkin_data::screen::WindowType;
    match container {
        ContainerName::HotBar => Some(if player_screen {
            36 + usize::from(slot)
        } else {
            container_slots + 27 + usize::from(slot)
        }),
        ContainerName::Inventory | ContainerName::CombinedHotBarAndInventory => match slot {
            0..=8 => Some(if player_screen {
                36 + usize::from(slot)
            } else {
                container_slots + 27 + usize::from(slot)
            }),
            9..=35 => Some(if player_screen {
                usize::from(slot)
            } else {
                container_slots + usize::from(slot - 9)
            }),
            _ => None,
        },
        ContainerName::Armor => (slot < 4).then(|| 5 + usize::from(slot)),
        ContainerName::Offhand => (slot == 0).then_some(45),
        ContainerName::Cursor => None,
        ContainerName::CraftingInput
            if player_screen || screen.window_kind == WindowType::Crafting as u16 =>
        {
            let start = if player_screen { 28 } else { 32 };
            slot.checked_sub(start).and_then(|index| {
                let index = usize::from(index) + 1;
                (index < if player_screen { 5 } else { 10 }).then_some(index)
            })
        }
        ContainerName::CreatedOutput | ContainerName::CraftingOutputPreview => {
            if player_screen || screen.window_kind == WindowType::Crafting as u16 {
                Some(0)
            } else if screen.window_kind == WindowType::Stonecutter as u16 {
                Some(1)
            } else if screen.window_kind == WindowType::Loom as u16 {
                Some(3)
            } else if matches!(
                screen.window_kind,
                kind if kind == WindowType::Anvil as u16
                    || kind == WindowType::Grindstone as u16
                    || kind == WindowType::Merchant as u16
            ) {
                Some(2)
            } else {
                None
            }
        }
        ContainerName::AnvilInput
            if screen.window_kind == WindowType::Anvil as u16 && slot == 0 =>
        {
            Some(0)
        }
        ContainerName::AnvilMaterial
            if screen.window_kind == WindowType::Anvil as u16 && slot == 0 =>
        {
            Some(1)
        }
        ContainerName::AnvilResultPreview
            if screen.window_kind == WindowType::Anvil as u16 && slot == 0 =>
        {
            Some(2)
        }
        ContainerName::BeaconPayment
            if screen.window_kind == WindowType::Beacon as u16 && slot == 0 =>
        {
            Some(0)
        }
        ContainerName::LoomInput if screen.window_kind == WindowType::Loom as u16 && slot == 0 => {
            Some(0)
        }
        ContainerName::LoomDye if screen.window_kind == WindowType::Loom as u16 && slot == 0 => {
            Some(1)
        }
        ContainerName::LoomMaterial
            if screen.window_kind == WindowType::Loom as u16 && slot == 0 =>
        {
            Some(2)
        }
        ContainerName::LoomResultPreview
            if screen.window_kind == WindowType::Loom as u16 && slot == 0 =>
        {
            Some(3)
        }
        ContainerName::GrindstoneInput
            if screen.window_kind == WindowType::Grindstone as u16 && slot == 0 =>
        {
            Some(0)
        }
        ContainerName::GrindstoneAdditional
            if screen.window_kind == WindowType::Grindstone as u16 && slot == 0 =>
        {
            Some(1)
        }
        ContainerName::GrindstoneResultPreview
            if screen.window_kind == WindowType::Grindstone as u16 && slot == 0 =>
        {
            Some(2)
        }
        ContainerName::TradeIngredient1 | ContainerName::Trade2Ingredient1
            if screen.window_kind == WindowType::Merchant as u16 && slot == 4 =>
        {
            Some(0)
        }
        ContainerName::TradeIngredient2 | ContainerName::Trade2Ingredient2
            if screen.window_kind == WindowType::Merchant as u16 && slot == 5 =>
        {
            Some(1)
        }
        ContainerName::TradeResultPreview | ContainerName::Trade2ResultPreview
            if screen.window_kind == WindowType::Merchant as u16 && slot == 50 =>
        {
            Some(2)
        }
        ContainerName::LevelEntity => {
            (usize::from(slot) < container_slots).then_some(usize::from(slot))
        }
        _ => None,
    }
}

pub(crate) fn bedrock_slot_state(
    player: &crate::entity::player::Player,
    slot: &ItemStackRequestSlotInfo,
) -> Result<(InventorySlotRef, pumpkin_cluster::inventory::InventoryCell), InventoryApplyError> {
    let screen = current_screen_snapshot(player)?;
    let (slot_ref, stack) = if slot.container_name.container_name
        == pumpkin_protocol::bedrock::network_item::ContainerName::Cursor
    {
        let slot_ref = InventorySlotRef::new(
            InventoryTarget::Player(
                player
                    .cluster_gid()
                    .ok_or(InventoryApplyError::TargetUnavailable)?,
            ),
            InvLoc::new(INV_CURSOR, 0),
        );
        let cell = screen.cursor.clone();
        let actual_stack_id = stack_from_effect(&cell).map_or(0, |stack| stack.uid.get());
        if slot.stack_id != actual_stack_id {
            return Err(InventoryApplyError::Rejected);
        }
        return Ok((slot_ref, cell));
    } else {
        let screen_slot =
            bedrock_screen_index(&screen, slot.container_name.container_name, slot.slot_id)
                .ok_or(InventoryApplyError::Rejected)?;
        let entry = screen
            .slots
            .iter()
            .find(|entry| usize::from(entry.screen_slot) == screen_slot)
            .ok_or(InventoryApplyError::Rejected)?;
        let stack = stack_from_effect(&entry.cell)?;
        (entry.slot, stack)
    };
    let actual_stack_id = if stack.is_empty() { 0 } else { stack.uid.get() };
    if slot.stack_id != actual_stack_id {
        return Err(InventoryApplyError::Rejected);
    }
    let cell = super::cluster_invsee::inventory_cell_from_item_stack(slot_ref.loc, &stack)?;
    Ok((slot_ref, cell))
}

fn screen_slot_state(
    player: &crate::entity::player::Player,
    screen_slot: usize,
) -> Result<
    (
        InventorySlotRef,
        pumpkin_cluster::inventory::InventoryCell,
        u8,
    ),
    InventoryApplyError,
> {
    let screen = current_screen_snapshot(player)?;
    let slot = screen
        .slots
        .iter()
        .find(|entry| usize::from(entry.screen_slot) == screen_slot)
        .ok_or(InventoryApplyError::Rejected)?;
    if slot.limit == 0 {
        return Err(InventoryApplyError::Rejected);
    }
    Ok((slot.slot, slot.cell.clone(), slot.limit))
}

fn cursor_state(
    player: &crate::entity::player::Player,
) -> Result<
    (
        InventorySlotRef,
        pumpkin_cluster::inventory::InventoryCell,
        u8,
    ),
    InventoryApplyError,
> {
    let gid = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let slot_ref = InventorySlotRef::new(InventoryTarget::Player(gid), InvLoc::new(INV_CURSOR, 0));
    let screen = current_screen_snapshot(player)?;
    let cell = screen.cursor;
    let limit = stack_from_effect(&cell)?.get_max_stack_size().max(1);
    Ok((slot_ref, cell, limit))
}

fn screen_slot_count(player: &crate::entity::player::Player) -> (usize, usize, bool) {
    current_screen_snapshot(player).map_or((0, 0, false), |screen| {
        (
            screen
                .slots
                .iter()
                .map(|slot| usize::from(slot.screen_slot) + 1)
                .max()
                .unwrap_or(0),
            usize::from(screen.container_slots),
            screen.session.session == 0,
        )
    })
}

fn unresolved_result_slot(
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    index: usize,
) -> bool {
    use pumpkin_data::screen::WindowType;

    if screen.session.session == 0 {
        return index == 0;
    }
    match screen.window_kind {
        kind if kind == WindowType::Crafting as u16 => index == 0,
        kind if kind == WindowType::Stonecutter as u16 => index == 1,
        kind if kind == WindowType::Anvil as u16
            || kind == WindowType::Grindstone as u16
            || kind == WindowType::Merchant as u16
            || kind == WindowType::Furnace as u16
            || kind == WindowType::BlastFurnace as u16
            || kind == WindowType::Smoker as u16
            || kind == WindowType::CartographyTable as u16 =>
        {
            index == 2
        }
        kind if kind == WindowType::Loom as u16 || kind == WindowType::Smithing as u16 => {
            index == 3
        }
        _ => false,
    }
}

fn screen_destinations(
    player: &crate::entity::player::Player,
    indices: impl Iterator<Item = usize>,
) -> Result<Vec<(InventorySlotRef, u8)>, InventoryApplyError> {
    let screen = current_screen_snapshot(player)?;
    let destinations: Vec<_> = indices
        .filter_map(|index| {
            if unresolved_result_slot(&screen, index) {
                return None;
            }
            screen
                .slots
                .iter()
                .find(|slot| usize::from(slot.screen_slot) == index && slot.limit > 0)
                .map(|slot| (slot.slot, slot.limit))
        })
        .collect();
    if destinations.is_empty() {
        return Err(InventoryApplyError::Rejected);
    }
    Ok(destinations)
}

fn submit_gestures(
    player: &Arc<crate::entity::player::Player>,
    tick: pumpkin_cluster::time::TickStamp,
    gestures: Vec<pumpkin_cluster::inventory::InventoryGesture>,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let affected = inventory_affected_chunks(player, &gestures);
    let actor_chunk = player_chunk(player);
    let screen_session = current_screen_snapshot(player)
        .ok()
        .map(|screen| screen.session);
    super::cluster_invsee::submit_inventory_intent_with_sequence(
        &server,
        pumpkin_cluster::inventory::InventoryMutationIntent {
            actor: actor.into(),
            tick,
            payload: pumpkin_cluster::inventory::InventoryIntentPayload::Gestures(gestures),
            affected,
            actor_chunk,
            screen_session,
        },
        origin,
        input_sequence,
    )
}

#[must_use]
pub(crate) fn player_chunk(
    player: &crate::entity::player::Player,
) -> pumpkin_cluster::protocol::ChunkKey {
    let position = player.position();
    pumpkin_cluster::world_delta::chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    )
}

fn submit_java_quick_craft(
    player: &Arc<crate::entity::player::Player>,
    packet: &pumpkin_protocol::java::server::play::SClickSlot,
    tick: pumpkin_cluster::time::TickStamp,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::{InventoryScreenCommand, QuickCraftMode};

    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let screen = current_screen_snapshot(player)?;
    if packet.sync_id.0 != i32::from(screen.sync_id) {
        return Err(InventoryApplyError::Rejected);
    }
    let session = screen.session;
    let phase = packet.button & 3;
    let command = match phase {
        0 => {
            let (source, _, _) = cursor_state(player)?;
            let mode = match (packet.button >> 2) & 3 {
                0 => QuickCraftMode::Even,
                1 => QuickCraftMode::OneEach,
                2 if player.gamemode.load() == pumpkin_util::GameMode::Creative => {
                    QuickCraftMode::Fill
                }
                _ => return Err(InventoryApplyError::Rejected),
            };
            InventoryScreenCommand::BeginQuickCraft {
                session,
                source,
                mode,
            }
        }
        1 => {
            let screen_index =
                usize::try_from(packet.slot).map_err(|_| InventoryApplyError::Rejected)?;
            if unresolved_result_slot(&screen, screen_index) {
                return Err(InventoryApplyError::Rejected);
            }
            let (destination, _, limit) = screen_slot_state(player, screen_index)?;
            InventoryScreenCommand::AddQuickCraftSlot {
                session,
                destination,
                limit,
            }
        }
        2 => InventoryScreenCommand::FinishQuickCraft { session },
        _ => return Err(InventoryApplyError::Rejected),
    };
    let position = player.position();
    let mut affected = vec![pumpkin_cluster::world_delta::chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    )];
    affected.extend(screen.affected);
    match &command {
        InventoryScreenCommand::BeginQuickCraft { source, .. } => {
            affected.extend(source.target.intrinsic_chunk());
        }
        InventoryScreenCommand::AddQuickCraftSlot { destination, .. } => {
            affected.extend(destination.target.intrinsic_chunk());
        }
        InventoryScreenCommand::FinishQuickCraft { .. } | InventoryScreenCommand::Close { .. } => {}
    }
    affected.sort_unstable();
    affected.dedup();
    super::cluster_invsee::submit_inventory_screen_command_with_sequence(
        &server,
        command,
        tick,
        affected,
        (phase == 2).then(|| java_click_origin(packet.sync_id.0, packet.revision.0)),
        Some(input_sequence),
    )
}

fn process_java_click(
    player: &Arc<crate::entity::player::Player>,
    packet: pumpkin_protocol::java::server::play::SClickSlot,
    tick: pumpkin_cluster::time::TickStamp,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::{InventoryGesture, InventoryRemovalKind};
    use pumpkin_protocol::java::server::play::SlotActionType;

    let screen = current_screen_snapshot(player)?;
    if packet.sync_id.0 != i32::from(screen.sync_id) {
        return Err(InventoryApplyError::Rejected);
    }
    if packet.mode == SlotActionType::QuickCraft {
        return submit_java_quick_craft(player, &packet, tick, input_sequence);
    }

    let (cursor, cursor_cell, cursor_limit) = cursor_state(player)?;
    let mut gestures = Vec::new();
    if packet.slot == -999 {
        if packet.mode != SlotActionType::Pickup || !matches!(packet.button, 0 | 1) {
            return Err(InventoryApplyError::Rejected);
        }
        let count = if packet.button == 0 {
            cursor_cell.count
        } else {
            1
        };
        gestures.push(InventoryGesture::Remove {
            source: cursor,
            count,
            kind: InventoryRemovalKind::Drop,
        });
        return submit_gestures(
            player,
            tick,
            gestures,
            Some(java_click_origin(packet.sync_id.0, packet.revision.0)),
            Some(input_sequence),
        );
    }
    let screen_index = usize::try_from(packet.slot).map_err(|_| InventoryApplyError::Rejected)?;
    if unresolved_result_slot(&screen, screen_index) {
        return Err(InventoryApplyError::Rejected);
    }
    let (clicked, clicked_cell, clicked_limit) = screen_slot_state(player, screen_index)?;
    match packet.mode {
        SlotActionType::Pickup => {
            if !matches!(packet.button, 0 | 1) {
                return Err(InventoryApplyError::Rejected);
            }
            let right = packet.button == 1;
            if cursor_cell.is_empty() {
                if clicked_cell.is_empty() {
                    return Ok(());
                }
                gestures.push(InventoryGesture::Transfer {
                    source: clicked,
                    destination: cursor,
                    count: if right {
                        clicked_cell.count.div_ceil(2)
                    } else {
                        clicked_cell.count
                    },
                    destination_limit: cursor_limit,
                });
            } else if clicked_cell.is_empty()
                || (clicked_cell.item == cursor_cell.item && clicked_cell.nbt == cursor_cell.nbt)
            {
                gestures.push(InventoryGesture::Transfer {
                    source: cursor,
                    destination: clicked,
                    count: if right { 1 } else { cursor_cell.count },
                    destination_limit: clicked_limit,
                });
            } else {
                gestures.push(InventoryGesture::Swap {
                    first: clicked,
                    second: cursor,
                    first_limit: clicked_limit,
                    second_limit: cursor_limit,
                });
            }
        }
        SlotActionType::Swap => {
            let hotbar = match packet.button {
                0..=8 => InvLoc::new(INV_MAIN, packet.button as u16),
                40 => InvLoc::new(INV_OFFHAND, 0),
                _ => return Err(InventoryApplyError::Rejected),
            };
            let hotbar = InventorySlotRef::new(
                InventoryTarget::Player(
                    player
                        .cluster_gid()
                        .ok_or(InventoryApplyError::TargetUnavailable)?,
                ),
                hotbar,
            );
            let hotbar_limit = screen
                .slots
                .iter()
                .find(|slot| slot.slot == hotbar && slot.limit > 0)
                .map(|slot| slot.limit)
                .or_else(|| (packet.button == 40).then_some(64))
                .ok_or(InventoryApplyError::Rejected)?;
            gestures.push(InventoryGesture::Swap {
                first: clicked,
                second: hotbar,
                first_limit: clicked_limit,
                second_limit: hotbar_limit,
            });
        }
        SlotActionType::Throw => {
            if !matches!(packet.button, 0 | 1) {
                return Err(InventoryApplyError::Rejected);
            }
            gestures.push(InventoryGesture::Remove {
                source: clicked,
                count: if packet.button == 0 {
                    1
                } else {
                    clicked_cell.count
                },
                kind: InventoryRemovalKind::Drop,
            });
        }
        SlotActionType::Clone => {
            if player.gamemode.load() != pumpkin_util::GameMode::Creative
                || clicked_cell.is_empty()
                || !cursor_cell.is_empty()
            {
                return Err(InventoryApplyError::Rejected);
            }
            let mut stack = clicked_cell;
            stack.loc = cursor.loc;
            stack.count = clicked_limit;
            gestures.push(InventoryGesture::CreativeSet {
                destination: cursor,
                stack,
                destination_limit: cursor_limit,
            });
        }
        SlotActionType::QuickMove => {
            let (slot_count, container_slots, player_screen) = screen_slot_count(player);
            let destinations = if player_screen {
                match screen_index {
                    9..=35 => screen_destinations(player, 36..45)?,
                    36..=44 => screen_destinations(player, 9..36)?,
                    5..=8 | 45 => screen_destinations(player, 9..45)?,
                    _ => return Err(InventoryApplyError::Rejected),
                }
            } else if screen_index < container_slots {
                screen_destinations(player, (container_slots..slot_count).rev())?
            } else {
                screen_destinations(player, 0..container_slots)?
            };
            gestures.push(InventoryGesture::QuickMove {
                source: clicked,
                destinations,
            });
        }
        SlotActionType::PickupAll => {
            if cursor_cell.is_empty() {
                return Err(InventoryApplyError::Rejected);
            }
            let (slot_count, _, _) = screen_slot_count(player);
            let mut sources = Vec::with_capacity(slot_count);
            for index in 0..slot_count {
                if unresolved_result_slot(&screen, index) {
                    continue;
                }
                if let Ok((source, _, _)) = screen_slot_state(player, index) {
                    sources.push(source);
                }
            }
            gestures.push(InventoryGesture::PickupAll {
                destination: cursor,
                sources,
                destination_limit: cursor_limit,
            });
        }
        SlotActionType::QuickCraft => unreachable!(),
    }
    submit_gestures(
        player,
        tick,
        gestures,
        Some(java_click_origin(packet.sync_id.0, packet.revision.0)),
        Some(input_sequence),
    )
}

fn process_java_creative(
    player: &Arc<crate::entity::player::Player>,
    packet: pumpkin_protocol::java::server::play::SSetCreativeSlot,
    tick: pumpkin_cluster::time::TickStamp,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    if player.gamemode.load() != pumpkin_util::GameMode::Creative {
        return Err(InventoryApplyError::Rejected);
    }
    let stack = packet
        .clicked_item
        .to_stack_for_version(&player.client.java_version());
    process_java_creative_stack(player, packet.slot, stack, tick, input_sequence)
}

fn process_java_creative_stack(
    player: &Arc<crate::entity::player::Player>,
    slot: i16,
    stack: ItemStack,
    tick: pumpkin_cluster::time::TickStamp,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    if player.gamemode.load() != pumpkin_util::GameMode::Creative {
        return Err(InventoryApplyError::Rejected);
    }
    if !stack.is_empty() && stack.item_count > stack.get_max_stack_size() {
        return Err(InventoryApplyError::Rejected);
    }
    if slot < 0 {
        if stack.is_empty() {
            return Ok(());
        }
        let owner = InventoryTarget::Player(
            player
                .cluster_gid()
                .ok_or(InventoryApplyError::TargetUnavailable)?,
        );
        let cell = super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_CURSOR, 0),
            &stack,
        )?;
        return submit_gestures(
            player,
            tick,
            vec![pumpkin_cluster::inventory::InventoryGesture::CreativeDrop { owner, stack: cell }],
            Some(java_click_origin(0, 0)),
            Some(input_sequence),
        );
    }
    if !(1..=45).contains(&slot) {
        return Err(InventoryApplyError::Rejected);
    }
    let screen_slot = usize::try_from(slot).map_err(|_| InventoryApplyError::Rejected)?;
    let (destination, _, limit) = screen_slot_state(player, screen_slot)?;
    let after = super::cluster_invsee::inventory_cell_from_item_stack(destination.loc, &stack)?;
    submit_gestures(
        player,
        tick,
        vec![pumpkin_cluster::inventory::InventoryGesture::CreativeSet {
            destination,
            stack: after,
            destination_limit: limit,
        }],
        Some(java_click_origin(0, 0)),
        Some(input_sequence),
    )
}

#[must_use]
pub(crate) fn inventory_affected_chunks(
    player: &crate::entity::player::Player,
    gestures: &[pumpkin_cluster::inventory::InventoryGesture],
) -> Vec<pumpkin_cluster::protocol::ChunkKey> {
    let position = player.position();
    let mut chunks = vec![pumpkin_cluster::world_delta::chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    )];
    chunks.extend(
        gestures
            .iter()
            .flat_map(pumpkin_cluster::inventory::InventoryGesture::targets)
            .filter_map(InventoryTarget::intrinsic_chunk),
    );
    if let (Some(server), Some(actor)) = (player.world().server.upgrade(), player.cluster_gid()) {
        let session = pumpkin_cluster::inventory::InventoryScreenSession {
            actor,
            session: player
                .cluster_current_inventory_session
                .load(Ordering::Acquire),
        };
        if let Some(screen) = super::cluster_invsee::inventory_screen_snapshot(&server, session) {
            chunks.extend(screen.affected);
        }
    }
    chunks.sort_unstable();
    chunks.dedup();
    chunks
}

fn find_player(
    server: &Server,
    target: GlobalPlayerId,
) -> Option<Arc<crate::entity::player::Player>> {
    server
        .worlds
        .load()
        .iter()
        .flat_map(|world| world.players.load().iter().cloned().collect::<Vec<_>>())
        .find(|player| player.cluster_gid() == Some(target))
}

fn bedrock_window_type(window_kind: u16) -> Option<u8> {
    use pumpkin_data::screen::WindowType;

    if matches!(
        window_kind,
        kind if kind == WindowType::Generic9x1 as u16
            || kind == WindowType::Generic9x2 as u16
            || kind == WindowType::Generic9x3 as u16
            || kind == WindowType::Generic9x4 as u16
            || kind == WindowType::Generic9x5 as u16
            || kind == WindowType::Generic9x6 as u16
            || kind == WindowType::ShulkerBox as u16
    ) {
        Some(0)
    } else if window_kind == WindowType::Crafting as u16 {
        Some(1)
    } else if window_kind == WindowType::Furnace as u16 {
        Some(2)
    } else if window_kind == WindowType::Enchantment as u16 {
        Some(3)
    } else if window_kind == WindowType::BrewingStand as u16 {
        Some(4)
    } else if window_kind == WindowType::Anvil as u16 {
        Some(5)
    } else if window_kind == WindowType::Hopper as u16 {
        Some(8)
    } else if window_kind == WindowType::Beacon as u16 {
        Some(13)
    } else if window_kind == WindowType::Merchant as u16 {
        Some(15)
    } else if window_kind == WindowType::Lectern as u16 {
        Some(25)
    } else if window_kind == WindowType::BlastFurnace as u16 {
        Some(27)
    } else if window_kind == WindowType::Smoker as u16 {
        Some(28)
    } else if window_kind == WindowType::Stonecutter as u16 {
        Some(29)
    } else if window_kind == WindowType::CartographyTable as u16 {
        Some(30)
    } else if window_kind == WindowType::Grindstone as u16 {
        Some(26)
    } else if window_kind == WindowType::Loom as u16 {
        Some(24)
    } else if window_kind == WindowType::Smithing as u16 {
        Some(33)
    } else if window_kind == WindowType::Crafter3x3 as u16 {
        Some(36)
    } else {
        None
    }
}

pub(crate) fn publish_cluster_screen_close(
    player: &crate::entity::player::Player,
    sync_id: u8,
    window_kind: u16,
) -> Result<(), InventoryApplyError> {
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(client) => {
            let data = client
                .serialize_packet(&CCloseContainer::new(sync_id.into()))
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            let container_type = bedrock_window_type(window_kind)
                .ok_or(InventoryApplyError::Rejected)?;
            let data = client
                .serialize_packet(&SContainerClose {
                    container_id: sync_id,
                    container_type,
                    server_initiated_close: true,
                })
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
    }
    Ok(())
}

pub(crate) fn publish_cluster_screen_open(
    server: &Server,
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    presentation: &super::cluster_invsee::ClusterScreenPresentation,
) -> Result<(), InventoryApplyError> {
    let player =
        find_player(server, screen.session.actor).ok_or(InventoryApplyError::TargetUnavailable)?;
    let window_type = presentation.window_type;
    let java_open = COpenScreen::new(
        screen.sync_id.into(),
        (window_type as i32).into(),
        &presentation.title,
    );
    let stacks = presentation
        .stacks
        .iter()
        .cloned()
        .map(ItemStackSerializer::from)
        .collect::<Vec<_>>();
    let cursor = ItemStackSerializer::from(stack_from_effect(&screen.cursor)?);
    let java_content =
        CSetContainerContent::new(VarInt(screen.sync_id.into()), VarInt(0), &stacks, &cursor);
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(client) => {
            let open = client
                .serialize_packet(&java_open)
                .map_err(|_| InventoryApplyError::Rejected)?;
            let content = client
                .serialize_packet(&java_content)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(open);
            client.try_enqueue_packet(content);
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::client::inventory_content::CInventoryContent;
            use pumpkin_protocol::bedrock::network_item::{
                ContainerName, FullContainerName, NetworkItemStackDescriptor,
            };
            use pumpkin_protocol::codec::var_uint::VarUInt;

            let bedrock_open = CContainerOpen {
                container_id: screen.sync_id,
                container_type: bedrock_window_type(window_type as u16)
                    .ok_or(InventoryApplyError::Rejected)?,
                position: BlockPos::ZERO,
                target_entity_id: VarLong(-1),
            };

            let container_stacks = presentation
                .stacks
                .get(..usize::from(screen.container_slots))
                .ok_or(InventoryApplyError::Rejected)?;
            let bedrock_content = CInventoryContent {
                container_id: VarUInt(u32::from(screen.sync_id)),
                slots: container_stacks
                    .iter()
                    .map(NetworkItemStackDescriptor::from)
                    .collect(),
                full_container_name: FullContainerName {
                    container_name: ContainerName::LevelEntity,
                    dynamic_id: None,
                },
                storage_item: NetworkItemStackDescriptor::default(),
            };
            let open = client
                .serialize_packet(&bedrock_open)
                .map_err(|_| InventoryApplyError::Rejected)?;
            let content = client
                .serialize_packet(&bedrock_content)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(open);
            client.try_enqueue_packet(content);
        }
    }
    Ok(())
}

pub(crate) fn publish_session_zero_content(
    player: &crate::entity::player::Player,
) -> Result<(), InventoryApplyError> {
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let screen = super::cluster_invsee::inventory_screen_snapshot(
        &server,
        pumpkin_cluster::inventory::InventoryScreenSession { actor, session: 0 },
    )
    .ok_or(InventoryApplyError::TargetUnavailable)?;
    let mut stacks = vec![
        ItemStackSerializer::from(ItemStack::EMPTY.clone());
        screen
            .slots
            .iter()
            .map(|slot| slot.screen_slot as usize)
            .max()
            .unwrap_or_default()
            + 1
    ];
    for slot in &screen.slots {
        stacks[slot.screen_slot as usize] =
            ItemStackSerializer::from(stack_from_effect(&slot.cell)?);
    }
    let cursor = ItemStackSerializer::from(stack_from_effect(&screen.cursor)?);
    let java_content = CSetContainerContent::new(VarInt(0), VarInt(0), &stacks, &cursor);
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(client) => {
            let data = client
                .serialize_packet(&java_content)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::client::inventory_content::CInventoryContent;
            use pumpkin_protocol::bedrock::network_item::{
                ContainerName, FullContainerName, NetworkItemStackDescriptor,
            };
            use pumpkin_protocol::codec::var_uint::VarUInt;

            let mut slots = Vec::with_capacity(36);
            for index in (36..45).chain(9..36) {
                let stack = stacks.get(index).ok_or(InventoryApplyError::Rejected)?;
                slots.push(NetworkItemStackDescriptor::from(&*stack.0));
            }
            let packet = CInventoryContent {
                container_id: VarUInt(0),
                slots,
                full_container_name: FullContainerName {
                    container_name: ContainerName::Inventory,
                    dynamic_id: None,
                },
                storage_item: NetworkItemStackDescriptor::default(),
            };
            let data = client
                .serialize_packet(&packet)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
    }
    Ok(())
}

pub(crate) fn resync_inventory_screen(player: &crate::entity::player::Player) {
    let (Some(server), Some(actor)) = (player.world().server.upgrade(), player.cluster_gid())
    else {
        return;
    };
    let session = pumpkin_cluster::inventory::InventoryScreenSession {
        actor,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
    };
    let Some(screen) = super::cluster_invsee::inventory_screen_snapshot(&server, session) else {
        return;
    };
    for slot in &screen.slots {
        if send_inventory_screen_slot(player, &screen, slot).is_err() {
            player.client.try_kick(
                crate::net::DisconnectReason::Unknown,
                &pumpkin_util::text::TextComponent::text("Inventory correction unavailable"),
            );
            return;
        }
    }
    if send_inventory_cursor(player, &screen.cursor).is_err() {
        player.client.try_kick(
            crate::net::DisconnectReason::Unknown,
            &pumpkin_util::text::TextComponent::text("Inventory correction unavailable"),
        );
    }
}

fn stack_from_effect(
    cell: &pumpkin_cluster::inventory::InventoryCell,
) -> Result<ItemStack, InventoryApplyError> {
    super::cluster_invsee::item_stack_from_inventory_cell(cell)
}

pub(crate) fn apply_inventory_effects(
    server: &Server,
    resolution: &InventoryResolution,
) -> Result<(), InventoryApplyError> {
    fn drop_owner(target: InventoryTarget) -> Option<GlobalPlayerId> {
        match target {
            InventoryTarget::Player(player) => Some(player),
            InventoryTarget::Virtual {
                owner: pumpkin_cluster::actor::ActorId::Player(player),
                ..
            } => Some(player),
            _ => None,
        }
    }

    if resolution.result.is_err() {
        return Ok(());
    }
    for effect in &resolution.effects {
        match effect {
            InventoryGestureEffect::Removed {
                source,
                stack,
                kind: InventoryRemovalKind::Drop,
            } => {
                let target =
                    drop_owner(source.target).ok_or(InventoryApplyError::TargetUnavailable)?;
                if target.server.0 != server.advanced_config.cluster.server_id {
                    continue;
                }
                let player =
                    find_player(server, target).ok_or(InventoryApplyError::TargetUnavailable)?;
                player.drop_item(stack_from_effect(stack)?);
            }
            InventoryGestureEffect::Removed { .. } | InventoryGestureEffect::Crafted { .. } => {}
            InventoryGestureEffect::CreatedDrop { owner, stack } => {
                let target = drop_owner(*owner).ok_or(InventoryApplyError::TargetUnavailable)?;
                if target.server.0 != server.advanced_config.cluster.server_id {
                    continue;
                }
                let player =
                    find_player(server, target).ok_or(InventoryApplyError::TargetUnavailable)?;
                player.drop_item(stack_from_effect(stack)?);
            }
        }
    }
    Ok(())
}

pub(crate) fn publish_inventory_screen_deltas(
    server: &Server,
    before: &InventoryScreenMap,
    after: &InventoryScreenMap,
) -> Result<(), InventoryApplyError> {
    for (session, current) in after {
        let Some(previous) = before.get(session) else {
            continue;
        };
        let Some(player) = find_player(server, session.actor) else {
            continue;
        };
        for slot in &current.slots {
            let changed = previous
                .slots
                .iter()
                .find(|old| old.screen_slot == slot.screen_slot)
                .is_none_or(|old| old.cell != slot.cell);
            if changed {
                send_inventory_screen_slot(&player, current, slot)?;
            }
        }
        if previous.cursor != current.cursor {
            send_inventory_cursor(&player, &current.cursor)?;
        }
    }
    Ok(())
}

pub(crate) fn publish_inventory_menu_effects(
    server: &Server,
    resolution: &InventoryResolution,
) -> Result<(), InventoryApplyError> {
    if resolution.result.is_err() || resolution.menu_effects.is_empty() {
        return Ok(());
    }
    let pumpkin_cluster::actor::ActorId::Player(actor) = resolution.action.actor else {
        return Ok(());
    };
    let Some(player) = find_player(server, actor) else {
        return Ok(());
    };
    let screen = current_screen_snapshot(&player)?;
    for effect in &resolution.menu_effects {
        use pumpkin_cluster::inventory::{InventoryContainerButton, InventoryMenuEffect};

        match effect {
            InventoryMenuEffect::AnvilNameChanged { level_cost, .. }
            | InventoryMenuEffect::AnvilResultTaken { level_cost, .. } => {
                send_java_menu_property(&player, screen.sync_id, 0, *level_cost)?;
            }
            InventoryMenuEffect::ContainerSelectionChanged { selection } => match selection {
                InventoryContainerButton::StonecutterRecipe { recipe } => {
                    send_java_menu_property(&player, screen.sync_id, 0, *recipe)?;
                }
                InventoryContainerButton::LoomPattern { pattern } => {
                    send_java_menu_property(&player, screen.sync_id, 0, u32::from(*pattern))?;
                }
                InventoryContainerButton::LecternPage { page } => {
                    send_java_menu_property(&player, screen.sync_id, 0, u32::from(*page))?;
                }
                InventoryContainerButton::Enchant { .. } => {}
            },
            InventoryMenuEffect::LoomPatternApplied { menu_index, .. } => {
                send_java_menu_property(&player, screen.sync_id, 0, u32::from(*menu_index))?;
            }
            InventoryMenuEffect::TradeSelected { merchant, .. }
            | InventoryMenuEffect::TradeCompleted { merchant, .. } => {
                send_trade_presentation(server, &player, screen.sync_id, merchant)?;
            }
            InventoryMenuEffect::BeaconConfigured { .. }
            | InventoryMenuEffect::GrindstoneResultTaken { .. }
            | InventoryMenuEffect::CraftCompleted { .. } => {}
        }
    }
    Ok(())
}

fn send_java_menu_property(
    player: &crate::entity::player::Player,
    sync_id: u8,
    property: i16,
    value: u32,
) -> Result<(), InventoryApplyError> {
    let value = i16::try_from(value).map_err(|_| InventoryApplyError::Rejected)?;
    if let crate::net::ClientPlatform::Java(client) = player.client.as_ref() {
        let packet = CSetContainerProperty::new(VarInt(i32::from(sync_id)), property, value);
        if let Ok(data) = client.serialize_packet(&packet) {
            client.try_enqueue_packet(data);
        }
    }
    Ok(())
}

fn merchant_offers(
    presentation: &pumpkin_cluster::inventory::InventoryMerchantPresentation,
) -> Result<Vec<MerchantOffer>, InventoryApplyError> {
    presentation
        .offers
        .iter()
        .map(|offer| {
            Ok(MerchantOffer {
                base_cost_a: ItemStackSerializer::from(stack_from_effect(&offer.first_cost)?),
                output: ItemStackSerializer::from(stack_from_effect(&offer.result)?),
                cost_b: offer
                    .second_cost
                    .as_ref()
                    .map(stack_from_effect)
                    .transpose()?
                    .filter(|stack| !stack.is_empty())
                    .map(ItemStackSerializer::from),
                reward_exp: offer.reward_experience,
                uses: i32::try_from(offer.uses).map_err(|_| InventoryApplyError::Rejected)?,
                max_uses: i32::try_from(offer.max_uses)
                    .map_err(|_| InventoryApplyError::Rejected)?,
                xp: i32::try_from(offer.merchant_experience)
                    .map_err(|_| InventoryApplyError::Rejected)?,
                special_price: offer.special_price,
                price_multiplier: f32::from_bits(offer.price_multiplier_bits),
                demand: offer.demand,
            })
        })
        .collect()
}

fn send_trade_presentation(
    server: &Server,
    player: &crate::entity::player::Player,
    sync_id: u8,
    presentation: &pumpkin_cluster::inventory::InventoryMerchantPresentation,
) -> Result<(), InventoryApplyError> {
    let offers = merchant_offers(presentation)?;
    if let crate::net::ClientPlatform::Java(client) = player.client.as_ref() {
        let packet = CMerchantOffers::new(
            VarInt(i32::from(sync_id)),
            offers,
            VarInt(i32::from(presentation.tier)),
            VarInt(
                i32::try_from(presentation.experience)
                    .map_err(|_| InventoryApplyError::Rejected)?,
            ),
            presentation.regular_villager,
            presentation.can_restock,
        );
        if let Ok(data) = client.serialize_packet(&packet) {
            client.try_enqueue_packet(data);
        }
        return Ok(());
    }
    send_bedrock_trade_presentation(server, player, sync_id, presentation, offers)
}

fn send_bedrock_trade_presentation(
    server: &Server,
    player: &crate::entity::player::Player,
    sync_id: u8,
    presentation: &pumpkin_cluster::inventory::InventoryMerchantPresentation,
    offers: Vec<MerchantOffer>,
) -> Result<(), InventoryApplyError> {
    use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};
    use pumpkin_protocol::{bedrock::client::CUpdateTrade, codec::var_long::VarLong};

    let world = player.world();
    let entity_id =
        super::cluster_entity_emit::entity_id_for_global(server, &world, presentation.merchant)
            .ok_or(InventoryApplyError::TargetUnavailable)?;
    let display_name = world
        .get_entity_by_id(entity_id)
        .map(|entity| entity.get_display_name().to_pretty_console())
        .ok_or(InventoryApplyError::TargetUnavailable)?;

    fn trade_item(stack: &ItemStack, count: u8) -> NbtCompound {
        use pumpkin_data::item::JavaToBedrockItemMapping;

        let mut item = NbtCompound::new();
        if !stack.is_empty()
            && let Some(mapping) = JavaToBedrockItemMapping::from_java_item_id(stack.item.id)
        {
            item.put_byte("Count", count as i8);
            item.put_short("Damage", mapping.bedrock_data as i16);
            item.put_string("Name", mapping.bedrock_item.registry_key.to_owned());
        }
        item
    }

    let tier = i32::from(presentation.tier).saturating_sub(1).max(0);
    let mut recipes = Vec::with_capacity(offers.len() + usize::from(presentation.tier < 5));
    for (index, offer) in offers.iter().enumerate() {
        let base = &offer.base_cost_a.0;
        let bonus = (f32::from(base.item_count) * offer.demand as f32 * offer.price_multiplier)
            .floor()
            .max(0.0) as i32;
        let adjusted = i32::from(base.item_count)
            .saturating_add(bonus)
            .saturating_add(offer.special_price)
            .clamp(1, i32::from(base.get_max_stack_size())) as u8;
        let mut recipe = NbtCompound::new();
        recipe.put_int(
            "netId",
            i32::try_from(index).map_err(|_| InventoryApplyError::Rejected)? + 1,
        );
        recipe.put_int(
            "maxUses",
            if offer.is_out_of_stock() {
                0
            } else {
                offer.max_uses
            },
        );
        recipe.put_int("traderExp", offer.xp);
        recipe.put_float("priceMultiplierA", offer.price_multiplier);
        recipe.put_float("priceMultiplierB", 0.0);
        recipe.put_compound(
            "sell",
            trade_item(&offer.output.0, offer.output.0.item_count),
        );
        recipe.put_int("buyCountA", i32::from(base.item_count));
        recipe.put_int(
            "buyCountB",
            offer
                .cost_b
                .as_ref()
                .map_or(0, |cost| i32::from(cost.0.item_count)),
        );
        recipe.put_int("demand", offer.demand);
        recipe.put_int(
            "tier",
            (i32::try_from(index).map_err(|_| InventoryApplyError::Rejected)? / 2).min(tier),
        );
        recipe.put_compound("buyA", trade_item(base, adjusted));
        recipe.put_compound(
            "buyB",
            offer.cost_b.as_ref().map_or_else(NbtCompound::new, |cost| {
                trade_item(&cost.0, cost.0.item_count)
            }),
        );
        recipe.put_int("uses", offer.uses);
        recipe.put_byte("rewardExp", i8::from(offer.reward_exp));
        recipes.push(NbtTag::Compound(recipe));
    }
    if presentation.tier < 5 {
        let mut next = NbtCompound::new();
        next.put_int("maxUses", 0);
        next.put_int("traderExp", 0);
        next.put_float("priceMultiplierA", 0.0);
        next.put_float("priceMultiplierB", 0.0);
        next.put_int("buyCountA", 0);
        next.put_int("buyCountB", 0);
        next.put_int("demand", 0);
        next.put_int("tier", 5);
        next.put_int("uses", 0);
        next.put_byte("rewardExp", 0);
        recipes.push(NbtTag::Compound(next));
    }
    let mut data = NbtCompound::new();
    data.put_list("Recipes", recipes);
    let packet = CUpdateTrade {
        container_id: sync_id,
        r#type: 15,
        size: VarInt(0),
        trader_tier: VarInt(tier),
        entity_unique_id: VarLong(i64::from(entity_id)),
        last_trading_player: VarLong(i64::from(player.entity_id())),
        display_name,
        use_new_trade_screen: presentation.new_trading_ui,
        using_economy_trade: presentation.economy_trade,
        data,
    };
    if let crate::net::ClientPlatform::Bedrock(client) = player.client.as_ref() {
        client.try_enqueue_client_packet(&packet);
    }
    Ok(())
}

fn send_inventory_screen_slot(
    player: &crate::entity::player::Player,
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    slot: &pumpkin_cluster::inventory::InventoryScreenSlot,
) -> Result<(), InventoryApplyError> {
    let stack = stack_from_effect(&slot.cell)?;
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(client) => {
            let serializer = ItemStackSerializer::from(stack);
            let packet = CSetContainerSlot::new(
                screen.sync_id as i8,
                0,
                slot.screen_slot as i16,
                &serializer,
            );
            let data = client
                .serialize_packet(&packet)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::client::inventory_slot::CInventorySlot;
            use pumpkin_protocol::bedrock::network_item::{
                FullContainerName, NetworkItemStackDescriptor,
            };
            use pumpkin_protocol::codec::var_uint::VarUInt;

            let (container_name, bedrock_slot) = bedrock_screen_address(screen, slot.screen_slot)
                .ok_or(InventoryApplyError::Rejected)?;
            let packet = CInventorySlot {
                container_id: VarUInt(u32::from(screen.sync_id)),
                slot: VarUInt(u32::from(bedrock_slot)),
                full_container_name: Some(FullContainerName {
                    container_name,
                    dynamic_id: None,
                }),
                storage_item: None,
                item: NetworkItemStackDescriptor::from(&stack),
            };
            let data = client
                .serialize_packet(&packet)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
    }
    Ok(())
}

fn send_inventory_cursor(
    player: &crate::entity::player::Player,
    cell: &pumpkin_cluster::inventory::InventoryCell,
) -> Result<(), InventoryApplyError> {
    let stack = stack_from_effect(cell)?;
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(client) => {
            let serializer = ItemStackSerializer::from(stack);
            let data = client
                .serialize_packet(&CSetCursorItem::new(&serializer))
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::client::inventory_content::CInventoryContent;
            use pumpkin_protocol::bedrock::network_item::{
                ContainerName, FullContainerName, NetworkItemStackDescriptor,
            };
            use pumpkin_protocol::codec::var_uint::VarUInt;

            let packet = CInventoryContent {
                container_id: VarUInt(59),
                slots: vec![NetworkItemStackDescriptor::from(&stack)],
                full_container_name: FullContainerName {
                    container_name: ContainerName::Cursor,
                    dynamic_id: None,
                },
                storage_item: NetworkItemStackDescriptor::default(),
            };
            let data = client
                .serialize_packet(&packet)
                .map_err(|_| InventoryApplyError::Rejected)?;
            client.try_enqueue_packet(data);
        }
    }
    Ok(())
}

fn bedrock_screen_address(
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    screen_slot: u16,
) -> Option<(pumpkin_protocol::bedrock::network_item::ContainerName, u8)> {
    use pumpkin_protocol::bedrock::network_item::ContainerName;

    if screen.sync_id == 0 {
        return match screen_slot {
            0 => Some((ContainerName::CreatedOutput, 0)),
            1..=4 => Some((ContainerName::CraftingInput, (screen_slot + 27) as u8)),
            5..=8 => Some((ContainerName::Armor, (screen_slot - 5) as u8)),
            9..=35 => Some((ContainerName::Inventory, screen_slot as u8)),
            36..=44 => Some((ContainerName::Inventory, (screen_slot - 36) as u8)),
            45 => Some((ContainerName::Offhand, 0)),
            _ => None,
        };
    }
    if screen_slot < screen.container_slots {
        use pumpkin_data::screen::WindowType;
        if screen.window_kind == pumpkin_data::screen::WindowType::Crafting as u16 {
            return if screen_slot == 0 {
                Some((ContainerName::CreatedOutput, 0))
            } else {
                Some((ContainerName::CraftingInput, (32 + screen_slot - 1) as u8))
            };
        }
        let named = if screen.window_kind == WindowType::Anvil as u16 {
            [
                ContainerName::AnvilInput,
                ContainerName::AnvilMaterial,
                ContainerName::AnvilResultPreview,
            ]
            .get(usize::from(screen_slot))
            .copied()
            .map(|name| (name, 0))
        } else if screen.window_kind == WindowType::Beacon as u16 && screen_slot == 0 {
            Some((ContainerName::BeaconPayment, 0))
        } else if screen.window_kind == WindowType::Loom as u16 {
            [
                ContainerName::LoomInput,
                ContainerName::LoomDye,
                ContainerName::LoomMaterial,
                ContainerName::LoomResultPreview,
            ]
            .get(usize::from(screen_slot))
            .copied()
            .map(|name| (name, 0))
        } else if screen.window_kind == WindowType::Grindstone as u16 {
            [
                ContainerName::GrindstoneInput,
                ContainerName::GrindstoneAdditional,
                ContainerName::GrindstoneResultPreview,
            ]
            .get(usize::from(screen_slot))
            .copied()
            .map(|name| (name, 0))
        } else if screen.window_kind == WindowType::Merchant as u16 {
            [
                (ContainerName::Trade2Ingredient1, 4),
                (ContainerName::Trade2Ingredient2, 5),
                (ContainerName::Trade2ResultPreview, 50),
            ]
            .get(usize::from(screen_slot))
            .copied()
        } else {
            None
        };
        if let Some(address) = named {
            return Some(address);
        }
        return Some((ContainerName::LevelEntity, screen_slot as u8));
    }
    let player_slot = screen_slot - screen.container_slots;
    match player_slot {
        0..=26 => Some((ContainerName::Inventory, (player_slot + 9) as u8)),
        27..=35 => Some((ContainerName::Inventory, (player_slot - 27) as u8)),
        _ => None,
    }
}

pub(crate) fn publish_inventory_origin(server: &Server, publication: &InventoryPublication) {
    let Some(origin) = publication.origin else {
        return;
    };
    let pumpkin_cluster::actor::ActorId::Player(action_actor) = publication.resolution.action.actor
    else {
        return;
    };
    if is_java_click_origin(origin) {
        return;
    }
    if origin & ORIGIN_TAG_MASK == JAVA_HELD_TAG {
        if let Some(player) = find_player(server, action_actor)
            && let Some(selected) = authoritative_selected_slot(publication)
        {
            player.try_send_selected_slot(selected);
        }
        return;
    }
    if origin & ORIGIN_TAG_MASK == BEDROCK_HELD_TAG {
        let Some(player) = find_player(server, action_actor) else {
            return;
        };
        let Some(snapshot) = publication
            .resolution
            .authoritative
            .iter()
            .find(|snapshot| snapshot.target == InventoryTarget::Player(action_actor))
        else {
            return;
        };
        let selected = selected_slot_from_snapshot(snapshot);
        if let (Some(client), Some(selected)) = (player.client.bedrock(), selected) {
            client.try_enqueue_client_packet(&pumpkin_protocol::bedrock::client::CPlayerHotbar {
                selected_slot: pumpkin_protocol::codec::var_uint::VarUInt(u32::from(selected)),
                container_id: 0,
                should_select_slot: true,
            });
        }
        return;
    }
    let Some(request_id) = bedrock_stack_request_id(origin) else {
        return;
    };
    let Some(player) = find_player(server, action_actor) else {
        return;
    };
    let Some(client) = player.client.bedrock() else {
        return;
    };
    let containers = bedrock_response_containers(server, &player, publication);
    client.try_enqueue_client_packet(&CItemStackResponse {
        responses: vec![ItemStackResponseInfo {
            result: u8::from(publication.resolution.result.is_err()),
            client_request_id: VarInt(request_id),
            containers,
        }],
    });
}

fn authoritative_selected_slot(publication: &InventoryPublication) -> Option<u8> {
    let pumpkin_cluster::actor::ActorId::Player(actor) = publication.resolution.action.actor else {
        return None;
    };
    publication
        .resolution
        .authoritative
        .iter()
        .find(|snapshot| snapshot.target == InventoryTarget::Player(actor))
        .and_then(selected_slot_from_snapshot)
}

fn selected_slot_from_snapshot(
    snapshot: &pumpkin_cluster::inventory::InventoryStateSnapshot,
) -> Option<u8> {
    snapshot.cells.iter().find_map(|cell| {
        (cell.loc.inv == pumpkin_cluster::inventory::INV_SELECTED && cell.loc.slot == 0)
            .then(|| cell.item.checked_sub(1))
            .flatten()
            .and_then(|slot| u8::try_from(slot).ok())
    })
}

fn bedrock_response_containers(
    server: &Server,
    player: &crate::entity::player::Player,
    publication: &InventoryPublication,
) -> Vec<ItemStackResponseContainerInfo> {
    use pumpkin_protocol::bedrock::network_item::{ContainerName, FullContainerName};

    let Some(actor) = player.cluster_gid() else {
        return Vec::new();
    };
    let session = pumpkin_cluster::inventory::InventoryScreenSession {
        actor,
        session: player
            .cluster_current_inventory_session
            .load(Ordering::Acquire),
    };
    let Some(screen) = super::cluster_invsee::inventory_screen_snapshot(server, session) else {
        return Vec::new();
    };
    let mut touched = Vec::new();
    if let Ok(delta) = &publication.resolution.result {
        touched.extend(
            delta
                .operations
                .iter()
                .map(|operation| InventorySlotRef::new(operation.target, operation.after.loc)),
        );
    }
    touched.extend(
        publication
            .resolution
            .correction
            .operations
            .iter()
            .map(|operation| InventorySlotRef::new(operation.target, operation.after.loc)),
    );
    touched.sort_unstable_by_key(|slot| (slot.target, slot.loc.inv, slot.loc.slot));
    touched.dedup();
    let mut containers: Vec<ItemStackResponseContainerInfo> = Vec::new();
    for entry in screen
        .slots
        .iter()
        .filter(|entry| touched.contains(&entry.slot))
    {
        let Some((container_name, slot_id)) = bedrock_screen_address(&screen, entry.screen_slot)
        else {
            continue;
        };
        let stack = stack_from_effect(&entry.cell).unwrap_or_else(|_| ItemStack::EMPTY.clone());
        let slot = ItemStackResponseSlotInfo {
            requested_slot: slot_id,
            slot: slot_id,
            amount: entry.cell.count,
            item_stack_net_id: VarInt(stack.uid.get()),
            custom_name: String::new(),
            filtered_custom_name: String::new(),
            durability_correction: VarInt(0),
        };
        if let Some(container) = containers
            .iter_mut()
            .find(|container| container.full_container_name.container_name == container_name)
        {
            container.slots.push(slot);
        } else {
            containers.push(ItemStackResponseContainerInfo {
                full_container_name: FullContainerName {
                    container_name,
                    dynamic_id: None,
                },
                slots: vec![slot],
            });
        }
    }
    if touched.iter().any(|slot| slot.loc.inv == INV_CURSOR) {
        let stack = stack_from_effect(&screen.cursor).unwrap_or_else(|_| ItemStack::EMPTY.clone());
        containers.push(ItemStackResponseContainerInfo {
            full_container_name: FullContainerName {
                container_name: ContainerName::Cursor,
                dynamic_id: None,
            },
            slots: vec![ItemStackResponseSlotInfo {
                requested_slot: 0,
                slot: 0,
                amount: screen.cursor.count,
                item_stack_net_id: VarInt(stack.uid.get()),
                custom_name: String::new(),
                filtered_custom_name: String::new(),
                durability_correction: VarInt(0),
            }],
        });
    }
    containers
}
