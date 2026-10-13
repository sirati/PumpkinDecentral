use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;

use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::invsee::{
    INVSEE_MAX_SLOT_NBT_BYTES, InventorySnapshot, InvseeSlot, InvseeWrite,
};
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_data::screen::WindowType;
use pumpkin_data::translation;
use pumpkin_inventory::inventory::{Inventory, SimpleInventory};
use pumpkin_inventory::player::player_inventory::PlayerInventory;
use pumpkin_inventory::screen_handler::{
    InventoryPlayer, ScreenHandler, ScreenHandlerBehaviour, ScreenHandlerFactory,
    SharedScreenHandler,
};
use pumpkin_inventory::slot::{ArmorSlot, NormalSlot, Slot};
use pumpkin_nbt::Nbt;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command, literal};
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::argument_types::entity_selector::EntitySelector;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::entity::player::Player;
use crate::server::cluster_invsee;

const DESCRIPTION: &str = "Opens another player's inventory for viewing.";
const EDIT_DESCRIPTION: &str = "Opens another player's inventory for editing.";
const PERMISSION: &str = "minecraft:command.invsee";
const EDIT_PERMISSION: &str = "minecraft:command.invsee.edit";

const ERROR_NOT_PLAYER: CommandErrorType<0> = CommandErrorType::new(
    translation::java::PERMISSIONS_REQUIRES_PLAYER,
    translation::java::PERMISSIONS_REQUIRES_PLAYER,
);

const CONTAINER_SIZE: usize = 45;
const ARMOR_COUNT: usize = 4;
const ARMOR_SLOTS: [usize; ARMOR_COUNT] = [39, 38, 37, 36];
const ARMOR_EQUIPMENT: [EquipmentSlot; ARMOR_COUNT] = [
    EquipmentSlot::HEAD,
    EquipmentSlot::CHEST,
    EquipmentSlot::LEGS,
    EquipmentSlot::FEET,
];
const FILLER_SLOTS: [usize; 4] = [4, 5, 6, 7];
const OFFHAND_CONTAINER: usize = 8;
const PLAYER_OFFHAND: usize = 40;
const STORAGE_START: usize = 9;
const STORAGE_END: usize = 36;
const HOTBAR_START: usize = 36;
const HOTBAR_END: usize = 45;
const HOTBAR_COUNT: usize = 9;

pub(crate) fn container_to_player(container: usize) -> Option<usize> {
    if container < ARMOR_COUNT {
        return Some(ARMOR_SLOTS[container]);
    }
    if container == OFFHAND_CONTAINER {
        return Some(PLAYER_OFFHAND);
    }
    if (STORAGE_START..STORAGE_END).contains(&container) {
        return Some(container);
    }
    if (HOTBAR_START..HOTBAR_END).contains(&container) {
        return Some(container - HOTBAR_START);
    }
    None
}

pub(crate) fn container_to_inventory_loc(
    container: usize,
) -> Option<pumpkin_cluster::inventory::InvLoc> {
    let player_slot = container_to_player(container)?;
    use pumpkin_cluster::inventory::{INV_ARMOR, INV_MAIN, INV_OFFHAND, InvLoc};
    match player_slot {
        0..=35 => Some(InvLoc::new(INV_MAIN, player_slot as u16)),
        36..=39 => Some(InvLoc::new(INV_ARMOR, (player_slot - 36) as u16)),
        40 => Some(InvLoc::new(INV_OFFHAND, 0)),
        _ => None,
    }
}

fn player_to_container(player_index: usize) -> Option<usize> {
    if player_index == PLAYER_OFFHAND {
        return Some(OFFHAND_CONTAINER);
    }
    if (STORAGE_START..STORAGE_END).contains(&player_index) {
        return Some(player_index);
    }
    if player_index < HOTBAR_COUNT {
        return Some(HOTBAR_START + player_index);
    }
    ARMOR_SLOTS.iter().position(|slot| *slot == player_index)
}

fn filler_stack() -> ItemStack {
    let mut stack = ItemStack::new(1, &Item::GRAY_STAINED_GLASS_PANE);
    stack.set_custom_name(String::new());
    stack
}

fn encode_stack(index: u16, stack: &ItemStack) -> Option<InvseeSlot> {
    if stack.is_empty() {
        return None;
    }
    let mut compound = pumpkin_nbt::compound::NbtCompound::new();
    let mut template = stack.clone();
    template.item_count = 1;
    template.write_item_stack(&mut compound);
    let bytes = Nbt::new(String::new(), compound)
        .write_unnamed_canonical()
        .ok()?
        .to_vec();
    if bytes.len() > INVSEE_MAX_SLOT_NBT_BYTES {
        return None;
    }
    Some(InvseeSlot::new(
        index,
        stack.item.id,
        stack.item_count,
        bytes,
    ))
}

fn decode_stack(slot: &InvseeSlot) -> ItemStack {
    if slot.nbt.is_empty() || slot.count == 0 {
        return ItemStack::EMPTY.clone();
    }
    let mut cursor = Cursor::new(slot.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let Ok(nbt) = Nbt::read_unnamed(&mut reader) else {
        return ItemStack::EMPTY.clone();
    };
    if cursor.position() != slot.nbt.len() as u64
        || nbt.clone().write_unnamed_canonical().ok().as_deref() != Some(slot.nbt.as_slice())
    {
        return ItemStack::EMPTY.clone();
    }
    let mut stack =
        ItemStack::read_item_stack(&nbt.root_tag).unwrap_or_else(|| ItemStack::EMPTY.clone());
    if stack.item.id != slot.item_id || stack.is_empty() {
        return ItemStack::EMPTY.clone();
    }
    stack.item_count = slot.count;
    stack
}

#[cfg(test)]
mod invsee_stack_tests {
    use super::*;

    #[test]
    fn changing_stack_count_keeps_item_identity_and_restores_display_count() {
        let five = ItemStack::new(5, &Item::STONE);
        let two = ItemStack::new(2, &Item::STONE);
        let encoded_five = encode_stack(9, &five).expect("five items");
        let encoded_two = encode_stack(9, &two).expect("two items");
        assert_eq!(encoded_five.nbt, encoded_two.nbt);
        assert_eq!(decode_stack(&encoded_five).item_count, 5);
        assert_eq!(decode_stack(&encoded_two).item_count, 2);
    }
}

pub struct RemoteEditSink {
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub server: Arc<crate::server::Server>,
}

struct LockedSlot {
    inventory: Arc<dyn Inventory>,
    index: usize,
    id: AtomicU8,
}

impl LockedSlot {
    fn new(inventory: Arc<dyn Inventory>, index: usize) -> Self {
        Self {
            inventory,
            index,
            id: AtomicU8::new(0),
        }
    }
}

impl Slot for LockedSlot {
    fn get_inventory(&self) -> Arc<dyn Inventory> {
        self.inventory.clone()
    }

    fn get_index(&self) -> usize {
        self.index
    }

    fn set_id(&self, index: usize) {
        self.id
            .store(index as u8, std::sync::atomic::Ordering::Relaxed);
    }

    fn can_insert(&self, _stack: &ItemStack) -> bool {
        false
    }

    fn can_take_items(&self, _player: &dyn InventoryPlayer) -> bool {
        false
    }

    fn mark_dirty(&self) {}
}

enum CloseAction {
    Detached,
    LocalEdit {
        server: Arc<crate::server::Server>,
        viewer: GlobalPlayerId,
        target_gid: GlobalPlayerId,
        baseline: InventorySnapshot,
    },
    RemoteEdit {
        sink: RemoteEditSink,
        baseline: InventorySnapshot,
    },
}

pub struct InvseeScreenHandler {
    behaviour: ScreenHandlerBehaviour,
    container: Arc<dyn Inventory>,
    editable: bool,
    close_action: CloseAction,
}

impl InvseeScreenHandler {
    fn push_container_slots(&mut self, editable: bool) {
        for container in 0..ARMOR_COUNT {
            if editable {
                self.add_slot(Arc::new(ArmorSlot::new(
                    self.container.clone(),
                    container,
                    ARMOR_EQUIPMENT[container].clone(),
                )));
            } else {
                self.add_slot(Arc::new(LockedSlot::new(self.container.clone(), container)));
            }
        }
        for container in FILLER_SLOTS {
            self.add_slot(Arc::new(LockedSlot::new(self.container.clone(), container)));
        }
        if editable {
            self.add_slot(Arc::new(NormalSlot::new(
                self.container.clone(),
                OFFHAND_CONTAINER,
            )));
        } else {
            self.add_slot(Arc::new(LockedSlot::new(
                self.container.clone(),
                OFFHAND_CONTAINER,
            )));
        }
        for container in STORAGE_START..CONTAINER_SIZE {
            if editable {
                self.add_slot(Arc::new(NormalSlot::new(self.container.clone(), container)));
            } else {
                self.add_slot(Arc::new(LockedSlot::new(self.container.clone(), container)));
            }
        }
    }

    fn collect_slots(&self) -> Vec<InvseeSlot> {
        let mut slots = Vec::new();
        for container in 0..CONTAINER_SIZE {
            if let Some(player_index) = container_to_player(container) {
                let stack = self.container.get_stack(container);
                if let Some(slot) = encode_stack(player_index as u16, &stack) {
                    slots.push(slot);
                }
            }
        }
        slots
    }

    fn return_cursor_to_target(&mut self) {
        let mut cursor = self
            .behaviour
            .cursor_stack
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cursor.is_empty() {
            return;
        }
        for container in 0..CONTAINER_SIZE {
            if container_to_player(container).is_none() {
                continue;
            }
            let stack = self.container.get_stack(container);
            if stack.is_empty() {
                self.container.set_stack(container, cursor.clone());
                *cursor = ItemStack::EMPTY.clone();
                return;
            }
            if ItemStack::are_items_and_components_equal(&stack, &cursor) {
                let room = stack.get_max_stack_size().saturating_sub(stack.item_count);
                if room >= cursor.item_count {
                    let mut merged = stack;
                    merged.increment(cursor.item_count);
                    self.container.set_stack(container, merged);
                    *cursor = ItemStack::EMPTY.clone();
                    return;
                }
            }
        }
    }
}

impl ScreenHandler for InvseeScreenHandler {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_behaviour(&self) -> &ScreenHandlerBehaviour {
        &self.behaviour
    }

    fn get_behaviour_mut(&mut self) -> &mut ScreenHandlerBehaviour {
        &mut self.behaviour
    }

    fn on_closed(&mut self, player: &dyn InventoryPlayer) {
        if self.editable {
            self.return_cursor_to_target();
        } else {
            self.default_on_closed(player);
        }
        match &self.close_action {
            CloseAction::Detached => {}
            CloseAction::LocalEdit {
                server,
                viewer,
                target_gid,
                baseline,
            } => {
                if let Ok(write) = InvseeWrite::replacement(*viewer, baseline, self.collect_slots())
                {
                    debug_assert_eq!(write.target, *target_gid);
                    cluster_invsee::submit_remote_write(server, write);
                }
            }
            CloseAction::RemoteEdit { sink, baseline } => {
                if let Ok(write) =
                    InvseeWrite::replacement(sink.viewer, baseline, self.collect_slots())
                {
                    debug_assert_eq!(write.target, sink.target);
                    cluster_invsee::submit_remote_write(&sink.server, write);
                }
            }
        }
    }

    fn quick_move(&mut self, _player: &dyn InventoryPlayer, slot_index: i32) -> ItemStack {
        if !self.editable {
            return ItemStack::EMPTY.clone();
        }
        if slot_index < 0 || slot_index as usize >= self.get_behaviour().slots.len() {
            return ItemStack::EMPTY.clone();
        }
        let slot = self.get_behaviour().slots[slot_index as usize].clone();
        if !slot.has_stack() {
            return ItemStack::EMPTY.clone();
        }
        let mut slot_stack = slot.get_stack();
        let moved = slot_stack.clone();
        if slot_index < CONTAINER_SIZE as i32 {
            if !self.insert_item(
                &mut slot_stack,
                CONTAINER_SIZE as i32,
                self.get_behaviour().slots.len() as i32,
                true,
            ) {
                return ItemStack::EMPTY.clone();
            }
        } else if !self.insert_item(&mut slot_stack, 0, CONTAINER_SIZE as i32, false) {
            return ItemStack::EMPTY.clone();
        }
        if slot_stack.is_empty() {
            slot.set_stack(ItemStack::EMPTY.clone());
        } else {
            slot.set_stack(slot_stack);
        }
        moved
    }
}

struct InvseeScreenFactory {
    title: TextComponent,
    container: Arc<dyn Inventory>,
    editable: bool,
}

impl ScreenHandlerFactory for InvseeScreenFactory {
    fn create_screen_handler(
        &self,
        sync_id: u8,
        player_inventory: &Arc<PlayerInventory>,
        _player: &dyn InventoryPlayer,
    ) -> Option<SharedScreenHandler> {
        let mut handler = InvseeScreenHandler {
            behaviour: ScreenHandlerBehaviour::new(sync_id, Some(WindowType::Generic9x5)),
            container: self.container.clone(),
            editable: self.editable,
            close_action: CloseAction::Detached,
        };
        handler.push_container_slots(self.editable);
        let viewer_inventory: Arc<dyn Inventory> = player_inventory.clone();
        for index in 9..PlayerInventory::MAIN_SIZE {
            handler.add_slot(Arc::new(LockedSlot::new(viewer_inventory.clone(), index)));
        }
        for index in 0..HOTBAR_COUNT {
            handler.add_slot(Arc::new(LockedSlot::new(viewer_inventory.clone(), index)));
        }
        Some(Arc::new(Mutex::new(handler)) as SharedScreenHandler)
    }

    fn get_display_name(&self) -> TextComponent {
        self.title.clone()
    }
}

fn fill_from_snapshot(container: &SimpleInventory, snapshot: &InventorySnapshot) {
    for filler in FILLER_SLOTS {
        container.set_stack(filler, filler_stack());
    }
    for slot in &snapshot.slots {
        if let Some(container_index) = player_to_container(usize::from(slot.index)) {
            if container_index < CONTAINER_SIZE {
                container.set_stack(container_index, decode_stack(slot));
            }
        }
    }
}

fn set_close_action(viewer: &Player, action: CloseAction) {
    let current = viewer
        .current_screen_handler
        .try_lock()
        .ok()
        .map(|guard| guard.clone());
    if let Some(current) = current {
        if let Ok(mut concrete) = current.try_lock() {
            if let Some(invsee) = concrete.as_any_mut().downcast_mut::<InvseeScreenHandler>() {
                invsee.close_action = action;
            }
        }
    }
}

fn local_gid(player: &Player, server_id: u16) -> GlobalPlayerId {
    player
        .cluster_gid()
        .unwrap_or(GlobalPlayerId::new(ServerId(server_id), PlayerSlot(0)))
}

pub fn open_remote_view(
    viewer: &Arc<Player>,
    snapshot: &InventorySnapshot,
    editable: bool,
    sink: Option<RemoteEditSink>,
) -> bool {
    if viewer
        .cluster_departing
        .load(std::sync::atomic::Ordering::Acquire)
        || viewer.client.closed()
    {
        return false;
    }
    let Some(&target_chunk) = snapshot.affected.first() else {
        return false;
    };
    if viewer
        .world()
        .server
        .upgrade()
        .is_some_and(|server| server.advanced_config.cluster.enabled)
    {
        return open_cluster_invsee_view(viewer, snapshot, editable, target_chunk).is_ok();
    }
    let container = Arc::new(SimpleInventory::new(CONTAINER_SIZE));
    fill_from_snapshot(&container, snapshot);
    let factory = InvseeScreenFactory {
        title: TextComponent::text(snapshot.owner_name.clone()),
        container: container.clone(),
        editable,
    };
    if viewer
        .open_handled_screen(
            &factory,
            crate::entity::player::InventoryScreenSource::PlayerAt {
                target: snapshot.target,
                chunk: target_chunk,
                revision: snapshot.revision,
            },
        )
        .is_some()
    {
        match sink {
            Some(sink) => set_close_action(
                viewer,
                CloseAction::RemoteEdit {
                    sink,
                    baseline: snapshot.clone(),
                },
            ),
            None => set_close_action(viewer, CloseAction::Detached),
        }
        true
    } else {
        false
    }
}

fn open_cluster_invsee_view(
    viewer: &Arc<Player>,
    snapshot: &InventorySnapshot,
    editable: bool,
    target_chunk: pumpkin_cluster::protocol::ChunkKey,
) -> Result<(), pumpkin_cluster::inventory::InventoryApplyError> {
    use pumpkin_cluster::inventory::{
        INV_CURSOR, INV_SELECTED, InvLoc, InventoryCell, InventoryScreenSession,
        InventoryScreenSlot, InventoryScreenSnapshot, InventorySlotRef, InventoryStateSnapshot,
        InventoryTarget,
    };

    if snapshot.incarnation == 0
        || snapshot.lease.is_none_or(|lease| {
            lease.gid != snapshot.target
                || lease.incarnation != snapshot.incarnation
                || lease.incarnation == 0
        })
        || snapshot.authority.as_ref().is_none_or(|authority| {
            snapshot
                .lease
                .is_none_or(|lease| !authority.is_valid(lease, &snapshot.affected))
        })
    {
        return Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected);
    }

    let server = viewer
        .world()
        .server
        .upgrade()
        .ok_or(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable)?;
    let viewer_gid = viewer
        .cluster_gid()
        .ok_or(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable)?;
    let viewer_target = InventoryTarget::Player(viewer_gid);
    let viewer_state = cluster_invsee::inventory_snapshot(&server, viewer_target)
        .ok_or(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable)?;
    let target = InventoryTarget::Player(snapshot.target);
    let mut target_cells = snapshot
        .slots
        .iter()
        .map(|slot| {
            Ok(InventoryCell {
                loc: pumpkin_cluster::invsee::player_inventory_loc(slot.index)
                    .ok_or(pumpkin_cluster::inventory::InventoryApplyError::Rejected)?,
                item: slot.item_id,
                count: slot.count,
                nbt: slot.nbt.clone(),
            })
        })
        .collect::<Result<Vec<_>, pumpkin_cluster::inventory::InventoryApplyError>>()?;
    target_cells.push(InventoryCell {
        loc: InvLoc::new(INV_SELECTED, 0),
        item: u16::from(snapshot.selected) + 1,
        count: 1,
        nbt: Vec::new(),
    });
    let preview = InventoryStateSnapshot {
        target,
        revision: snapshot.revision,
        cells: target_cells,
    };
    cluster_invsee::validate_imported_inventory_snapshots(std::slice::from_ref(&preview))?;
    viewer.increment_screen_handler_sync_id();
    let sync_id = viewer
        .screen_handler_sync_id
        .load(std::sync::atomic::Ordering::Relaxed);
    let session_token = server
        .cluster_inventory_close_token
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let session = InventoryScreenSession {
        actor: viewer_gid,
        session: session_token,
    };
    let mut slots = Vec::with_capacity(CONTAINER_SIZE + 36 - FILLER_SLOTS.len());
    let mut stacks = vec![ItemStack::EMPTY.clone(); CONTAINER_SIZE + 36];
    for screen_slot in 0..CONTAINER_SIZE {
        if FILLER_SLOTS.contains(&screen_slot) {
            stacks[screen_slot] = filler_stack();
            continue;
        }
        let loc = container_to_inventory_loc(screen_slot)
            .ok_or(pumpkin_cluster::inventory::InventoryApplyError::Rejected)?;
        let cell = preview
            .cells
            .iter()
            .find(|cell| cell.loc == loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(loc));
        stacks[screen_slot] = cluster_invsee::item_stack_from_inventory_cell(&cell)?;
        slots.push(InventoryScreenSlot {
            screen_slot: screen_slot as u16,
            slot: InventorySlotRef::new(target, loc),
            cell,
            limit: if !editable {
                0
            } else if screen_slot < ARMOR_COUNT {
                1
            } else {
                64
            },
        });
    }
    for index in 0..36 {
        let screen_slot = CONTAINER_SIZE + index;
        let player_slot = if index < 27 { index + 9 } else { index - 27 };
        let loc = InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, player_slot as u16);
        let cell = viewer_state
            .cells
            .iter()
            .find(|cell| cell.loc == loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(loc));
        stacks[screen_slot] = cluster_invsee::item_stack_from_inventory_cell(&cell)?;
        slots.push(InventoryScreenSlot {
            screen_slot: screen_slot as u16,
            slot: InventorySlotRef::new(viewer_target, loc),
            cell,
            limit: if editable { 64 } else { 0 },
        });
    }
    let cursor_loc = InvLoc::new(INV_CURSOR, 0);
    let cursor = viewer_state
        .cells
        .iter()
        .find(|cell| cell.loc == cursor_loc)
        .cloned()
        .unwrap_or_else(|| InventoryCell::empty(cursor_loc));
    let position = viewer.position();
    let viewer_chunk = pumpkin_cluster::world_delta::chunk_of_block(
        crate::server::cluster_world_delta::world_id(&viewer.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    );
    let mut affected = vec![viewer_chunk, target_chunk];
    affected.sort_unstable();
    affected.dedup();
    let screen = InventoryScreenSnapshot {
        session,
        sync_id,
        state_id: 0,
        window_kind: WindowType::Generic9x5 as u16,
        container_slots: CONTAINER_SIZE as u16,
        slots,
        cursor,
        affected,
        preview: Some(preview),
        preview_incarnation: Some(snapshot.incarnation),
        preview_lease: snapshot.lease,
        preview_authority: snapshot.authority.clone(),
    };
    viewer
        .cluster_current_inventory_session
        .compare_exchange(
            0,
            session_token,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .map_err(|_| pumpkin_cluster::inventory::InventoryApplyError::Rejected)?;
    if viewer
        .cluster_departing
        .load(std::sync::atomic::Ordering::Acquire)
        || viewer.client.closed()
    {
        let _ = viewer.cluster_current_inventory_session.compare_exchange(
            session_token,
            0,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
        return Err(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable);
    }
    viewer
        .cluster_inventory_screen_target
        .store(Some(Arc::new(target)));
    if viewer
        .cluster_departing
        .load(std::sync::atomic::Ordering::Acquire)
        || viewer.client.closed()
    {
        viewer.cluster_inventory_screen_target.store(None);
        let _ = viewer.cluster_current_inventory_session.compare_exchange(
            session_token,
            0,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
        return Err(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable);
    }
    let submitted = cluster_invsee::open_inventory_screen_for_lease(
        &server,
        crate::server::cluster_world_owner::player_action_lease(&server, viewer).ok_or(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable)?,
        screen,
        None,
        Some(cluster_invsee::ClusterScreenPresentation {
            title: TextComponent::text(snapshot.owner_name.clone()),
            window_type: WindowType::Generic9x5,
            stacks,
        }),
    );
    if submitted.is_err() {
        viewer.cluster_inventory_screen_target.store(None);
        viewer
            .cluster_current_inventory_session
            .store(0, std::sync::atomic::Ordering::Release);
    }
    submitted?;
    Ok(())
}

fn execute_invsee(context: &CommandContext, editable: bool) -> CommandExecutorResult {
    let viewer = context
        .source
        .output
        .as_player()
        .ok_or_else(|| ERROR_NOT_PLAYER.create_without_context())?;
    let selector = context.get_argument::<EntitySelector>("target")?;
    match selector.find_single_player(&context.source) {
        Ok(target) => {
            let server = context.server().clone();
            let server_id = server.advanced_config.cluster.server_id;
            let viewer_gid = local_gid(&viewer, server_id);
            let target_gid = local_gid(&target, server_id);
            let viewer_task = viewer.clone();
            let source = context.source.clone();
            let target_name = target.gameprofile.name.clone();
            tokio::spawn(async move {
                if let Some(snapshot) =
                    cluster_invsee::request_inventory_snapshot(&server, viewer_gid, target_gid)
                        .await
                {
                    let opened = open_remote_view(&viewer_task, &snapshot, editable, None);
                    if opened {
                        source.send_feedback(TextComponent::text(target_name), false);
                    } else {
                        source.send_error(TextComponent::text("Inventory screen unavailable"));
                    }
                    if opened && editable && viewer_task.cluster_gid().is_none() {
                        set_close_action(
                            &viewer_task,
                            CloseAction::LocalEdit {
                                server,
                                viewer: viewer_gid,
                                target_gid,
                                baseline: snapshot,
                            },
                        );
                    }
                } else {
                    source.send_error(TextComponent::text("Inventory snapshot unavailable"));
                }
            });
            return Ok(1);
        }
        Err(local_error) => {
            let target_name = selector.player_name.clone().unwrap_or_default();
            if target_name.trim().is_empty() {
                return Err(local_error);
            }
            let server = context.server().clone();
            let source = context.source.clone();
            let server_id = server.advanced_config.cluster.server_id;
            let viewer_gid = local_gid(&viewer, server_id);
            let editable_flag = editable;
            tokio::spawn(async move {
                match cluster_invsee::request_remote_snapshot(
                    &server,
                    viewer_gid,
                    &target_name,
                    editable_flag,
                )
                .await
                {
                    Some(snapshot) => {
                        let target_gid = snapshot.target;
                        let owner = snapshot.owner_name.clone();
                        let opened = if editable_flag {
                            open_remote_view(
                                &viewer,
                                &snapshot,
                                true,
                                Some(RemoteEditSink {
                                    viewer: viewer_gid,
                                    target: target_gid,
                                    server: server.clone(),
                                }),
                            )
                        } else {
                            open_remote_view(&viewer, &snapshot, false, None)
                        };
                        if opened {
                            source.send_feedback(TextComponent::text(owner), false);
                        } else {
                            source.send_error(TextComponent::text("Inventory screen unavailable"));
                        }
                    }
                    None => {
                        if server.player_in_cluster_lobby(&target_name) {
                            source.send_error(TextComponent::text(format!(
                                "{target_name} is currently in the lobby, and not in the game world"
                            )));
                        } else if crate::server::cluster_presence::remote_presence_entries(&server)
                            .into_iter()
                            .any(|(_, entry)| entry.name.eq_ignore_ascii_case(&target_name))
                        {
                            source.send_error(TextComponent::text("No entity was found"));
                        } else {
                            source.send_error(TextComponent::text(format!(
                                "Player '{target_name}' is not online on this server or any linked peer"
                            )));
                        }
                    }
                }
            });
            return Ok(1);
        }
    }
}

struct ViewExecutor;

impl CommandExecutor for ViewExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        execute_invsee(context, false)
    }
}

struct EditExecutor;

impl CommandExecutor for EditExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        execute_invsee(context, true)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));
    registry.register_permission_or_panic(Permission::new(
        EDIT_PERMISSION,
        EDIT_DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    dispatcher.register(
        command("invsee", DESCRIPTION)
            .requires(PERMISSION)
            .then(argument("target", EntityArgumentType::Player).executes(ViewExecutor))
            .then(
                literal("edit").then(
                    argument("target", EntityArgumentType::Player)
                        .requires(EDIT_PERMISSION)
                        .executes(EditExecutor),
                ),
            ),
    );
}
