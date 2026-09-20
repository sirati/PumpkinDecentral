use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;

use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::invsee::{InventorySnapshot, InvseeSlot, InvseeWrite};
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
const PLAYER_SLOTS: usize = 41;

fn container_to_player(container: usize) -> Option<usize> {
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
    ARMOR_SLOTS
        .iter()
        .position(|slot| *slot == player_index)
}

fn filler_stack() -> ItemStack {
    let mut stack = ItemStack::new(1, &Item::LIGHT_GRAY_STAINED_GLASS_PANE);
    stack.set_custom_name(String::new());
    stack
}

fn encode_stack(index: u16, stack: &ItemStack) -> Option<InvseeSlot> {
    if stack.is_empty() {
        return None;
    }
    let mut compound = pumpkin_nbt::compound::NbtCompound::new();
    stack.write_item_stack(&mut compound);
    let bytes = Nbt::new(String::new(), compound).write_unnamed().to_vec();
    Some(InvseeSlot::new(index, stack.item.id, stack.item_count, bytes))
}

fn decode_stack(slot: &InvseeSlot) -> ItemStack {
    if slot.nbt.is_empty() {
        return ItemStack::EMPTY.clone();
    }
    let mut cursor = Cursor::new(slot.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    Nbt::read_unnamed(&mut reader)
        .ok()
        .and_then(|nbt| ItemStack::read_item_stack(&nbt.root_tag))
        .unwrap_or_else(|| ItemStack::EMPTY.clone())
}

#[must_use]
pub fn snapshot_player(player: &Player, target: GlobalPlayerId) -> InventorySnapshot {
    let mut slots = Vec::new();
    for index in 0..PLAYER_SLOTS {
        let stack = player.inventory.get_stack(index);
        if let Some(slot) = encode_stack(index as u16, &stack) {
            slots.push(slot);
        }
    }
    InventorySnapshot::new(
        target,
        player.gameprofile.name.clone(),
        player.inventory.get_selected_slot(),
        slots,
    )
}

pub fn apply_write_to_player(player: &Player, write: &InvseeWrite) {
    for slot in &write.slots {
        if usize::from(slot.index) < PLAYER_SLOTS {
            player
                .inventory
                .set_stack(usize::from(slot.index), decode_stack(slot));
        }
    }
}

pub struct RemoteEditSink {
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
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
        target: Arc<Player>,
        viewer: GlobalPlayerId,
        target_gid: GlobalPlayerId,
    },
    RemoteEdit { sink: RemoteEditSink },
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
        self.default_on_closed(player);
        match &self.close_action {
            CloseAction::Detached => {}
            CloseAction::LocalEdit {
                target,
                viewer,
                target_gid,
            } => {
                let write = InvseeWrite::new(*viewer, *target_gid, self.collect_slots());
                apply_write_to_player(target, &write);
            }
            CloseAction::RemoteEdit { sink } => {
                let write = InvseeWrite::new(sink.viewer, sink.target, self.collect_slots());
                cluster_invsee::submit_remote_write(&write);
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
        handler.add_player_slots(&viewer_inventory);
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

fn open_local_view(server_id: u16, viewer: &Arc<Player>, target: &Arc<Player>, editable: bool) {
    let target_gid = local_gid(target, server_id);
    let snapshot = snapshot_player(target, target_gid);
    open_remote_view(viewer, &snapshot, editable, None);
    if editable {
        set_close_action(
            viewer,
            CloseAction::LocalEdit {
                target: target.clone(),
                viewer: local_gid(viewer, server_id),
                target_gid,
            },
        );
    }
}

pub fn open_remote_view(
    viewer: &Arc<Player>,
    snapshot: &InventorySnapshot,
    editable: bool,
    sink: Option<RemoteEditSink>,
) {
    let container = Arc::new(SimpleInventory::new(CONTAINER_SIZE));
    fill_from_snapshot(&container, snapshot);
    let factory = InvseeScreenFactory {
        title: TextComponent::text(snapshot.owner_name.clone()),
        container: container.clone(),
        editable,
    };
    if viewer.open_handled_screen(&factory, None).is_some() {
        match sink {
            Some(sink) => set_close_action(viewer, CloseAction::RemoteEdit { sink }),
            None => set_close_action(viewer, CloseAction::Detached),
        }
    }
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
            let server_id = context.server().advanced_config.cluster.server_id;
            open_local_view(server_id, &viewer, &target, editable);
            context.source.send_feedback(
                TextComponent::text(target.gameprofile.name.clone()),
                false,
            );
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
                        if editable_flag {
                            open_remote_view(
                                &viewer,
                                &snapshot,
                                true,
                                Some(RemoteEditSink {
                                    viewer: viewer_gid,
                                    target: target_gid,
                                }),
                            );
                        } else {
                            open_remote_view(&viewer, &snapshot, false, None);
                        }
                        source.send_feedback(TextComponent::text(owner), false);
                    }
                    None => {
                        if server.player_in_cluster_lobby(&target_name) {
                            source.send_error(TextComponent::text(format!(
                                "{target_name} is currently in the lobby, and not in the game world"
                            )));
                        } else if crate::server::cluster_presence::remote_presence_entries()
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
            .then(
                argument("target", EntityArgumentType::Player)
                    .executes(ViewExecutor),
            )
            .then(
                literal("edit").then(
                    argument("target", EntityArgumentType::Player)
                        .requires(EDIT_PERMISSION)
                        .executes(EditExecutor),
                ),
            ),
    );
}
