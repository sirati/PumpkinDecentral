use std::any::Any;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::inventory::{
    InvLoc, InvOpKind, InventoryOp, InventoryStack, capture_semantic_inv_op, next_inv_seq,
};
use pumpkin_cluster::invsee::{InventorySnapshot, InvseeSlot};
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
use pumpkin_protocol::java::server::play::SlotActionType;
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
    let mut stack = ItemStack::new(1, &Item::GRAY_STAINED_GLASS_PANE);
    stack.set_custom_name(String::new());
    stack
}

fn encode_stack(index: u16, stack: &ItemStack) -> InvseeSlot {
    if stack.is_empty() {
        return InvseeSlot::new(index, 0, 0, Vec::new());
    }
    let mut compound = pumpkin_nbt::compound::NbtCompound::new();
    stack.write_item_stack(&mut compound);
    let bytes = pumpkin_nbt::Nbt::new(String::new(), compound)
        .write_unnamed()
        .to_vec();
    InvseeSlot::new(index, stack.item.id, stack.item_count, bytes)
}

fn decode_stack(slot: &InvseeSlot) -> ItemStack {
    if slot.is_empty_slot() || slot.nbt.is_empty() {
        return ItemStack::EMPTY.clone();
    }
    let mut cursor = Cursor::new(slot.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    pumpkin_nbt::Nbt::read_unnamed(&mut reader)
        .ok()
        .and_then(|nbt| ItemStack::read_item_stack(&nbt.root_tag))
        .unwrap_or_else(|| ItemStack::EMPTY.clone())
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

fn snapshot_values(snapshot: &InventorySnapshot) -> BTreeMap<u16, InvseeSlot> {
    let mut values = BTreeMap::new();
    for index in 0..=PLAYER_OFFHAND {
        values.insert(index as u16, InvseeSlot::new(index as u16, 0, 0, Vec::new()));
    }
    for slot in &snapshot.slots {
        if usize::from(slot.index) <= PLAYER_OFFHAND {
            values.insert(slot.index, slot.clone());
        }
    }
    values
}

fn stack_from_slot(slot: &InvseeSlot) -> InventoryStack {
    InventoryStack {
        item: slot.item_id,
        count: slot.count,
        nbt: slot.nbt.clone(),
    }
}

fn make_move(
    gid: GlobalPlayerId,
    tick: pumpkin_cluster::time::TickStamp,
    source: &InvseeSlot,
    destination: &InvseeSlot,
    count: u8,
) -> Option<InventoryOp> {
    capture_semantic_inv_op(
        gid,
        next_inv_seq(gid),
        tick,
        InvOpKind::Move,
        InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, source.index),
        InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, destination.index),
        stack_from_slot(source),
        stack_from_slot(destination),
        count,
    )
}

fn make_swap(
    gid: GlobalPlayerId,
    tick: pumpkin_cluster::time::TickStamp,
    source: &InvseeSlot,
    destination: &InvseeSlot,
) -> Option<InventoryOp> {
    capture_semantic_inv_op(
        gid,
        next_inv_seq(gid),
        tick,
        InvOpKind::Swap,
        InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, source.index),
        InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, destination.index),
        stack_from_slot(source),
        stack_from_slot(destination),
        source.count,
    )
}

fn semantic_changes(
    target: GlobalPlayerId,
    original: &InventorySnapshot,
    current: &[InvseeSlot],
) -> Option<Vec<InventoryOp>> {
    let tick = crate::server::cluster::disciplined_tick_now()?;
    let before = snapshot_values(original);
    let after = current
        .iter()
        .cloned()
        .map(|slot| (slot.index, slot))
        .collect::<BTreeMap<_, _>>();
    let mut working = before.clone();
    let changed = before
        .iter()
        .filter_map(|(index, old)| {
            let new = after.get(index)?;
            (old != new).then_some((old.clone(), new.clone()))
        })
        .collect::<Vec<_>>();
    if changed.is_empty() {
        return Some(Vec::new());
    }
    if changed.len() == 2
        && changed[0].0.item_id == changed[1].1.item_id
        && changed[0].0.count == changed[1].1.count
        && changed[0].0.nbt == changed[1].1.nbt
        && changed[1].0.item_id == changed[0].1.item_id
        && changed[1].0.count == changed[0].1.count
        && changed[1].0.nbt == changed[0].1.nbt
        && !changed[0].0.is_empty_slot()
        && !changed[1].0.is_empty_slot()
    {
        return make_swap(target, tick, &changed[0].0, &changed[1].0)
            .map(|operation| vec![operation]);
    }
    let mut deficits = Vec::<(InvseeSlot, u8)>::new();
    let mut surpluses = Vec::<(InvseeSlot, u8)>::new();
    for (old, new) in changed {
        if old.is_empty_slot() {
            if new.is_empty_slot() {
                continue;
            }
            surpluses.push((new.clone(), new.count));
            continue;
        }
        if new.is_empty_slot() {
            deficits.push((old.clone(), old.count));
            continue;
        }
        if old.item_id != new.item_id || old.nbt != new.nbt {
            return None;
        }
        if old.count > new.count {
            deficits.push((old.clone(), old.count.saturating_sub(new.count)));
        } else if new.count > old.count {
            surpluses.push((new.clone(), new.count.saturating_sub(old.count)));
        }
    }
    let mut operations = Vec::new();
    for (source, remaining) in &mut deficits {
        while *remaining > 0 {
            let destination_index = surpluses.iter().position(|(destination, amount)| {
                *amount > 0
                    && destination.item_id == source.item_id
                    && destination.nbt == source.nbt
            })?;
            let (destination, amount) = &mut surpluses[destination_index];
            let count = (*remaining).min(*amount);
            let current_source = working.get(&source.index)?.clone();
            let current_destination = working.get(&destination.index)?.clone();
            operations.push(make_move(
                target,
                tick,
                &current_source,
                &current_destination,
                count,
            )?);
            let mut source_after = current_source.clone();
            source_after.count = source_after.count.saturating_sub(count);
            if source_after.count == 0 {
                source_after.item_id = 0;
                source_after.nbt.clear();
            }
            let mut destination_after = current_destination.clone();
            if destination_after.is_empty_slot() {
                destination_after.item_id = current_source.item_id;
                destination_after.nbt = current_source.nbt.clone();
            }
            destination_after.count = destination_after.count.saturating_add(count);
            working.insert(source_after.index, source_after);
            working.insert(destination_after.index, destination_after);
            *remaining = remaining.saturating_sub(count);
            *amount = amount.saturating_sub(count);
        }
    }
    if surpluses.iter().any(|(_, amount)| *amount > 0) {
        return None;
    }
    if working != after {
        return None;
    }
    Some(operations)
}

pub struct InvseeScreenHandler {
    behaviour: ScreenHandlerBehaviour,
    container: Arc<dyn Inventory>,
    editable: bool,
    target: GlobalPlayerId,
    original: InventorySnapshot,
}

impl InvseeScreenHandler {
    fn push_container_slots(&mut self) {
        for container in 0..ARMOR_COUNT {
            if self.editable {
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
        if self.editable {
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
            if self.editable {
                self.add_slot(Arc::new(NormalSlot::new(self.container.clone(), container)));
            } else {
                self.add_slot(Arc::new(LockedSlot::new(self.container.clone(), container)));
            }
        }
    }

    fn current_values(&self) -> Vec<InvseeSlot> {
        let mut values = Vec::new();
        for container in 0..CONTAINER_SIZE {
            if let Some(player_index) = container_to_player(container) {
                values.push(encode_stack(
                    player_index as u16,
                    &self.container.get_stack(container),
                ));
            }
        }
        values
    }

    fn push_viewer_slots(&mut self, player_inventory: &Arc<PlayerInventory>) {
        let inventory: Arc<dyn Inventory> = player_inventory.clone();
        for index in 9..36 {
            self.add_slot(Arc::new(LockedSlot::new(inventory.clone(), index)));
        }
        for index in 0..9 {
            self.add_slot(Arc::new(LockedSlot::new(inventory.clone(), index)));
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

    fn on_slot_click(
        &mut self,
        slot_index: i32,
        button: i32,
        action_type: SlotActionType,
        player: &dyn InventoryPlayer,
    ) {
        if !self.editable
            || !matches!(&action_type, SlotActionType::Pickup | SlotActionType::QuickCraft)
        {
            self.cancel();
            return;
        }
        self.internal_on_slot_click(slot_index, button, action_type, player);
    }

    fn on_closed(&mut self, _player: &dyn InventoryPlayer) {
        if !self.editable {
            return;
        }
        let Some(operations) = semantic_changes(self.target, &self.original, &self.current_values())
        else {
            return;
        };
        let _ = cluster_invsee::submit_inventory_ops(operations);
    }

    fn quick_move(&mut self, _player: &dyn InventoryPlayer, _slot_index: i32) -> ItemStack {
        ItemStack::EMPTY.clone()
    }
}

struct InvseeScreenFactory {
    title: TextComponent,
    container: Arc<dyn Inventory>,
    editable: bool,
    target: GlobalPlayerId,
    original: InventorySnapshot,
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
            target: self.target,
            original: self.original.clone(),
        };
        handler.push_container_slots();
        handler.push_viewer_slots(player_inventory);
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

fn open_replicated_view(viewer: &Arc<Player>, snapshot: InventorySnapshot, editable: bool) {
    let container = Arc::new(SimpleInventory::new(CONTAINER_SIZE));
    fill_from_snapshot(&container, &snapshot);
    let factory = InvseeScreenFactory {
        title: TextComponent::text(snapshot.owner_name.clone()),
        container,
        editable,
        target: snapshot.target,
        original: snapshot,
    };
    let _ = viewer.open_handled_screen(&factory, None);
}

fn local_gid(player: &Player) -> Option<GlobalPlayerId> {
    player.cluster_gid()
}

fn named_remote_target(name: &str) -> Option<(GlobalPlayerId, String)> {
    crate::server::cluster_presence::remote_presence_entries()
        .into_iter()
        .find(|(_, entry)| entry.name.eq_ignore_ascii_case(name))
        .map(|(gid, entry)| (gid, entry.name))
}

fn execute_invsee(context: &CommandContext, editable: bool) -> CommandExecutorResult {
    let viewer = context
        .source
        .output
        .as_player()
        .ok_or_else(|| ERROR_NOT_PLAYER.create_without_context())?;
    let selector = context.get_argument::<EntitySelector>("target")?;
    let server = context.server();
    let name = selector.player_name.clone().unwrap_or_default();
    let target = selector
        .find_single_player(&context.source)
        .ok()
        .and_then(|player| {
            local_gid(&player).map(|gid| (gid, player.gameprofile.name.clone()))
        })
        .or_else(|| named_remote_target(&name));
    let Some((target, owner_name)) = target else {
        if server.player_in_cluster_lobby(&name) {
            context.source.send_error(TextComponent::text(format!(
                "{name} is currently in the lobby, and not in the game world"
            )));
        } else {
            context.source.send_error(TextComponent::text(format!(
                "Player '{name}' is not online on this server or any linked peer"
            )));
        }
        return Ok(0);
    };
    let Some(snapshot) = cluster_invsee::replicated_snapshot(target, &owner_name) else {
        context.source.send_error(TextComponent::text("No entity was found"));
        return Ok(0);
    };
    let owner = snapshot.owner_name.clone();
    open_replicated_view(&viewer, snapshot, editable);
    context.source.send_feedback(TextComponent::text(owner), false);
    Ok(1)
}

struct ViewExecutor;

impl CommandExecutor for ViewExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        execute_invsee(context, context.source.has_permission(EDIT_PERMISSION))
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
