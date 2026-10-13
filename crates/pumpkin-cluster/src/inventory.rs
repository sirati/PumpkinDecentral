use std::collections::HashMap;

use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use crate::protocol::{BlockPos, ChunkKey, EntityRef, GlobalEntityId};
use crate::time::TickStamp;

pub const INV_MAIN: u8 = 0;
pub const INV_ARMOR: u8 = 1;
pub const INV_OFFHAND: u8 = 2;
pub const INV_CURSOR: u8 = 3;
pub const INV_SELECTED: u8 = 4;
pub const INV_MAX_STACK: u8 = 99;
pub const INV_DEFAULT_STACK_LIMIT: u8 = 64;
pub const INVENTORY_SERVICE_MAX_SLOTS: usize = 128;
pub const INVENTORY_MAX_NBT_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityInventoryId {
    pub origin: crate::identity::ServerId,
    pub local_id: i32,
    pub generation: u64,
}

impl From<EntityRef> for EntityInventoryId {
    fn from(entity: EntityRef) -> Self {
        Self {
            origin: entity.origin,
            local_id: entity.local_id,
            generation: entity.generation,
        }
    }
}

impl From<GlobalEntityId> for EntityInventoryId {
    fn from(entity: GlobalEntityId) -> Self {
        Self {
            origin: entity.origin,
            local_id: entity.local_id,
            generation: entity.generation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InventoryTarget {
    Player(GlobalPlayerId),
    LeasedPlayer {
        key: uuid::Uuid,
        player: GlobalPlayerId,
        incarnation: u64,
    },
    Entity(EntityInventoryId),
    Block {
        chunk: ChunkKey,
        position: BlockPos,
    },
    Virtual {
        host: crate::identity::ServerId,
        owner: ActorId,
        session: u64,
        kind: u16,
    },
    LeasedVirtual {
        host: crate::identity::ServerId,
        key: uuid::Uuid,
        player: GlobalPlayerId,
        incarnation: u64,
        session: u64,
        kind: u16,
    },
    BlockVirtual {
        host: crate::identity::ServerId,
        chunk: ChunkKey,
        position: BlockPos,
        session: u64,
        kind: u16,
    },
}

impl From<GlobalPlayerId> for InventoryTarget {
    fn from(value: GlobalPlayerId) -> Self {
        Self::Player(value)
    }
}

impl InventoryTarget {
    #[must_use]
    pub const fn is_screen_virtual(self) -> bool {
        matches!(self, Self::Virtual { kind, .. } | Self::LeasedVirtual { kind, .. } | Self::BlockVirtual { kind, .. } if kind != 0x100)
    }

    #[must_use]
    pub const fn intrinsic_chunk(self) -> Option<ChunkKey> {
        match self {
            Self::Block { chunk, .. } | Self::BlockVirtual { chunk, .. } => Some(chunk),
            Self::Player(_)
            | Self::LeasedPlayer { .. }
            | Self::Entity(_)
            | Self::Virtual { .. }
            | Self::LeasedVirtual { .. } => None,
        }
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        match self {
            Self::Block { chunk, position }
            | Self::BlockVirtual {
                chunk, position, ..
            } => {
                position.x.div_euclid(16) == chunk.position.x
                    && position.z.div_euclid(16) == chunk.position.z
            }
            Self::Virtual {
                owner: ActorId::Peer(_),
                ..
            } => false,
            Self::LeasedPlayer { incarnation, .. } => incarnation != 0,
            Self::LeasedVirtual { incarnation, .. } => incarnation != 0,
            Self::Player(_) | Self::Entity(_) | Self::Virtual { .. } => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InvLoc {
    pub inv: u8,
    pub slot: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryCell {
    pub loc: InvLoc,
    pub item: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
}

impl InventoryCell {
    #[must_use]
    pub fn empty(loc: InvLoc) -> Self {
        Self {
            loc,
            item: 0,
            count: 0,
            nbt: Vec::new(),
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.item == 0 || self.count == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryStateSnapshot {
    pub target: InventoryTarget,
    pub revision: u64,
    pub cells: Vec<InventoryCell>,
}

impl InventoryStateSnapshot {
    pub fn has_complete_persistent_cells(&self) -> bool {
        if matches!(
            self.target,
            InventoryTarget::Player(_) | InventoryTarget::LeasedPlayer { .. }
        ) {
            return self.has_complete_player_cells();
        }
        let InventoryTarget::LeasedVirtual {
            kind, session: 0, ..
        } = self.target
        else {
            return true;
        };
        let count = match kind {
            crate::avatar_lifecycle::PLAYER_CRAFTING_INVENTORY_KIND => 5,
            crate::avatar_lifecycle::PLAYER_ENDER_INVENTORY_KIND => 27,
            _ => return true,
        };
        let mut expected = (0..count)
            .map(|slot| InvLoc::new(INV_MAIN, slot))
            .collect::<std::collections::BTreeSet<_>>();
        self.cells.len() == expected.len()
            && self.cells.iter().all(|cell| {
                expected.remove(&cell.loc)
                    && cell.count <= INV_MAX_STACK
                    && (cell.item == 0) == (cell.count == 0)
                    && cell.nbt.len() <= INVENTORY_MAX_NBT_BYTES
                    && (!cell.is_empty() || cell.nbt.is_empty())
            })
            && expected.is_empty()
    }

    pub fn has_complete_player_cells(&self) -> bool {
        if !matches!(
            self.target,
            InventoryTarget::Player(_) | InventoryTarget::LeasedPlayer { .. }
        ) {
            return false;
        }
        let mut expected = crate::avatar_lifecycle::PlayerInventoryAdmission::slots()
            .collect::<std::collections::BTreeSet<_>>();
        self.cells.len() == expected.len()
            && self.cells.iter().all(|cell| {
                expected.remove(&cell.loc)
                    && cell.nbt.len() <= INVENTORY_MAX_NBT_BYTES
                    && if cell.loc.inv == INV_SELECTED {
                        (1..=9).contains(&cell.item) && cell.count == 1 && cell.nbt.is_empty()
                    } else {
                        cell.count <= INV_MAX_STACK
                            && (cell.item == 0) == (cell.count == 0)
                            && (!cell.is_empty() || cell.nbt.is_empty())
                    }
            })
            && expected.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticInventoryOp {
    pub target: InventoryTarget,
    pub before: InventoryCell,
    pub after: InventoryCell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMutation {
    pub expected_revisions: Vec<(InventoryTarget, u64)>,
    pub operations: Vec<SemanticInventoryOp>,
}

#[must_use]
pub fn plan_inventory_insertion<Compatible, WithCount>(
    snapshot: &InventoryStateSnapshot,
    source_after: Option<&InventoryCell>,
    item: &InventoryCell,
    max_stack: u8,
    destinations: &[InvLoc],
    compatible: Compatible,
    with_count: WithCount,
) -> Option<Vec<SemanticInventoryOp>>
where
    Compatible: Fn(&InventoryCell, &InventoryCell) -> bool,
    WithCount: Fn(&InventoryCell, InvLoc, u8) -> Option<InventoryCell>,
{
    if item.is_empty()
        || max_stack == 0
        || !snapshot.target.is_valid()
        || destinations.len() > INVENTORY_SERVICE_MAX_SLOTS
        || snapshot.cells.len() > INVENTORY_SERVICE_MAX_SLOTS
    {
        return None;
    }
    let mut locations =
        Vec::with_capacity(destinations.len() + usize::from(source_after.is_some()));
    if let Some(source) = source_after.filter(|source| source.is_empty()) {
        locations.push(source.loc);
    }
    for location in destinations {
        if !locations.contains(location) {
            locations.push(*location);
        }
    }
    let mut remaining = item.count;
    let mut operations = Vec::new();
    for pass in 0..3 {
        for location in &locations {
            if remaining == 0 {
                break;
            }
            let before = snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == *location)
                .cloned()
                .unwrap_or_else(|| InventoryCell::empty(*location));
            let current = if source_after.is_some_and(|source| source.loc == *location) {
                source_after.cloned().unwrap_or_else(|| before.clone())
            } else {
                before.clone()
            };
            let is_source = source_after.is_some_and(|source| source.loc == *location);
            if (pass == 0 && (!is_source || !current.is_empty()))
                || (pass == 1 && (is_source || current.is_empty()))
                || (pass == 2 && (is_source || !current.is_empty()))
            {
                continue;
            }
            if !current.is_empty() && !compatible(&current, item) {
                continue;
            }
            let room = max_stack.saturating_sub(if current.is_empty() { 0 } else { current.count });
            let placed = room.min(remaining);
            if placed == 0 {
                continue;
            }
            remaining -= placed;
            let new_count = if current.is_empty() {
                placed
            } else {
                current.count.checked_add(placed)?
            };
            let after = with_count(item, *location, new_count)?;
            if after.loc != *location || after.count != new_count || after.item != item.item {
                return None;
            }
            operations.push(SemanticInventoryOp {
                target: snapshot.target,
                before,
                after,
            });
        }
    }
    (remaining == 0).then_some(operations)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryIntentPayload {
    Mutation(InventoryMutation),
    Gestures(Vec<InventoryGesture>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryMutationIntent {
    pub actor: ActorId,
    pub tick: TickStamp,
    pub payload: InventoryIntentPayload,
    pub screen_session: Option<InventoryScreenSession>,
    pub actor_chunk: ChunkKey,
    pub affected: Vec<ChunkKey>,
}

#[must_use]
pub fn inventory_intent_targets(intent: &InventoryMutationIntent) -> Vec<InventoryTarget> {
    let mut targets: Vec<InventoryTarget> = match &intent.payload {
        InventoryIntentPayload::Mutation(mutation) => mutation
            .expected_revisions
            .iter()
            .map(|(target, _)| *target)
            .chain(mutation.operations.iter().map(|operation| operation.target))
            .collect(),
        InventoryIntentPayload::Gestures(gestures) => gestures
            .iter()
            .flat_map(InventoryGesture::targets)
            .collect(),
    };
    targets.sort_unstable();
    targets.dedup();
    targets
}

impl InventoryMutationIntent {
    pub fn into_action(
        self,
        seq: PlayerSeq,
        ledger: &InvLedger,
    ) -> Result<InventoryOp, InventoryGestureError> {
        self.into_action_with_preview(seq, ledger, None)
    }

    pub fn into_action_with_preview(
        self,
        seq: PlayerSeq,
        ledger: &InvLedger,
        preview: Option<&InventoryStateSnapshot>,
    ) -> Result<InventoryOp, InventoryGestureError> {
        let actor_chunk = self.actor_chunk;
        let mut action = match self.payload {
            InventoryIntentPayload::Mutation(mutation) => Ok(capture_inventory_mutation(
                self.actor,
                seq,
                self.tick,
                mutation.expected_revisions,
                mutation.operations,
                actor_chunk,
                self.affected,
            )),
            InventoryIntentPayload::Gestures(gestures) => {
                let mut targets = Vec::new();
                for gesture in &gestures {
                    gesture.push_targets(&mut targets);
                }
                targets.sort_unstable();
                targets.dedup();
                let snapshots = targets
                    .into_iter()
                    .map(|target| {
                        ledger
                            .snapshot(target)
                            .or_else(|| {
                                preview
                                    .filter(|snapshot| snapshot.target == target)
                                    .cloned()
                            })
                            .ok_or(InventoryGestureError::MissingInventory)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let reduction = reduce_inventory_gestures(&snapshots, &gestures)?;
                let mut action = capture_inventory_mutation_with_effects(
                    self.actor,
                    seq,
                    self.tick,
                    reduction.mutation.expected_revisions,
                    reduction.mutation.operations,
                    reduction.effects,
                    actor_chunk,
                    self.affected,
                );
                action.gestures = gestures;
                Ok(action)
            }
        }?;
        action.actor_chunk = actor_chunk;
        Ok(action)
    }
}

impl InventoryMutation {
    pub fn replace(
        snapshot: &InventoryStateSnapshot,
        mut desired: Vec<InventoryCell>,
    ) -> Result<Self, InventoryMutationError> {
        if !desired.iter().any(|cell| cell.loc.inv == INV_SELECTED)
            && let Some(selected) = snapshot
                .cells
                .iter()
                .find(|cell| cell.loc.inv == INV_SELECTED)
        {
            desired.push(selected.clone());
        }
        if desired.len() > INVENTORY_SERVICE_MAX_SLOTS {
            return Err(InventoryMutationError::InvalidLocation);
        }
        let mut before: HashMap<_, _> = snapshot
            .cells
            .iter()
            .cloned()
            .map(|cell| (cell.loc, cell))
            .collect();
        let mut after = HashMap::with_capacity(desired.len());
        for cell in desired {
            if !valid_cell(&cell) {
                return Err(InventoryMutationError::InvalidStack);
            }
            if after.insert(cell.loc, cell).is_some() {
                return Err(InventoryMutationError::DuplicateLocation);
            }
        }
        let mut locations: Vec<_> = before.keys().chain(after.keys()).copied().collect();
        locations.sort_unstable_by_key(|loc| (loc.inv, loc.slot));
        locations.dedup();
        let operations = locations
            .into_iter()
            .filter_map(|loc| {
                let before = before
                    .remove(&loc)
                    .unwrap_or_else(|| InventoryCell::empty(loc));
                let after = after
                    .remove(&loc)
                    .unwrap_or_else(|| InventoryCell::empty(loc));
                (before != after).then_some(SemanticInventoryOp {
                    target: snapshot.target,
                    before,
                    after,
                })
            })
            .collect();
        Ok(Self {
            expected_revisions: vec![(snapshot.target, snapshot.revision)],
            operations,
        })
    }

    pub fn select_hotbar(
        snapshot: &InventoryStateSnapshot,
        selected_slot: u8,
    ) -> Result<Self, InventoryMutationError> {
        if selected_slot > 8 {
            return Err(InventoryMutationError::InvalidLocation);
        }
        let loc = InvLoc::new(INV_SELECTED, 0);
        let before = snapshot
            .cells
            .iter()
            .find(|cell| cell.loc == loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(loc));
        let after = InventoryCell {
            loc,
            item: u16::from(selected_slot) + 1,
            count: 1,
            nbt: Vec::new(),
        };
        Ok(Self {
            expected_revisions: vec![(snapshot.target, snapshot.revision)],
            operations: (before != after)
                .then_some(SemanticInventoryOp {
                    target: snapshot.target,
                    before,
                    after,
                })
                .into_iter()
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryDelta {
    pub revisions: Vec<(InventoryTarget, u64, u64)>,
    pub operations: Vec<SemanticInventoryOp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryMutationError {
    MissingInventory,
    TargetCollision,
    StaleRevision,
    InvalidLocation,
    DuplicateLocation,
    PreconditionFailed,
    InvalidStack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryApplyError {
    Rejected,
    TargetUnavailable,
    QueueFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryResolution {
    pub action: crate::completion::ActionKey,
    pub tick: TickStamp,
    pub result: Result<InventoryDelta, InventoryApplyError>,
    pub authoritative: Vec<InventoryStateSnapshot>,
    pub correction: InventoryDelta,
    pub effects: Vec<InventoryGestureEffect>,
    pub menu_effects: Vec<InventoryMenuEffect>,
}

#[derive(Debug, Default)]
pub struct InventorySequence {
    next: u16,
}

impl InventorySequence {
    #[must_use]
    pub const fn new() -> Self {
        Self { next: 0 }
    }

    pub fn issue(&mut self) -> PlayerSeq {
        let issued = PlayerSeq(self.next);
        self.next = self.next.wrapping_add(1);
        issued
    }

    pub fn issue_or_observe(&mut self, provided: Option<PlayerSeq>) -> PlayerSeq {
        let Some(provided) = provided else {
            return self.issue();
        };
        if provided.is_newer_than(PlayerSeq(self.next.wrapping_sub(1))) {
            self.next = provided.0.wrapping_add(1);
        }
        provided
    }
}

#[derive(Debug, Default, Clone)]
struct ServiceInventory {
    revision: u64,
    cells: HashMap<InvLoc, InventoryCell>,
    subscribers: Vec<mpsc::Sender<InventoryDelta>>,
}

#[derive(Debug, Default)]
pub struct InventoryService {
    inventories: HashMap<InventoryTarget, ServiceInventory>,
    chunks: HashMap<InventoryTarget, Vec<ChunkKey>>,
}

impl InventoryService {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install(
        &mut self,
        target: impl Into<InventoryTarget>,
        revision: u64,
        cells: Vec<InventoryCell>,
    ) -> Result<(), InventoryMutationError> {
        let target = target.into();
        if !target.is_valid() {
            return Err(InventoryMutationError::InvalidLocation);
        }
        let nbt_bytes = cells
            .iter()
            .try_fold(0_usize, |total, cell| total.checked_add(cell.nbt.len()));
        if cells.len() > INVENTORY_SERVICE_MAX_SLOTS
            || nbt_bytes.is_none_or(|bytes| bytes > INVENTORY_MAX_NBT_BYTES)
            || cells.iter().any(|cell| !valid_cell(cell))
        {
            return Err(InventoryMutationError::InvalidStack);
        }
        let mut stored = HashMap::with_capacity(cells.len());
        let mut locations = HashMap::with_capacity(cells.len());
        for cell in cells {
            if locations.insert(cell.loc, ()).is_some() {
                return Err(InventoryMutationError::DuplicateLocation);
            }
            if !cell.is_empty() {
                stored.insert(cell.loc, cell);
            }
        }
        self.inventories.insert(
            target,
            ServiceInventory {
                revision,
                cells: stored,
                subscribers: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn install_owned(
        &mut self,
        target: InventoryTarget,
        revision: u64,
        cells: Vec<InventoryCell>,
        mut chunks: Vec<ChunkKey>,
    ) -> Result<(), InventoryMutationError> {
        chunks.extend(target.intrinsic_chunk());
        chunks.sort_unstable();
        chunks.dedup();
        if chunks.is_empty() {
            return Err(InventoryMutationError::InvalidLocation);
        }
        self.install(target, revision, cells)?;
        self.chunks.insert(target, chunks);
        Ok(())
    }

    pub fn relocate(
        &mut self,
        target: InventoryTarget,
        mut chunks: Vec<ChunkKey>,
    ) -> Result<(), InventoryMutationError> {
        if !self.inventories.contains_key(&target) {
            return Err(InventoryMutationError::MissingInventory);
        }
        chunks.extend(target.intrinsic_chunk());
        chunks.sort_unstable();
        chunks.dedup();
        if chunks.is_empty() {
            return Err(InventoryMutationError::InvalidLocation);
        }
        self.chunks.insert(target, chunks);
        Ok(())
    }

    pub fn remove(&mut self, target: InventoryTarget) -> Option<InventoryStateSnapshot> {
        self.chunks.remove(&target);
        let inventory = self.inventories.remove(&target)?;
        let mut cells: Vec<_> = inventory.cells.into_values().collect();
        cells.sort_by_key(|cell| (cell.loc.inv, cell.loc.slot));
        Some(InventoryStateSnapshot {
            target,
            revision: inventory.revision,
            cells,
        })
    }

    #[must_use]
    pub fn chunks(&self, target: InventoryTarget) -> Option<&[ChunkKey]> {
        self.chunks.get(&target).map(Vec::as_slice)
    }

    #[must_use]
    pub fn snapshot(&self, target: impl Into<InventoryTarget>) -> Option<InventoryStateSnapshot> {
        let target = target.into();
        let inventory = self.inventories.get(&target)?;
        let mut cells: Vec<_> = inventory.cells.values().cloned().collect();
        cells.sort_by_key(|cell| (cell.loc.inv, cell.loc.slot));
        Some(InventoryStateSnapshot {
            target,
            revision: inventory.revision,
            cells,
        })
    }

    pub fn subscribe(
        &mut self,
        target: impl Into<InventoryTarget>,
        capacity: usize,
    ) -> Option<(InventoryStateSnapshot, mpsc::Receiver<InventoryDelta>)> {
        let target = target.into();
        let snapshot = self.snapshot(target)?;
        let inventory = self.inventories.get_mut(&target)?;
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        inventory.subscribers.push(sender);
        Some((snapshot, receiver))
    }

    pub fn mutate(
        &mut self,
        mutation: InventoryMutation,
    ) -> Result<InventoryDelta, InventoryMutationError> {
        if mutation.operations.len() > INVENTORY_SERVICE_MAX_SLOTS {
            return Err(InventoryMutationError::InvalidLocation);
        }
        let nbt_bytes = mutation
            .operations
            .iter()
            .try_fold(0_usize, |total, operation| {
                total
                    .checked_add(operation.before.nbt.len())?
                    .checked_add(operation.after.nbt.len())
            });
        if nbt_bytes.is_none_or(|bytes| bytes > INVENTORY_MAX_NBT_BYTES) {
            return Err(InventoryMutationError::InvalidStack);
        }
        let mut expected = HashMap::with_capacity(mutation.expected_revisions.len());
        for (target, revision) in &mutation.expected_revisions {
            if !target.is_valid() {
                return Err(InventoryMutationError::InvalidLocation);
            }
            if expected.insert(*target, *revision).is_some() {
                return Err(InventoryMutationError::DuplicateLocation);
            }
            let inventory = self
                .inventories
                .get(target)
                .ok_or(InventoryMutationError::MissingInventory)?;
            if inventory.revision != *revision {
                return Err(InventoryMutationError::StaleRevision);
            }
        }
        let mut next: HashMap<_, _> = expected
            .keys()
            .map(|target| (*target, self.inventories[target].cells.clone()))
            .collect();
        let mut touched = HashMap::with_capacity(mutation.operations.len());
        for operation in &mutation.operations {
            let Some(cells) = next.get_mut(&operation.target) else {
                return Err(InventoryMutationError::MissingInventory);
            };
            if operation.before.loc != operation.after.loc {
                return Err(InventoryMutationError::InvalidLocation);
            }
            if touched
                .insert((operation.target, operation.before.loc), ())
                .is_some()
            {
                return Err(InventoryMutationError::DuplicateLocation);
            }
            if !valid_cell(&operation.before) || !valid_cell(&operation.after) {
                return Err(InventoryMutationError::InvalidStack);
            }
            let actual = cells
                .get(&operation.before.loc)
                .cloned()
                .unwrap_or_else(|| InventoryCell::empty(operation.before.loc));
            if actual != operation.before {
                return Err(InventoryMutationError::PreconditionFailed);
            }
            if operation.after.is_empty() {
                cells.remove(&operation.after.loc);
            } else {
                cells.insert(operation.after.loc, operation.after.clone());
            }
        }
        let mut revisions = Vec::with_capacity(expected.len());
        for (target, from_revision) in &mutation.expected_revisions {
            let inventory = self.inventories.get_mut(&target).unwrap();
            inventory.revision = inventory.revision.wrapping_add(1);
            inventory.cells = next.remove(&target).unwrap();
            revisions.push((*target, *from_revision, inventory.revision));
        }
        let delta = InventoryDelta {
            revisions,
            operations: mutation.operations,
        };
        for (target, _, _) in &delta.revisions {
            self.inventories
                .get_mut(target)
                .unwrap()
                .subscribers
                .retain(|subscriber| subscriber.try_send(delta.clone()).is_ok());
        }
        Ok(delta)
    }
}

#[must_use]
pub fn inventory_content_revision<'a>(
    selected: u8,
    cells: impl IntoIterator<Item = &'a InventoryCell>,
) -> Result<u64, InventoryMutationError> {
    use xxhash_rust::xxh64::xxh64;

    let mut ordered: Vec<_> = cells.into_iter().collect();
    let nbt_bytes = ordered
        .iter()
        .try_fold(0_usize, |total, cell| total.checked_add(cell.nbt.len()));
    if ordered.len() > INVENTORY_SERVICE_MAX_SLOTS
        || nbt_bytes.is_none_or(|bytes| bytes > INVENTORY_MAX_NBT_BYTES)
        || ordered.iter().any(|cell| !valid_cell(cell))
    {
        return Err(InventoryMutationError::InvalidStack);
    }
    ordered.sort_by_key(|cell| (cell.loc.inv, cell.loc.slot));
    let mut bytes = Vec::new();
    bytes.push(selected);
    for cell in ordered {
        bytes.push(cell.loc.inv);
        bytes.extend_from_slice(&cell.loc.slot.to_be_bytes());
        bytes.extend_from_slice(&cell.item.to_be_bytes());
        bytes.push(cell.count);
        bytes.extend_from_slice(&(cell.nbt.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&cell.nbt);
    }
    Ok(xxh64(&bytes, 0x494e_5645_4e54_4f52))
}

fn valid_cell(cell: &InventoryCell) -> bool {
    if usize::from(cell.loc.slot) >= INVENTORY_SERVICE_MAX_SLOTS {
        return false;
    }
    if cell.nbt.len() > INVENTORY_MAX_NBT_BYTES {
        return false;
    }
    if cell.is_empty() {
        return cell.item == 0 && cell.count == 0 && cell.nbt.is_empty();
    }
    cell.count <= INV_MAX_STACK
}

impl InvLoc {
    #[must_use]
    pub const fn new(inv: u8, slot: u16) -> Self {
        Self { inv, slot }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvOpKind {
    Move,
    MoveHalf,
    Split,
    Swap,
    Drop,
    Pickup,
    Consume,
    Set,
    Transaction,
}

impl InvOpKind {
    #[must_use]
    pub const fn reads_source(self) -> bool {
        match self {
            Self::Move | Self::MoveHalf | Self::Split | Self::Swap | Self::Drop | Self::Consume => {
                true
            }
            Self::Pickup | Self::Set | Self::Transaction => false,
        }
    }

    #[must_use]
    pub const fn writes_destination(self) -> bool {
        match self {
            Self::Move
            | Self::MoveHalf
            | Self::Split
            | Self::Swap
            | Self::Pickup
            | Self::Set
            | Self::Transaction => true,
            Self::Drop | Self::Consume => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryOp {
    pub actor: ActorId,
    pub player_gid: Option<GlobalPlayerId>,
    pub player_target: Option<GlobalPlayerId>,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub kind: InvOpKind,
    pub src: InvLoc,
    pub dst: InvLoc,
    pub item: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
    pub expected_revision: Option<u64>,
    pub expected_revisions: Vec<(InventoryTarget, u64)>,
    pub changes: Vec<SemanticInventoryOp>,
    pub gestures: Vec<InventoryGesture>,
    pub effects: Vec<InventoryGestureEffect>,
    pub menu_effects: Vec<InventoryMenuEffect>,
    pub actor_chunk: crate::protocol::ChunkKey,
    pub affected: Vec<crate::protocol::ChunkKey>,
    pub dependencies: Vec<crate::completion::ActionKey>,
}

impl InventoryOp {
    #[must_use]
    pub const fn stream_kind() -> crate::protocol::StreamKind {
        crate::protocol::StreamKind::PlayerWorld
    }
}

#[must_use]
pub fn inventory_op_targets(operation: &InventoryOp) -> Vec<InventoryTarget> {
    let mut targets: Vec<_> = operation
        .expected_revisions
        .iter()
        .map(|(target, _)| *target)
        .chain(operation.changes.iter().map(|change| change.target))
        .collect();
    targets.sort_unstable();
    targets.dedup();
    targets
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JointInventoryLeg {
    pub chunk: ChunkKey,
    pub holders: Vec<ServerId>,
    pub finished: Vec<ServerId>,
    pub ground_ready: bool,
    pub inventories: Vec<InventoryStateSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JointInventoryError {
    IncompleteChunks,
    DuplicateChunk,
    IncompleteManifest,
    GroundUnavailable,
    DuplicateTarget,
    MissingTarget,
    InvalidTargetChunk,
    Install(InventoryMutationError),
    Apply(InventoryApplyError),
}

pub fn preview_joint_inventory_action(
    operation: &InventoryOp,
    legs: &[JointInventoryLeg],
) -> Result<InventoryDelta, JointInventoryError> {
    let mut affected = operation.affected.clone();
    affected.sort_unstable();
    affected.dedup();
    if affected.is_empty()
        || !affected.contains(&operation.actor_chunk)
        || legs.len() != affected.len()
    {
        return Err(JointInventoryError::IncompleteChunks);
    }
    let mut seen_chunks = Vec::with_capacity(legs.len());
    let mut seen_targets = Vec::new();
    let mut ledger = InvLedger::new();
    for leg in legs {
        if !affected.contains(&leg.chunk) || seen_chunks.contains(&leg.chunk) {
            return Err(JointInventoryError::DuplicateChunk);
        }
        seen_chunks.push(leg.chunk);
        let mut holders = leg.holders.clone();
        let mut finished = leg.finished.clone();
        holders.sort_unstable();
        finished.sort_unstable();
        if holders.is_empty()
            || holders.windows(2).any(|pair| pair[0] == pair[1])
            || finished != holders
        {
            return Err(JointInventoryError::IncompleteManifest);
        }
        if !leg.ground_ready {
            return Err(JointInventoryError::GroundUnavailable);
        }
        for inventory in &leg.inventories {
            if seen_targets.contains(&inventory.target) {
                return Err(JointInventoryError::DuplicateTarget);
            }
            if inventory
                .target
                .intrinsic_chunk()
                .is_some_and(|chunk| chunk != leg.chunk)
                || matches!(operation.actor, ActorId::Player(actor) if inventory.target == InventoryTarget::Player(actor) && leg.chunk != operation.actor_chunk)
            {
                return Err(JointInventoryError::InvalidTargetChunk);
            }
            seen_targets.push(inventory.target);
            ledger
                .install_owned_snapshot(inventory.clone(), vec![leg.chunk])
                .map_err(JointInventoryError::Install)?;
        }
    }
    if inventory_op_targets(operation)
        .iter()
        .any(|target| !seen_targets.contains(target))
    {
        return Err(JointInventoryError::MissingTarget);
    }
    ledger
        .apply_op_result(operation)
        .map_err(JointInventoryError::Apply)
}

#[derive(Debug, Default)]
pub struct InventoryCaptureBarrier {
    pending: HashMap<crate::completion::ActionKey, Vec<InventoryTarget>>,
}

impl InventoryCaptureBarrier {
    pub fn register(&mut self, operation: &InventoryOp) {
        self.pending.insert(
            crate::completion::ActionKey::inventory(operation),
            inventory_op_targets(operation),
        );
    }

    pub fn resolve(&mut self, action: crate::completion::ActionKey) {
        self.pending.remove(&action);
    }

    #[must_use]
    pub fn blocks(&self, actor: ActorId, targets: &[InventoryTarget]) -> bool {
        self.pending.iter().any(|(action, pending)| {
            action.actor == actor && targets.iter().any(|target| pending.contains(target))
        })
    }
}

pub trait InvCells {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)>;
    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvVerdict {
    Applied,
    Rejected,
}

#[must_use]
pub const fn inv_precondition_met(
    actual_item: u16,
    actual_count: u8,
    expected_item: u16,
    expected_count: u8,
) -> bool {
    actual_item == expected_item && actual_count == expected_count
}

#[must_use]
pub const fn inv_stack_has(
    actual_item: u16,
    actual_count: u8,
    expected_item: u16,
    needed: u8,
) -> bool {
    actual_item == expected_item && actual_count >= needed
}

fn dst_accepts(dst_item: Option<(u16, u8)>, item: u16, count: u8) -> bool {
    match dst_item {
        None => true,
        Some((0, _)) => true,
        Some((existing, _)) if count == 0 => existing == item,
        Some((existing, have)) => {
            existing == item && have.saturating_add(count) <= INV_DEFAULT_STACK_LIMIT
        }
    }
}

fn take_from(cells: &mut impl InvCells, loc: InvLoc, item: u16, count: u8) -> bool {
    let Some((have_item, have_count)) = cells.cell(loc) else {
        return false;
    };
    if !inv_stack_has(have_item, have_count, item, count) {
        return false;
    }
    let left = have_count.saturating_sub(count);
    if left == 0 {
        cells.set_cell(loc, 0, 0)
    } else {
        cells.set_cell(loc, item, left)
    }
}

fn give_to(cells: &mut impl InvCells, loc: InvLoc, item: u16, count: u8) -> bool {
    if count == 0 {
        return cells.set_cell(loc, 0, 0);
    }
    let current = cells.cell(loc);
    if !dst_accepts(current, item, count) {
        return false;
    }
    match current {
        None | Some((0, _)) => cells.set_cell(loc, item, count),
        Some((_, have)) => cells.set_cell(loc, item, have.saturating_add(count)),
    }
}

pub fn replay(cells: &mut impl InvCells, op: &InventoryOp) -> InvVerdict {
    match op.kind {
        InvOpKind::Move | InvOpKind::Consume | InvOpKind::Drop => {
            if op.count == 0 {
                return InvVerdict::Rejected;
            }
            if op.kind != InvOpKind::Drop && op.kind != InvOpKind::Consume {
                let dst = cells.cell(op.dst);
                if !dst_accepts(dst, op.item, op.count) {
                    return InvVerdict::Rejected;
                }
            }
            if !take_from(cells, op.src, op.item, op.count) {
                return InvVerdict::Rejected;
            }
            if op.kind == InvOpKind::Move {
                if !give_to(cells, op.dst, op.item, op.count) {
                    let _ = give_to(cells, op.src, op.item, op.count);
                    return InvVerdict::Rejected;
                }
            }
            InvVerdict::Applied
        }
        InvOpKind::MoveHalf => {
            let Some((have_item, have_count)) = cells.cell(op.src) else {
                return InvVerdict::Rejected;
            };
            if have_count < 2 || have_item != op.item {
                return InvVerdict::Rejected;
            }
            let half = have_count.saturating_div(2);
            if op.count != half || half == 0 {
                return InvVerdict::Rejected;
            }
            let dst = cells.cell(op.dst);
            if !dst_accepts(dst, op.item, half) {
                return InvVerdict::Rejected;
            }
            if !take_from(cells, op.src, op.item, half) {
                return InvVerdict::Rejected;
            }
            if !give_to(cells, op.dst, op.item, half) {
                let _ = give_to(cells, op.src, op.item, half);
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Split => {
            if op.count != 1 {
                return InvVerdict::Rejected;
            }
            let Some((have_item, have_count)) = cells.cell(op.src) else {
                return InvVerdict::Rejected;
            };
            if have_item != op.item || have_count < 2 {
                return InvVerdict::Rejected;
            }
            match cells.cell(op.dst) {
                None | Some((0, _)) => {}
                _ => return InvVerdict::Rejected,
            }
            if !take_from(cells, op.src, op.item, 1) {
                return InvVerdict::Rejected;
            }
            if !cells.set_cell(op.dst, op.item, 1) {
                let _ = give_to(cells, op.src, op.item, 1);
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Swap => {
            if op.src == op.dst {
                return InvVerdict::Rejected;
            }
            let Some((src_item, src_count)) = cells.cell(op.src) else {
                return InvVerdict::Rejected;
            };
            let Some((dst_item, dst_count)) = cells.cell(op.dst) else {
                return InvVerdict::Rejected;
            };
            if src_count == 0 || dst_count == 0 {
                return InvVerdict::Rejected;
            }
            if !inv_precondition_met(src_item, src_count, op.item, op.count) {
                return InvVerdict::Rejected;
            }
            if !cells.set_cell(op.src, dst_item, dst_count) {
                return InvVerdict::Rejected;
            }
            if !cells.set_cell(op.dst, src_item, src_count) {
                let _ = cells.set_cell(op.src, src_item, src_count);
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Pickup => {
            if op.count == 0 {
                return InvVerdict::Rejected;
            }
            if give_to(cells, op.dst, op.item, op.count) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
        InvOpKind::Set => {
            if cells.set_cell(op.dst, op.item, op.count) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
        InvOpKind::Transaction => InvVerdict::Rejected,
    }
}

#[derive(Debug, Default)]
pub struct MemCells {
    cells: HashMap<InvLoc, (u16, u8)>,
}

impl MemCells {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed(&mut self, loc: InvLoc, item: u16, count: u8) {
        if count == 0 || item == 0 {
            self.cells.remove(&loc);
        } else {
            self.cells.insert(loc, (item, count));
        }
    }

    #[must_use]
    pub fn total(&self, item: u16) -> u32 {
        self.cells
            .values()
            .filter(|(id, _)| *id == item)
            .map(|(_, count)| u32::from(*count))
            .sum()
    }
}

impl InvCells for MemCells {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)> {
        self.cells.get(&loc).copied()
    }

    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool {
        if count == 0 || item == 0 {
            self.cells.remove(&loc);
        } else {
            self.cells.insert(loc, (item, count));
        }
        true
    }
}

#[derive(Debug, Default, Clone)]
pub struct InvLedger {
    cells: HashMap<InventoryTarget, std::sync::Arc<HashMap<(u8, u16), InventoryCell>>>,
    chunk_targets: HashMap<ChunkKey, std::collections::BTreeSet<InventoryTarget>>,
    revisions: HashMap<InventoryTarget, u64>,
    chunks: HashMap<InventoryTarget, Vec<ChunkKey>>,
}

impl InvLedger {
    fn clear_target_chunks(&mut self, target: InventoryTarget) {
        if let Some(chunks) = self.chunks.remove(&target) {
            for chunk in chunks {
                if let Some(targets) = self.chunk_targets.get_mut(&chunk) {
                    targets.remove(&target);
                    if targets.is_empty() { self.chunk_targets.remove(&chunk); }
                }
            }
        }
    }

    fn set_target_chunks(&mut self, target: InventoryTarget, chunks: Vec<ChunkKey>) {
        self.clear_target_chunks(target);
        for &chunk in &chunks { self.chunk_targets.entry(chunk).or_default().insert(target); }
        self.chunks.insert(target, chunks);
    }

    pub fn preflight_chunk_unit(&self, chunks: &std::collections::BTreeSet<ChunkKey>) -> Result<(), InventoryMutationError> {
        for chunk in chunks {
            for target in self.chunk_targets.get(chunk).into_iter().flat_map(|targets| targets.iter()) {
                if self.chunks.get(target).is_none_or(|owned| owned.iter().any(|key| !chunks.contains(key))) {
                    return Err(InventoryMutationError::TargetCollision);
                }
            }
        }
        Ok(())
    }

    pub fn take_chunk_unit(&mut self, chunks: &std::collections::BTreeSet<ChunkKey>) -> Result<Self, InventoryMutationError> {
        let targets: std::collections::BTreeSet<_> = chunks.iter().flat_map(|chunk| self.chunk_targets.get(chunk).into_iter().flat_map(|targets| targets.iter().copied())).collect();
        self.preflight_chunk_unit(chunks)?;
        let mut unit = Self::new();
        for target in targets {
            if let Some(cells) = self.cells.remove(&target) { unit.cells.insert(target, cells); }
            if let Some(revision) = self.revisions.remove(&target) { unit.revisions.insert(target, revision); }
            let owned = self.chunks[&target].clone();
            self.clear_target_chunks(target);
            unit.set_target_chunks(target, owned);
        }
        Ok(unit)
    }

    pub fn can_return_chunk_unit(&self, unit: &Self) -> bool {
        !unit.revisions.keys().any(|target| self.revisions.contains_key(target))
            && !unit.cells.keys().any(|target| self.cells.contains_key(target))
            && !unit.chunks.keys().any(|target| self.chunks.contains_key(target))
    }

    pub fn return_chunk_unit(&mut self, mut unit: Self) -> Result<(), Self> {
        if !self.can_return_chunk_unit(&unit) { return Err(unit); }
        self.cells.extend(unit.cells.drain());
        self.revisions.extend(unit.revisions.drain());
        for (target, chunks) in unit.chunks.drain() { self.set_target_chunks(target, chunks); }
        Ok(())
    }

    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, gid: GlobalPlayerId, loc: InvLoc) -> Option<(u16, u8)> {
        self.cells
            .get(&InventoryTarget::Player(gid))
            .and_then(|cells| cells.get(&(loc.inv, loc.slot)))
            .map(|cell| (cell.item, cell.count))
    }

    pub fn record(&mut self, gid: GlobalPlayerId, loc: InvLoc, item: u16, count: u8) {
        let target = InventoryTarget::Player(gid);
        if count == 0 || item == 0 {
            if let Some(cells) = self.cells.get_mut(&target) { std::sync::Arc::make_mut(cells).remove(&(loc.inv, loc.slot)); }
        } else {
            std::sync::Arc::make_mut(self.cells.entry(target).or_default()).insert(
                (loc.inv, loc.slot),
                InventoryCell {
                    loc,
                    item,
                    count,
                    nbt: Vec::new(),
                },
            );
        }
    }

    pub fn apply_op(&mut self, op: &InventoryOp) -> InvVerdict {
        self.apply_op_result(op)
            .map_or(InvVerdict::Rejected, |_| InvVerdict::Applied)
    }

    pub fn apply_op_result(
        &mut self,
        op: &InventoryOp,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        if op.kind == InvOpKind::Transaction {
            return self.apply_transaction(op);
        }
        let player = op.player_target.ok_or(InventoryApplyError::Rejected)?;
        let target = InventoryTarget::Player(player);
        let from_revision = self.revisions.get(&target).copied().unwrap_or_default();
        let mut view = LedgerView {
            ledger: self,
            gid: player,
        };
        let verdict = replay(&mut view, op);
        if verdict == InvVerdict::Applied {
            let revision = self.revisions.entry(target).or_default();
            *revision = revision.wrapping_add(1);
            return Ok(InventoryDelta {
                revisions: vec![(target, from_revision, *revision)],
                operations: op.changes.clone(),
            });
        }
        Err(InventoryApplyError::Rejected)
    }

    pub fn rebase_gesture_op(
        &self,
        op: &InventoryOp,
    ) -> Result<InventoryOp, InventoryGestureError> {
        if op.gestures.is_empty()
            || op.expected_revisions.iter().all(|(target, expected)| {
                self.revisions.get(target).copied().unwrap_or_default() == *expected
            })
        {
            return Ok(op.clone());
        }
        let mut targets = Vec::new();
        for gesture in &op.gestures {
            gesture.push_targets(&mut targets);
        }
        targets.sort_unstable();
        targets.dedup();
        let snapshots = targets
            .into_iter()
            .map(|target| {
                self.snapshot(target)
                    .ok_or(InventoryGestureError::MissingInventory)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reduction = reduce_inventory_gestures(&snapshots, &op.gestures)?;
        let mut rebased = capture_inventory_mutation_with_effects(
            op.actor,
            op.seq,
            op.tick,
            reduction.mutation.expected_revisions,
            reduction.mutation.operations,
            reduction.effects,
            op.actor_chunk,
            op.affected.clone(),
        );
        rebased.gestures = op.gestures.clone();
        rebased.menu_effects = op.menu_effects.clone();
        rebased.dependencies = op.dependencies.clone();
        rebased.actor_chunk = op.actor_chunk;
        Ok(rebased)
    }

    fn apply_transaction(
        &mut self,
        op: &InventoryOp,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        self.apply_mutation(InventoryMutation {
            expected_revisions: op.expected_revisions.clone(),
            operations: op.changes.clone(),
        })
    }

    pub fn apply_mutation(
        &mut self,
        mutation: InventoryMutation,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        if mutation.expected_revisions.is_empty() {
            return Err(InventoryApplyError::Rejected);
        }
        let mut service = InventoryService::new();
        for (target, revision) in &mutation.expected_revisions {
            let current_revision = self
                .revisions
                .get(target)
                .copied()
                .ok_or(InventoryApplyError::Rejected)?;
            if current_revision != *revision {
                return Err(InventoryApplyError::Rejected);
            }
            let cells = self.cells.get(target).into_iter().flat_map(|cells| cells.values().cloned()).collect();
            if service.install(*target, current_revision, cells).is_err() {
                return Err(InventoryApplyError::Rejected);
            }
        }
        let delta = service
            .mutate(mutation)
            .map_err(|_| InventoryApplyError::Rejected)?;
        for change in &delta.operations {
            let key = (change.after.loc.inv, change.after.loc.slot);
            let cells = std::sync::Arc::make_mut(self.cells.entry(change.target).or_default());
            if change.after.is_empty() {
                cells.remove(&key);
            } else {
                cells.insert(key, change.after.clone());
            }
        }
        for (target, _, revision) in &delta.revisions {
            self.revisions.insert(*target, *revision);
        }
        Ok(delta)
    }

    pub fn install_snapshot(
        &mut self,
        snapshot: InventoryStateSnapshot,
    ) -> Result<(), InventoryMutationError> {
        let mut service = InventoryService::new();
        service.install(snapshot.target, snapshot.revision, snapshot.cells.clone())?;
        self.cells.remove(&snapshot.target);
        for cell in snapshot.cells {
            if !cell.is_empty() {
                std::sync::Arc::make_mut(self.cells.entry(snapshot.target).or_default())
                    .insert((cell.loc.inv, cell.loc.slot), cell);
            }
        }
        self.revisions.insert(snapshot.target, snapshot.revision);
        Ok(())
    }

    pub fn install_owned_snapshot(
        &mut self,
        snapshot: InventoryStateSnapshot,
        mut chunks: Vec<ChunkKey>,
    ) -> Result<(), InventoryMutationError> {
        chunks.extend(snapshot.target.intrinsic_chunk());
        chunks.sort_unstable();
        chunks.dedup();
        if chunks.is_empty() {
            return Err(InventoryMutationError::InvalidLocation);
        }
        let target = snapshot.target;
        self.install_snapshot(snapshot)?;
        self.set_target_chunks(target, chunks);
        Ok(())
    }

    pub fn relocate_target(
        &mut self,
        target: InventoryTarget,
        mut chunks: Vec<ChunkKey>,
    ) -> Result<(), InventoryMutationError> {
        if !self.revisions.contains_key(&target) {
            return Err(InventoryMutationError::MissingInventory);
        }
        chunks.extend(target.intrinsic_chunk());
        chunks.sort_unstable();
        chunks.dedup();
        if chunks.is_empty() {
            return Err(InventoryMutationError::InvalidLocation);
        }
        self.set_target_chunks(target, chunks);
        Ok(())
    }

    pub fn remove_target(&mut self, target: InventoryTarget) -> Option<InventoryStateSnapshot> {
        let snapshot = self.snapshot(target)?;
        self.cells.remove(&target);
        self.revisions.remove(&target);
        self.clear_target_chunks(target);
        Some(snapshot)
    }

    pub fn rename_targets(
        &mut self,
        renames: &[(InventoryTarget, InventoryTarget)],
    ) -> Result<(), InventoryMutationError> {
        let mut sources = std::collections::HashSet::new();
        let mut destinations = std::collections::HashSet::new();
        for &(source, destination) in renames {
            if !self.revisions.contains_key(&source) {
                return Err(InventoryMutationError::MissingInventory);
            }
            if !destination.is_valid()
                || !sources.insert(source)
                || !destinations.insert(destination)
            {
                return Err(InventoryMutationError::TargetCollision);
            }
        }
        for &destination in &destinations {
            if self.revisions.contains_key(&destination) && !sources.contains(&destination) {
                return Err(InventoryMutationError::TargetCollision);
            }
        }
        let moved: Vec<_> = renames.iter().map(|&(source, destination)| {
            let cells = self.cells.remove(&source);
            let revision = self.revisions.remove(&source).unwrap();
            let chunks = self.chunks.get(&source).cloned();
            self.clear_target_chunks(source);
            (destination, cells, revision, chunks)
        }).collect();
        for (destination, cells, revision, chunks) in moved {
            if let Some(cells) = cells { self.cells.insert(destination, cells); }
            self.revisions.insert(destination, revision);
            if let Some(chunks) = chunks { self.set_target_chunks(destination, chunks); }
        }

        Ok(())
    }

    #[must_use]
    pub fn target_chunks(&self, target: InventoryTarget) -> Option<&[ChunkKey]> {
        self.chunks.get(&target).map(Vec::as_slice)
    }

    #[must_use]
    pub fn targets_for_chunk(&self, chunk: ChunkKey) -> Vec<InventoryTarget> {
        self.chunk_targets.get(&chunk).into_iter().flat_map(|targets| targets.iter().copied()).collect()
    }

    #[must_use]
    pub fn targets(&self) -> Vec<InventoryTarget> {
        let mut targets: Vec<_> = self.revisions.keys().copied().collect();
        targets.sort_unstable();
        targets
    }

    pub fn target_keys(&self) -> impl Iterator<Item = InventoryTarget> + '_ {
        self.revisions.keys().copied()
    }

    pub fn revision(&self, target: impl Into<InventoryTarget>) -> Option<u64> {
        self.revisions.get(&target.into()).copied()
    }

    #[must_use]
    pub fn snapshot(&self, target: impl Into<InventoryTarget>) -> Option<InventoryStateSnapshot> {
        let target = target.into();
        let revision = self.revisions.get(&target).copied()?;
        let mut cells: Vec<_> = self.cells.get(&target).into_iter().flat_map(|cells| cells.values().cloned()).collect();
        cells.sort_by_key(|cell| (cell.loc.inv, cell.loc.slot));
        Some(InventoryStateSnapshot {
            target,
            revision,
            cells,
        })
    }

    pub fn check_place(
        &mut self,
        gid: GlobalPlayerId,
        loc: InvLoc,
        item: u16,
        count_before: u8,
        count_after: u8,
    ) -> InvVerdict {
        match self.get(gid, loc) {
            None => {
                self.record(gid, loc, item, count_after);
                InvVerdict::Applied
            }
            Some((have_item, have_count)) => {
                if !inv_precondition_met(have_item, have_count, item, count_before) {
                    return InvVerdict::Rejected;
                }
                self.record(gid, loc, item, count_after);
                InvVerdict::Applied
            }
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.values().map(|cells| cells.len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.values().all(|cells| cells.is_empty())
    }
}

struct LedgerView<'ledger> {
    ledger: &'ledger mut InvLedger,
    gid: GlobalPlayerId,
}

impl InvCells for LedgerView<'_> {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)> {
        self.ledger.get(self.gid, loc)
    }

    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool {
        self.ledger.record(self.gid, loc, item, count);
        true
    }
}

#[must_use]
pub fn capture_inv_op(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    kind: InvOpKind,
    src: InvLoc,
    dst: InvLoc,
    item: u16,
    count: u8,
    actor_chunk: crate::protocol::ChunkKey,
    mut affected: Vec<crate::protocol::ChunkKey>,
) -> InventoryOp {
    if !affected.contains(&actor_chunk) {
        affected.push(actor_chunk);
    }
    InventoryOp {
        actor: ActorId::Player(gid),
        player_gid: Some(gid),
        player_target: Some(gid),
        seq,
        tick,
        kind,
        src,
        dst,
        item,
        count,
        nbt: Vec::new(),
        expected_revision: None,
        expected_revisions: Vec::new(),
        changes: Vec::new(),
        gestures: Vec::new(),
        effects: Vec::new(),
        menu_effects: Vec::new(),
        actor_chunk,
        affected,
        dependencies: Vec::new(),
    }
}

#[must_use]
pub fn capture_inv_set(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    dst: InvLoc,
    item: u16,
    count: u8,
    nbt: Vec<u8>,
    actor_chunk: crate::protocol::ChunkKey,
    mut affected: Vec<crate::protocol::ChunkKey>,
) -> InventoryOp {
    if !affected.contains(&actor_chunk) {
        affected.push(actor_chunk);
    }
    InventoryOp {
        actor: ActorId::Player(gid),
        player_gid: Some(gid),
        player_target: Some(gid),
        seq,
        tick,
        kind: InvOpKind::Set,
        src: dst,
        dst,
        item,
        count,
        nbt,
        expected_revision: None,
        expected_revisions: Vec::new(),
        changes: Vec::new(),
        gestures: Vec::new(),
        effects: Vec::new(),
        menu_effects: Vec::new(),
        actor_chunk,
        affected,
        dependencies: Vec::new(),
    }
}

#[must_use]
pub fn capture_inventory_transaction(
    actor: impl Into<ActorId>,
    target: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    expected_revision: u64,
    changes: Vec<SemanticInventoryOp>,
    actor_chunk: crate::protocol::ChunkKey,
    affected: Vec<crate::protocol::ChunkKey>,
) -> InventoryOp {
    let inventory = InventoryTarget::Player(target);
    let changes = changes
        .into_iter()
        .map(|mut change| {
            change.target = inventory;
            change
        })
        .collect();
    capture_inventory_mutation(
        actor,
        seq,
        tick,
        vec![(inventory, expected_revision)],
        changes,
        actor_chunk,
        affected,
    )
}

#[must_use]
pub fn capture_inventory_mutation(
    actor: impl Into<ActorId>,
    seq: PlayerSeq,
    tick: TickStamp,
    expected_revisions: Vec<(InventoryTarget, u64)>,
    changes: Vec<SemanticInventoryOp>,
    actor_chunk: crate::protocol::ChunkKey,
    affected: Vec<crate::protocol::ChunkKey>,
) -> InventoryOp {
    capture_inventory_mutation_with_effects(
        actor,
        seq,
        tick,
        expected_revisions,
        changes,
        Vec::new(),
        actor_chunk,
        affected,
    )
}

#[must_use]
pub fn capture_inventory_mutation_with_effects(
    actor: impl Into<ActorId>,
    seq: PlayerSeq,
    tick: TickStamp,
    expected_revisions: Vec<(InventoryTarget, u64)>,
    changes: Vec<SemanticInventoryOp>,
    effects: Vec<InventoryGestureEffect>,
    actor_chunk: crate::protocol::ChunkKey,
    mut affected: Vec<crate::protocol::ChunkKey>,
) -> InventoryOp {
    let actor = actor.into();
    affected.push(actor_chunk);
    affected.extend(
        expected_revisions
            .iter()
            .filter_map(|(target, _)| target.intrinsic_chunk()),
    );
    affected.extend(
        changes
            .iter()
            .filter_map(|change| change.target.intrinsic_chunk()),
    );
    affected.sort_unstable();
    affected.dedup();
    let (target, expected_revision) = expected_revisions
        .iter()
        .find_map(|(target, revision)| match target {
            InventoryTarget::Player(player) => Some((*player, Some(*revision))),
            _ => None,
        })
        .map_or((None, None), |(player, revision)| (Some(player), revision));
    InventoryOp {
        actor,
        player_gid: match actor {
            ActorId::Player(player) => Some(player),
            ActorId::Entity(_) | ActorId::Peer(_) => None,
        },
        player_target: target,
        seq,
        tick,
        kind: InvOpKind::Transaction,
        src: InvLoc::new(INV_MAIN, 0),
        dst: InvLoc::new(INV_MAIN, 0),
        item: 0,
        count: 0,
        nbt: Vec::new(),
        expected_revision,
        expected_revisions,
        changes,
        gestures: Vec::new(),
        effects,
        menu_effects: Vec::new(),
        actor_chunk,
        affected,
        dependencies: Vec::new(),
    }
}

fn inventory_touches(op: &InventoryOp) -> Vec<(InventoryTarget, InvLoc)> {
    if op.kind == InvOpKind::Transaction {
        let mut touched: Vec<_> = op
            .changes
            .iter()
            .map(|change| (change.target, change.before.loc))
            .collect();
        let mut unique = Vec::with_capacity(touched.len());
        for cell in touched.drain(..) {
            if !unique.contains(&cell) {
                unique.push(cell);
            }
        }
        touched = unique;
        return touched;
    }
    let Some(player) = op.player_target else {
        return Vec::new();
    };
    let target = InventoryTarget::Player(player);
    let mut touched = vec![(target, op.src), (target, op.dst)];
    touched.sort_by_key(|(_, loc)| (loc.inv, loc.slot));
    touched.dedup();
    touched
}

pub fn link_inventory_dependencies(operations: &mut [InventoryOp]) {
    for index in 0..operations.len() {
        let touched = inventory_touches(&operations[index]);
        let mut dependencies = Vec::new();
        for prior in &operations[..index] {
            let prior_touched = inventory_touches(prior);
            if touched.iter().any(|cell| prior_touched.contains(cell)) {
                dependencies.push(crate::completion::ActionKey::inventory(prior));
            }
        }
        dependencies.sort_unstable();
        dependencies.dedup();
        operations[index].dependencies = dependencies;
    }
}

#[must_use]
pub fn dependent_inventory_actions(
    operations: &[InventoryOp],
    rejected: crate::completion::ActionKey,
) -> Vec<crate::completion::ActionKey> {
    let mut affected = vec![rejected];
    for operation in operations {
        if operation
            .dependencies
            .iter()
            .any(|dependency| affected.contains(dependency))
        {
            affected.push(crate::completion::ActionKey::inventory(operation));
        }
    }
    affected.sort_unstable();
    affected.dedup();
    affected
}

#[must_use]
pub fn corrective_inventory_delta(
    operation: &InventoryOp,
    authoritative: &[InventoryStateSnapshot],
) -> InventoryDelta {
    let mut revisions = Vec::new();
    for snapshot in authoritative {
        let from = operation
            .expected_revisions
            .iter()
            .find_map(|(target, revision)| (*target == snapshot.target).then_some(*revision))
            .unwrap_or(snapshot.revision);
        revisions.push((snapshot.target, from, snapshot.revision));
    }
    let mut corrections = Vec::new();
    for predicted in &operation.changes {
        let Some(snapshot) = authoritative
            .iter()
            .find(|snapshot| snapshot.target == predicted.target)
        else {
            continue;
        };
        let actual = snapshot
            .cells
            .iter()
            .find(|cell| cell.loc == predicted.after.loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(predicted.after.loc));
        if predicted.after != actual {
            corrections.push(SemanticInventoryOp {
                target: predicted.target,
                before: predicted.after.clone(),
                after: actual,
            });
        }
    }
    InventoryDelta {
        revisions,
        operations: corrections,
    }
}

pub fn sort_inv_ops_for_tick(tick: TickStamp, ops: &mut [InventoryOp]) {
    sort_inv_ops_for_tick_with_seed(crate::protocol::INV_OP_SEED, tick, ops);
}

pub fn sort_inv_ops_for_tick_with_seed(seed: u64, tick: TickStamp, ops: &mut [InventoryOp]) {
    ops.sort_by(|left, right| {
        crate::order::order_actors(seed, tick, left.actor, right.actor)
            .then_with(|| left.seq.0.cmp(&right.seq.0))
            .then_with(|| {
                (left.src.inv, left.src.slot, left.dst.inv, left.dst.slot).cmp(&(
                    right.src.inv,
                    right.src.slot,
                    right.dst.inv,
                    right.dst.slot,
                ))
            })
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InventorySlotRef {
    pub target: InventoryTarget,
    pub loc: InvLoc,
}

impl InventorySlotRef {
    #[must_use]
    pub const fn new(target: InventoryTarget, loc: InvLoc) -> Self {
        Self { target, loc }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryRemovalKind {
    Drop,
    Destroy,
    Consume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuickCraftMode {
    Even,
    OneEach,
    Fill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InventoryScreenSession {
    pub actor: GlobalPlayerId,
    pub session: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryScreenSlot {
    pub screen_slot: u16,
    pub slot: InventorySlotRef,
    pub cell: InventoryCell,
    pub limit: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryScreenSnapshot {
    pub session: InventoryScreenSession,
    pub sync_id: u8,
    pub state_id: i32,
    pub window_kind: u16,
    pub container_slots: u16,
    pub slots: Vec<InventoryScreenSlot>,
    pub cursor: InventoryCell,
    pub affected: Vec<ChunkKey>,
    pub preview: Option<InventoryStateSnapshot>,
    pub preview_incarnation: Option<u64>,
    pub preview_lease: Option<crate::canonical_action::LivePlayerLease>,
    pub preview_authority: Option<crate::invsee::InventoryPreviewAuthority>,
}

#[must_use]
pub fn inventory_screen_targets(screen: &InventoryScreenSnapshot) -> Vec<InventoryTarget> {
    let mut targets: Vec<_> = screen.slots.iter().map(|slot| slot.slot.target).collect();
    targets.push(InventoryTarget::Player(screen.session.actor));
    targets.sort_unstable();
    targets.dedup();
    targets
}

pub fn rename_inventory_screen_targets(
    screen: &mut InventoryScreenSnapshot,
    renames: &[(InventoryTarget, InventoryTarget)],
) {
    let replacements: HashMap<_, _> = renames.iter().copied().collect();
    for slot in &mut screen.slots {
        slot.slot.target = replacements
            .get(&slot.slot.target)
            .copied()
            .unwrap_or(slot.slot.target);
    }
    if let Some(preview) = &mut screen.preview {
        preview.target = replacements
            .get(&preview.target)
            .copied()
            .unwrap_or(preview.target);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InventoryQuickCraft {
    source: InventorySlotRef,
    destinations: Vec<(InventorySlotRef, u8)>,
    mode: QuickCraftMode,
}

#[derive(Debug, Default)]
pub struct InventoryScreenSessions {
    quick_craft: HashMap<InventoryScreenSession, InventoryQuickCraft>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryScreenCommand {
    BeginQuickCraft {
        session: InventoryScreenSession,
        source: InventorySlotRef,
        mode: QuickCraftMode,
    },
    AddQuickCraftSlot {
        session: InventoryScreenSession,
        destination: InventorySlotRef,
        limit: u8,
    },
    FinishQuickCraft {
        session: InventoryScreenSession,
    },
    Close {
        session: InventoryScreenSession,
    },
}

impl InventoryScreenSessions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_quick_craft(
        &mut self,
        session: InventoryScreenSession,
        source: InventorySlotRef,
        mode: QuickCraftMode,
    ) {
        self.quick_craft.insert(
            session,
            InventoryQuickCraft {
                source,
                destinations: Vec::new(),
                mode,
            },
        );
    }

    pub fn add_quick_craft_slot(
        &mut self,
        session: InventoryScreenSession,
        destination: InventorySlotRef,
        limit: u8,
    ) -> bool {
        let Some(state) = self.quick_craft.get_mut(&session) else {
            return false;
        };
        if !state
            .destinations
            .iter()
            .any(|(current, _)| *current == destination)
        {
            state.destinations.push((destination, limit));
        }
        true
    }

    pub fn finish_quick_craft(
        &mut self,
        session: InventoryScreenSession,
    ) -> Option<InventoryGesture> {
        let mut state = self.quick_craft.remove(&session)?;
        state
            .destinations
            .sort_unstable_by_key(|(slot, _)| (slot.target, slot.loc.inv, slot.loc.slot));
        Some(InventoryGesture::QuickCraft {
            source: state.source,
            destinations: state.destinations,
            mode: state.mode,
        })
    }

    pub fn close(&mut self, session: InventoryScreenSession) {
        self.quick_craft.remove(&session);
    }

    pub fn rename_targets(&mut self, renames: &[(InventoryTarget, InventoryTarget)]) {
        let replacements: HashMap<_, _> = renames.iter().copied().collect();
        for state in self.quick_craft.values_mut() {
            state.source.target = replacements
                .get(&state.source.target)
                .copied()
                .unwrap_or(state.source.target);
            for (slot, _) in &mut state.destinations {
                slot.target = replacements
                    .get(&slot.target)
                    .copied()
                    .unwrap_or(slot.target);
            }
        }
    }

    pub fn apply(&mut self, command: InventoryScreenCommand) -> Option<InventoryGesture> {
        match command {
            InventoryScreenCommand::BeginQuickCraft {
                session,
                source,
                mode,
            } => {
                self.begin_quick_craft(session, source, mode);
                None
            }
            InventoryScreenCommand::AddQuickCraftSlot {
                session,
                destination,
                limit,
            } => {
                self.add_quick_craft_slot(session, destination, limit);
                None
            }
            InventoryScreenCommand::FinishQuickCraft { session } => {
                self.finish_quick_craft(session)
            }
            InventoryScreenCommand::Close { session } => {
                self.close(session);
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipeIngredient {
    pub slot: InventorySlotRef,
    pub expected: InventoryCell,
    pub count_per_craft: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipePlacementIngredient {
    pub grid_slot: InventorySlotRef,
    pub acceptable_items: Vec<u16>,
    pub count_per_craft: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryTransferCount {
    Exact(u8),
    All,
    Half,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryGesture {
    ExpectIdentity {
        slot: InventorySlotRef,
        item: u16,
        components: Vec<u8>,
    },
    DropSelectedHand {
        player: InventoryTarget,
        count: InventoryTransferCount,
    },
    RemoveBy {
        source: InventorySlotRef,
        count: InventoryTransferCount,
        kind: InventoryRemovalKind,
    },
    SwapSelectedHand {
        player: InventoryTarget,
    },
    SelectHotbar {
        slot: InventorySlotRef,
        selected: u8,
    },
    Expect {
        slot: InventorySlotRef,
        cell: InventoryCell,
    },
    ExactSet {
        destination: InventorySlotRef,
        before: InventoryCell,
        after: InventoryCell,
    },
    Transfer {
        source: InventorySlotRef,
        destination: InventorySlotRef,
        count: u8,
        destination_limit: u8,
    },
    TransferBy {
        source: InventorySlotRef,
        destination: InventorySlotRef,
        count: InventoryTransferCount,
        destination_limit: u8,
    },
    ExchangeCursor {
        slot: InventorySlotRef,
        cursor: InventorySlotRef,
        single: bool,
        slot_limit: u8,
        cursor_limit: u8,
    },
    Swap {
        first: InventorySlotRef,
        second: InventorySlotRef,
        first_limit: u8,
        second_limit: u8,
    },
    Remove {
        source: InventorySlotRef,
        count: u8,
        kind: InventoryRemovalKind,
    },
    CreativeClone {
        source: InventorySlotRef,
        destination: InventorySlotRef,
        destination_limit: u8,
    },
    CreativeSet {
        destination: InventorySlotRef,
        stack: InventoryCell,
        destination_limit: u8,
    },
    CreativeDrop {
        owner: InventoryTarget,
        stack: InventoryCell,
    },
    QuickMove {
        source: InventorySlotRef,
        destinations: Vec<(InventorySlotRef, u8)>,
    },
    DrainToPlayer {
        player: GlobalPlayerId,
        source: InventorySlotRef,
        destinations: Vec<(InventorySlotRef, u8)>,
    },
    ClearResult {
        slot: InventorySlotRef,
    },
    QuickCraft {
        source: InventorySlotRef,
        destinations: Vec<(InventorySlotRef, u8)>,
        mode: QuickCraftMode,
    },
    PickupAll {
        destination: InventorySlotRef,
        sources: Vec<InventorySlotRef>,
        destination_limit: u8,
    },
    RecipeCraft {
        ingredients: Vec<RecipeIngredient>,
        destination: InventorySlotRef,
        result: InventoryCell,
        repetitions: u8,
        destination_limit: u8,
    },
    PlaceRecipe {
        player: InventoryTarget,
        grid: InventoryTarget,
        ingredients: Vec<RecipePlacementIngredient>,
        grid_slots: Vec<InventorySlotRef>,
        player_sources: Vec<InventorySlotRef>,
        use_max: bool,
    },
}

impl InventoryGesture {
    pub fn rename_targets(&mut self, renames: &[(InventoryTarget, InventoryTarget)]) {
        let replacements: HashMap<_, _> = renames.iter().copied().collect();
        let remap_target = |target: &mut InventoryTarget| {
            *target = replacements.get(target).copied().unwrap_or(*target);
        };
        let remap_slot = |slot: &mut InventorySlotRef| {
            slot.target = replacements
                .get(&slot.target)
                .copied()
                .unwrap_or(slot.target);
        };
        match self {
            Self::CreativeClone {
                source,
                destination,
                ..
            } => {
                remap_slot(source);
                remap_slot(destination);
            }
            Self::SwapSelectedHand { player } | Self::DropSelectedHand { player, .. } => {
                remap_target(player)
            }
            Self::RemoveBy { source, .. } => remap_slot(source),
            Self::SelectHotbar { slot, .. }
            | Self::ExpectIdentity { slot, .. }
            | Self::Expect { slot, .. }
            | Self::ClearResult { slot } => remap_slot(slot),
            Self::ExactSet { destination, .. } | Self::CreativeSet { destination, .. } => {
                remap_slot(destination);
            }
            Self::Transfer {
                source,
                destination,
                ..
            }
            | Self::TransferBy {
                source,
                destination,
                ..
            } => {
                remap_slot(source);
                remap_slot(destination);
            }
            Self::Swap { first, second, .. } => {
                remap_slot(first);
                remap_slot(second);
            }
            Self::ExchangeCursor { slot, cursor, .. } => {
                remap_slot(slot);
                remap_slot(cursor);
            }
            Self::Remove { source, .. } => remap_slot(source),
            Self::CreativeDrop { owner, .. } => remap_target(owner),
            Self::QuickMove {
                source,
                destinations,
            }
            | Self::QuickCraft {
                source,
                destinations,
                ..
            } => {
                remap_slot(source);
                for (destination, _) in destinations {
                    remap_slot(destination);
                }
            }
            Self::DrainToPlayer {
                source,
                destinations,
                ..
            } => {
                remap_slot(source);
                for (destination, _) in destinations {
                    remap_slot(destination);
                }
            }
            Self::PickupAll {
                destination,
                sources,
                ..
            } => {
                remap_slot(destination);
                for source in sources {
                    remap_slot(source);
                }
            }
            Self::RecipeCraft {
                ingredients,
                destination,
                ..
            } => {
                for ingredient in ingredients {
                    remap_slot(&mut ingredient.slot);
                }
                remap_slot(destination);
            }
            Self::PlaceRecipe {
                player,
                grid,
                ingredients,
                grid_slots,
                player_sources,
                ..
            } => {
                remap_target(player);
                remap_target(grid);
                for ingredient in ingredients {
                    remap_slot(&mut ingredient.grid_slot);
                }
                for slot in grid_slots.iter_mut().chain(player_sources) {
                    remap_slot(slot);
                }
            }
        }
    }

    #[must_use]
    pub fn targets(&self) -> Vec<InventoryTarget> {
        let mut targets = Vec::new();
        self.push_targets(&mut targets);
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    #[must_use]
    pub fn slots(&self) -> Vec<InventorySlotRef> {
        let mut slots = match self {
            Self::CreativeClone {
                source,
                destination,
                ..
            } => vec![*source, *destination],
            Self::SwapSelectedHand { player } | Self::DropSelectedHand { player, .. } => (0..9)
                .map(|slot| InventorySlotRef::new(*player, InvLoc::new(INV_MAIN, slot)))
                .chain([
                    InventorySlotRef::new(*player, InvLoc::new(INV_SELECTED, 0)),
                    InventorySlotRef::new(*player, InvLoc::new(INV_OFFHAND, 0)),
                ])
                .collect(),
            Self::SelectHotbar { slot, .. }
            | Self::ExpectIdentity { slot, .. }
            | Self::Expect { slot, .. }
            | Self::ClearResult { slot } => vec![*slot],
            Self::ExactSet { destination, .. } | Self::CreativeSet { destination, .. } => {
                vec![*destination]
            }
            Self::Transfer {
                source,
                destination,
                ..
            }
            | Self::TransferBy {
                source,
                destination,
                ..
            }
            | Self::Swap {
                first: source,
                second: destination,
                ..
            } => vec![*source, *destination],
            Self::ExchangeCursor { slot, cursor, .. } => vec![*slot, *cursor],
            Self::Remove { source, .. } | Self::RemoveBy { source, .. } => vec![*source],
            Self::CreativeDrop { .. } => Vec::new(),
            Self::QuickMove {
                source,
                destinations,
            }
            | Self::DrainToPlayer {
                source,
                destinations,
                ..
            }
            | Self::QuickCraft {
                source,
                destinations,
                ..
            } => std::iter::once(*source)
                .chain(destinations.iter().map(|(slot, _)| *slot))
                .collect(),
            Self::PickupAll {
                destination,
                sources,
                ..
            } => std::iter::once(*destination)
                .chain(sources.iter().copied())
                .collect(),
            Self::RecipeCraft {
                ingredients,
                destination,
                ..
            } => std::iter::once(*destination)
                .chain(ingredients.iter().map(|ingredient| ingredient.slot))
                .collect(),
            Self::PlaceRecipe {
                ingredients,
                grid_slots,
                player_sources,
                ..
            } => ingredients
                .iter()
                .map(|ingredient| ingredient.grid_slot)
                .chain(grid_slots.iter().copied())
                .chain(player_sources.iter().copied())
                .collect(),
        };
        slots.sort_unstable();
        slots.dedup();
        slots
    }

    fn push_targets(&self, targets: &mut Vec<InventoryTarget>) {
        match self {
            Self::ExchangeCursor { slot, cursor, .. } => {
                targets.push(slot.target);
                targets.push(cursor.target);
            }
            Self::RemoveBy { source, .. } => targets.push(source.target),
            Self::SwapSelectedHand { player } | Self::DropSelectedHand { player, .. } => {
                targets.push(*player)
            }
            Self::SelectHotbar { slot, .. }
            | Self::ExpectIdentity { slot, .. }
            | Self::Expect { slot, .. } => targets.push(slot.target),
            Self::ExactSet { destination, .. } => targets.push(destination.target),
            Self::Transfer {
                source,
                destination,
                ..
            }
            | Self::TransferBy {
                source,
                destination,
                ..
            }
            | Self::Swap {
                first: source,
                second: destination,
                ..
            } => {
                targets.push(source.target);
                targets.push(destination.target);
            }
            Self::Remove { source, .. } => targets.push(source.target),
            Self::CreativeClone {
                source,
                destination,
                ..
            } => {
                targets.push(source.target);
                targets.push(destination.target);
            }
            Self::CreativeSet { destination, .. } => targets.push(destination.target),
            Self::CreativeDrop { owner, .. } => targets.push(*owner),
            Self::QuickMove {
                source,
                destinations,
            }
            | Self::DrainToPlayer {
                source,
                destinations,
                ..
            }
            | Self::QuickCraft {
                source,
                destinations,
                ..
            } => {
                targets.push(source.target);
                targets.extend(destinations.iter().map(|(slot, _)| slot.target));
            }
            Self::ClearResult { slot } => targets.push(slot.target),
            Self::PickupAll {
                destination,
                sources,
                ..
            } => {
                targets.push(destination.target);
                targets.extend(sources.iter().map(|slot| slot.target));
            }
            Self::RecipeCraft {
                ingredients,
                destination,
                ..
            } => {
                targets.push(destination.target);
                targets.extend(ingredients.iter().map(|ingredient| ingredient.slot.target));
            }
            Self::PlaceRecipe {
                player,
                grid,
                ingredients,
                grid_slots,
                player_sources,
                ..
            } => {
                targets.push(*player);
                targets.push(*grid);
                targets.extend(
                    ingredients
                        .iter()
                        .map(|ingredient| ingredient.grid_slot.target),
                );
                targets.extend(grid_slots.iter().map(|slot| slot.target));
                targets.extend(player_sources.iter().map(|slot| slot.target));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryGestureEffect {
    Removed {
        source: InventorySlotRef,
        stack: InventoryCell,
        kind: InventoryRemovalKind,
    },
    Crafted {
        destination: InventorySlotRef,
        stack: InventoryCell,
        repetitions: u8,
    },
    CreatedDrop {
        owner: InventoryTarget,
        stack: InventoryCell,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryGestureReduction {
    pub mutation: InventoryMutation,
    pub effects: Vec<InventoryGestureEffect>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryGestureError {
    MissingInventory,
    DuplicateInventory,
    InvalidStack,
    InvalidCount,
    IncompatibleStack,
    InsufficientItems,
    CapacityExceeded,
}

#[derive(Clone)]
struct GestureInventory {
    revision: u64,
    initial: HashMap<InvLoc, InventoryCell>,
    current: HashMap<InvLoc, InventoryCell>,
}

fn gesture_cell(inventory: &GestureInventory, loc: InvLoc) -> InventoryCell {
    inventory
        .current
        .get(&loc)
        .cloned()
        .unwrap_or_else(|| InventoryCell::empty(loc))
}

fn gesture_inventory_mut(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    slot: InventorySlotRef,
) -> Result<&mut GestureInventory, InventoryGestureError> {
    inventories
        .get_mut(&slot.target)
        .ok_or(InventoryGestureError::MissingInventory)
}

fn gesture_set_cell(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    slot: InventorySlotRef,
    mut cell: InventoryCell,
) -> Result<(), InventoryGestureError> {
    cell.loc = slot.loc;
    if !valid_cell(&cell) {
        return Err(InventoryGestureError::InvalidStack);
    }
    let inventory = gesture_inventory_mut(inventories, slot)?;
    if cell.is_empty() {
        inventory.current.remove(&slot.loc);
    } else {
        inventory.current.insert(slot.loc, cell);
    }
    Ok(())
}

fn gesture_stack_matches(left: &InventoryCell, right: &InventoryCell) -> bool {
    !left.is_empty() && !right.is_empty() && left.item == right.item && left.nbt == right.nbt
}

fn gesture_transfer(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    source: InventorySlotRef,
    destination: InventorySlotRef,
    count: u8,
    destination_limit: u8,
) -> Result<(), InventoryGestureError> {
    if count == 0 || destination_limit == 0 {
        return Err(InventoryGestureError::InvalidCount);
    }
    if source == destination {
        return Ok(());
    }
    let source_cell = gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc);
    if source_cell.is_empty() || source_cell.count < count {
        return Err(InventoryGestureError::InsufficientItems);
    }
    let destination_cell = gesture_cell(
        gesture_inventory_mut(inventories, destination)?,
        destination.loc,
    );
    if !destination_cell.is_empty() && !gesture_stack_matches(&source_cell, &destination_cell) {
        return Err(InventoryGestureError::IncompatibleStack);
    }
    if destination_cell.count.saturating_add(count) > destination_limit {
        return Err(InventoryGestureError::CapacityExceeded);
    }
    let mut next_source = source_cell.clone();
    next_source.count -= count;
    if next_source.count == 0 {
        next_source = InventoryCell::empty(source.loc);
    }
    let mut next_destination = source_cell;
    next_destination.loc = destination.loc;
    next_destination.count = destination_cell.count + count;
    gesture_set_cell(inventories, source, next_source)?;
    gesture_set_cell(inventories, destination, next_destination)
}

fn gesture_remove(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    source: InventorySlotRef,
    count: u8,
) -> Result<InventoryCell, InventoryGestureError> {
    if count == 0 {
        return Err(InventoryGestureError::InvalidCount);
    }
    let source_cell = gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc);
    if source_cell.is_empty() || source_cell.count < count {
        return Err(InventoryGestureError::InsufficientItems);
    }
    let mut removed = source_cell.clone();
    removed.count = count;
    let mut remaining = source_cell;
    remaining.count -= count;
    if remaining.count == 0 {
        remaining = InventoryCell::empty(source.loc);
    }
    gesture_set_cell(inventories, source, remaining)?;
    Ok(removed)
}

fn gesture_quick_move(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    source: InventorySlotRef,
    destinations: &[(InventorySlotRef, u8)],
) -> Result<(), InventoryGestureError> {
    if destinations.is_empty() {
        return Err(InventoryGestureError::InvalidCount);
    }
    for (destination, limit) in destinations {
        let source_cell = gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc);
        if source_cell.is_empty() {
            return Ok(());
        }
        let destination_cell = gesture_cell(
            gesture_inventory_mut(inventories, *destination)?,
            destination.loc,
        );
        if !destination_cell.is_empty() && !gesture_stack_matches(&source_cell, &destination_cell) {
            continue;
        }
        let capacity = limit.saturating_sub(destination_cell.count);
        if capacity == 0 {
            continue;
        }
        gesture_transfer(
            inventories,
            source,
            *destination,
            source_cell.count.min(capacity),
            *limit,
        )?;
    }
    Ok(())
}

fn gesture_quick_craft(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    source: InventorySlotRef,
    destinations: &[(InventorySlotRef, u8)],
    mode: QuickCraftMode,
) -> Result<(), InventoryGestureError> {
    if destinations.is_empty() {
        return Err(InventoryGestureError::InvalidCount);
    }
    let original = gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc);
    if original.is_empty() {
        return Err(InventoryGestureError::InsufficientItems);
    }
    let even = original.count / u8::try_from(destinations.len()).unwrap_or(u8::MAX);
    for (destination, limit) in destinations {
        let remaining = gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc);
        if remaining.is_empty() {
            break;
        }
        let destination_cell = gesture_cell(
            gesture_inventory_mut(inventories, *destination)?,
            destination.loc,
        );
        if !destination_cell.is_empty() && !gesture_stack_matches(&remaining, &destination_cell) {
            continue;
        }
        let capacity = limit.saturating_sub(destination_cell.count);
        if mode == QuickCraftMode::Fill {
            if capacity != 0 {
                let mut filled = remaining;
                filled.loc = destination.loc;
                filled.count = *limit;
                gesture_set_cell(inventories, *destination, filled)?;
            }
            continue;
        }
        let requested = match mode {
            QuickCraftMode::Even => even,
            QuickCraftMode::OneEach => 1,
            QuickCraftMode::Fill => unreachable!(),
        };
        let count = requested.min(capacity).min(remaining.count);
        if count != 0 {
            gesture_transfer(inventories, source, *destination, count, *limit)?;
        }
    }
    Ok(())
}

fn gesture_place_recipe(
    inventories: &mut HashMap<InventoryTarget, GestureInventory>,
    player: InventoryTarget,
    grid: InventoryTarget,
    ingredients: &[RecipePlacementIngredient],
    grid_slots: &[InventorySlotRef],
    player_sources: &[InventorySlotRef],
    use_max: bool,
) -> Result<(), InventoryGestureError> {
    if player == grid
        || ingredients.is_empty()
        || grid_slots.is_empty()
        || player_sources.is_empty()
        || ingredients.iter().any(|ingredient| {
            ingredient.grid_slot.target != grid
                || ingredient.acceptable_items.is_empty()
                || ingredient.count_per_craft == 0
        })
        || grid_slots.iter().any(|slot| slot.target != grid)
        || player_sources.iter().any(|slot| slot.target != player)
    {
        return Err(InventoryGestureError::InvalidCount);
    }
    let mut destinations = player_sources
        .iter()
        .copied()
        .map(|slot| (slot, 64))
        .collect::<Vec<_>>();
    destinations.sort_unstable_by_key(|(slot, _)| (slot.loc.inv, slot.loc.slot));
    let mut cleared = grid_slots.to_vec();
    cleared.sort_unstable_by_key(|slot| (slot.loc.inv, slot.loc.slot));
    cleared.dedup();
    for source in cleared {
        if !gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc).is_empty() {
            gesture_quick_move(inventories, source, &destinations)?;
            if !gesture_cell(gesture_inventory_mut(inventories, source)?, source.loc).is_empty() {
                return Err(InventoryGestureError::CapacityExceeded);
            }
        }
    }
    let mut ordered = ingredients.to_vec();
    ordered.sort_unstable_by_key(|ingredient| {
        (ingredient.grid_slot.loc.inv, ingredient.grid_slot.loc.slot)
    });
    let mut sources = player_sources.to_vec();
    sources.sort_unstable_by_key(|slot| (slot.loc.inv, slot.loc.slot));
    sources.dedup();
    let mut crafts = 0_u8;
    loop {
        let before = inventories.clone();
        let mut units = ordered
            .iter()
            .flat_map(|ingredient| {
                std::iter::repeat_n(ingredient, usize::from(ingredient.count_per_craft))
            })
            .collect::<Vec<_>>();
        units.sort_unstable_by_key(|ingredient| {
            (
                ingredient.acceptable_items.len(),
                ingredient.grid_slot.loc.inv,
                ingredient.grid_slot.loc.slot,
            )
        });
        let mut counts = sources
            .iter()
            .copied()
            .map(|source| {
                let count = inventories
                    .get(&source.target)
                    .map(|inventory| gesture_cell(inventory, source.loc).count)
                    .unwrap_or(0);
                (source, count)
            })
            .collect::<HashMap<_, _>>();
        let mut assignment = Vec::with_capacity(units.len());
        if !assign_recipe_units(inventories, &units, &sources, &mut counts, &mut assignment) {
            *inventories = before;
            break;
        }
        let mut complete = true;
        for (ingredient, source) in units.iter().zip(assignment) {
            if gesture_transfer(inventories, source, ingredient.grid_slot, 1, 64).is_err() {
                complete = false;
                break;
            }
        }
        if !complete {
            *inventories = before;
            break;
        }
        crafts = crafts.saturating_add(1);
        if !use_max || crafts == u8::MAX {
            break;
        }
    }
    if crafts == 0 {
        return Err(InventoryGestureError::InsufficientItems);
    }
    Ok(())
}

fn assign_recipe_units(
    inventories: &HashMap<InventoryTarget, GestureInventory>,
    units: &[&RecipePlacementIngredient],
    sources: &[InventorySlotRef],
    counts: &mut HashMap<InventorySlotRef, u8>,
    assignment: &mut Vec<InventorySlotRef>,
) -> bool {
    let Some(ingredient) = units.first() else {
        return true;
    };
    for source in sources {
        let remaining = counts.get(source).copied().unwrap_or(0);
        if remaining == 0 {
            continue;
        }
        let Some(inventory) = inventories.get(&source.target) else {
            continue;
        };
        let cell = gesture_cell(inventory, source.loc);
        if !ingredient.acceptable_items.contains(&cell.item) {
            continue;
        }
        counts.insert(*source, remaining - 1);
        assignment.push(*source);
        if assign_recipe_units(inventories, &units[1..], sources, counts, assignment) {
            return true;
        }
        assignment.pop();
        counts.insert(*source, remaining);
    }
    false
}

pub fn reduce_inventory_gestures(
    snapshots: &[InventoryStateSnapshot],
    gestures: &[InventoryGesture],
) -> Result<InventoryGestureReduction, InventoryGestureError> {
    let mut inventories = HashMap::with_capacity(snapshots.len());
    let mut target_order = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        if !snapshot.target.is_valid() {
            return Err(InventoryGestureError::MissingInventory);
        }
        let mut cells = HashMap::with_capacity(snapshot.cells.len());
        for cell in &snapshot.cells {
            if !valid_cell(cell) || cells.insert(cell.loc, cell.clone()).is_some() {
                return Err(InventoryGestureError::InvalidStack);
            }
        }
        if inventories
            .insert(
                snapshot.target,
                GestureInventory {
                    revision: snapshot.revision,
                    initial: cells.clone(),
                    current: cells,
                },
            )
            .is_some()
        {
            return Err(InventoryGestureError::DuplicateInventory);
        }
        target_order.push(snapshot.target);
    }

    let mut effects = Vec::new();
    let mut effect_only_targets = std::collections::HashSet::new();
    for gesture in gestures {
        match gesture {
            InventoryGesture::ExpectIdentity {
                slot,
                item,
                components,
            } => {
                let current =
                    gesture_cell(gesture_inventory_mut(&mut inventories, *slot)?, slot.loc);
                if current.is_empty() || current.item != *item || current.nbt != *components {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
            }
            InventoryGesture::RemoveBy {
                source,
                count,
                kind,
            } => {
                let current = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *source)?,
                    source.loc,
                );
                let amount = match count {
                    InventoryTransferCount::All => current.count,
                    InventoryTransferCount::Half => current.count.div_ceil(2),
                    InventoryTransferCount::Exact(amount) => *amount,
                };
                if amount > 0 {
                    let stack = gesture_remove(&mut inventories, *source, amount)?;
                    effects.push(InventoryGestureEffect::Removed {
                        source: *source,
                        stack,
                        kind: *kind,
                    });
                }
            }
            InventoryGesture::DropSelectedHand { player, count } => {
                let selector = InventorySlotRef::new(*player, InvLoc::new(INV_SELECTED, 0));
                let selected = gesture_cell(
                    gesture_inventory_mut(&mut inventories, selector)?,
                    selector.loc,
                );
                let slot = selected
                    .item
                    .checked_sub(1)
                    .filter(|slot| *slot < 9)
                    .ok_or(InventoryGestureError::IncompatibleStack)?;
                if selected.count != 1 || !selected.nbt.is_empty() {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
                let source = InventorySlotRef::new(*player, InvLoc::new(INV_MAIN, slot));
                let current =
                    gesture_cell(gesture_inventory_mut(&mut inventories, source)?, source.loc);
                let amount = match count {
                    InventoryTransferCount::All => current.count,
                    InventoryTransferCount::Half => current.count.div_ceil(2),
                    InventoryTransferCount::Exact(amount) => *amount,
                };
                if amount > 0 {
                    let stack = gesture_remove(&mut inventories, source, amount)?;
                    effects.push(InventoryGestureEffect::Removed {
                        source,
                        stack,
                        kind: InventoryRemovalKind::Drop,
                    });
                }
            }
            InventoryGesture::SwapSelectedHand { player } => {
                let selector = InventorySlotRef::new(*player, InvLoc::new(INV_SELECTED, 0));
                let selected = gesture_cell(
                    gesture_inventory_mut(&mut inventories, selector)?,
                    selector.loc,
                );
                let slot = selected
                    .item
                    .checked_sub(1)
                    .filter(|slot| *slot < 9)
                    .ok_or(InventoryGestureError::IncompatibleStack)?;
                if selected.count != 1 || !selected.nbt.is_empty() {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
                let first = InventorySlotRef::new(*player, InvLoc::new(INV_MAIN, slot));
                let second = InventorySlotRef::new(*player, InvLoc::new(INV_OFFHAND, 0));
                let mut first_cell =
                    gesture_cell(gesture_inventory_mut(&mut inventories, first)?, first.loc);
                let mut second_cell =
                    gesture_cell(gesture_inventory_mut(&mut inventories, second)?, second.loc);
                first_cell.loc = second.loc;
                second_cell.loc = first.loc;
                gesture_set_cell(&mut inventories, first, second_cell)?;
                gesture_set_cell(&mut inventories, second, first_cell)?;
            }
            InventoryGesture::SelectHotbar { slot, selected } => {
                if *selected >= 9 || slot.loc != InvLoc::new(INV_SELECTED, 0) {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
                gesture_set_cell(
                    &mut inventories,
                    *slot,
                    InventoryCell {
                        loc: slot.loc,
                        item: u16::from(*selected) + 1,
                        count: 1,
                        nbt: Vec::new(),
                    },
                )?;
            }
            InventoryGesture::Expect { slot, cell } => {
                let actual =
                    gesture_cell(gesture_inventory_mut(&mut inventories, *slot)?, slot.loc);
                let mut expected = cell.clone();
                expected.loc = slot.loc;
                if actual != expected {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
            }
            InventoryGesture::ExactSet {
                destination,
                before,
                after,
            } => {
                let actual = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *destination)?,
                    destination.loc,
                );
                let mut expected = before.clone();
                expected.loc = destination.loc;
                if actual != expected {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
                gesture_set_cell(&mut inventories, *destination, after.clone())?;
            }
            InventoryGesture::Transfer {
                source,
                destination,
                count,
                destination_limit,
            } => gesture_transfer(
                &mut inventories,
                *source,
                *destination,
                *count,
                *destination_limit,
            )?,
            InventoryGesture::TransferBy {
                source,
                destination,
                count,
                destination_limit,
            } => {
                let source_cell = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *source)?,
                    source.loc,
                );
                let count = match count {
                    InventoryTransferCount::Exact(count) => *count,
                    InventoryTransferCount::All | InventoryTransferCount::Half => {
                        let destination_cell = gesture_cell(
                            gesture_inventory_mut(&mut inventories, *destination)?,
                            destination.loc,
                        );
                        let requested = if *count == InventoryTransferCount::Half {
                            source_cell.count.div_ceil(2)
                        } else {
                            source_cell.count
                        };
                        requested.min(destination_limit.saturating_sub(destination_cell.count))
                    }
                };
                gesture_transfer(
                    &mut inventories,
                    *source,
                    *destination,
                    count,
                    *destination_limit,
                )?;
            }
            InventoryGesture::ExchangeCursor {
                slot,
                cursor,
                single,
                slot_limit,
                cursor_limit,
            } => {
                let slot_cell =
                    gesture_cell(gesture_inventory_mut(&mut inventories, *slot)?, slot.loc);
                let cursor_cell = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *cursor)?,
                    cursor.loc,
                );
                if cursor_cell.is_empty() {
                    let count = if *single {
                        slot_cell.count.div_ceil(2)
                    } else {
                        slot_cell.count
                    };
                    if count > 0 {
                        gesture_transfer(
                            &mut inventories,
                            *slot,
                            *cursor,
                            count.min(*cursor_limit),
                            *cursor_limit,
                        )?;
                    }
                } else if slot_cell.is_empty()
                    || slot_cell.item == cursor_cell.item && slot_cell.nbt == cursor_cell.nbt
                {
                    let count = if *single { 1 } else { cursor_cell.count };
                    let count = count.min(slot_limit.saturating_sub(slot_cell.count));
                    if count > 0 {
                        gesture_transfer(&mut inventories, *cursor, *slot, count, *slot_limit)?;
                    }
                } else {
                    if cursor_cell.count > *slot_limit || slot_cell.count > *cursor_limit {
                        return Err(InventoryGestureError::CapacityExceeded);
                    }
                    let mut next_slot = cursor_cell;
                    next_slot.loc = slot.loc;
                    let mut next_cursor = slot_cell;
                    next_cursor.loc = cursor.loc;
                    gesture_set_cell(&mut inventories, *slot, next_slot)?;
                    gesture_set_cell(&mut inventories, *cursor, next_cursor)?;
                }
            }
            InventoryGesture::Swap {
                first,
                second,
                first_limit,
                second_limit,
            } => {
                if first == second {
                    continue;
                }
                let first_cell =
                    gesture_cell(gesture_inventory_mut(&mut inventories, *first)?, first.loc);
                let second_cell = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *second)?,
                    second.loc,
                );
                if second_cell.count > *first_limit || first_cell.count > *second_limit {
                    return Err(InventoryGestureError::CapacityExceeded);
                }
                let mut next_first = second_cell;
                next_first.loc = first.loc;
                let mut next_second = first_cell;
                next_second.loc = second.loc;
                gesture_set_cell(&mut inventories, *first, next_first)?;
                gesture_set_cell(&mut inventories, *second, next_second)?;
            }
            InventoryGesture::Remove {
                source,
                count,
                kind,
            } => {
                let stack = gesture_remove(&mut inventories, *source, *count)?;
                effects.push(InventoryGestureEffect::Removed {
                    source: *source,
                    stack,
                    kind: *kind,
                });
            }
            InventoryGesture::CreativeClone {
                source,
                destination,
                destination_limit,
            } => {
                if !gesture_cell(
                    gesture_inventory_mut(&mut inventories, *destination)?,
                    destination.loc,
                )
                .is_empty()
                {
                    return Err(InventoryGestureError::CapacityExceeded);
                }
                let mut stack = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *source)?,
                    source.loc,
                );
                if stack.is_empty() || *destination_limit == 0 || *destination_limit > INV_MAX_STACK
                {
                    return Err(InventoryGestureError::InvalidStack);
                }
                stack.loc = destination.loc;
                stack.count = *destination_limit;
                gesture_set_cell(&mut inventories, *destination, stack)?;
            }
            InventoryGesture::CreativeSet {
                destination,
                stack,
                destination_limit,
            } => {
                if stack.count > *destination_limit {
                    return Err(InventoryGestureError::CapacityExceeded);
                }
                gesture_set_cell(&mut inventories, *destination, stack.clone())?;
            }
            InventoryGesture::CreativeDrop { owner, stack } => {
                if !inventories.contains_key(owner) {
                    return Err(InventoryGestureError::MissingInventory);
                }
                if stack.is_empty() || !valid_cell(stack) {
                    return Err(InventoryGestureError::InvalidStack);
                }
                effect_only_targets.insert(*owner);
                effects.push(InventoryGestureEffect::CreatedDrop {
                    owner: *owner,
                    stack: stack.clone(),
                });
            }
            InventoryGesture::QuickMove {
                source,
                destinations,
            } => gesture_quick_move(&mut inventories, *source, destinations)?,
            InventoryGesture::DrainToPlayer {
                player,
                source,
                destinations,
            } => {
                let valid_source = match source.target {
                    InventoryTarget::Player(owner) => {
                        owner == *player && source.loc == InvLoc::new(INV_CURSOR, 0)
                    }
                    InventoryTarget::Virtual {
                        owner: ActorId::Player(owner),
                        ..
                    } => owner == *player && source.loc.inv == INV_MAIN,
                    _ => false,
                };
                if !valid_source
                    || destinations.iter().any(|(slot, limit)| {
                        slot.target != InventoryTarget::Player(*player)
                            || slot.loc.inv != INV_MAIN
                            || slot.loc.slot >= 36
                            || *limit == 0
                            || *limit > INV_MAX_STACK
                    })
                {
                    return Err(InventoryGestureError::InvalidCount);
                }
                effect_only_targets.insert(source.target);
                if !destinations.is_empty() {
                    gesture_quick_move(&mut inventories, *source, destinations)?;
                }
                let remaining = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *source)?,
                    source.loc,
                );
                if !remaining.is_empty() {
                    let stack = gesture_remove(&mut inventories, *source, remaining.count)?;
                    effects.push(InventoryGestureEffect::Removed {
                        source: *source,
                        stack,
                        kind: InventoryRemovalKind::Drop,
                    });
                }
            }
            InventoryGesture::ClearResult { slot } => {
                if !matches!(slot.target, InventoryTarget::Virtual { .. })
                    || slot.loc.inv != INV_MAIN
                {
                    return Err(InventoryGestureError::InvalidCount);
                }
                effect_only_targets.insert(slot.target);
                gesture_set_cell(&mut inventories, *slot, InventoryCell::empty(slot.loc))?;
            }
            InventoryGesture::QuickCraft {
                source,
                destinations,
                mode,
            } => gesture_quick_craft(&mut inventories, *source, destinations, *mode)?,
            InventoryGesture::PickupAll {
                destination,
                sources,
                destination_limit,
            } => {
                for source in sources {
                    let destination_cell = gesture_cell(
                        gesture_inventory_mut(&mut inventories, *destination)?,
                        destination.loc,
                    );
                    if destination_cell.is_empty() {
                        return Err(InventoryGestureError::IncompatibleStack);
                    }
                    let source_cell = gesture_cell(
                        gesture_inventory_mut(&mut inventories, *source)?,
                        source.loc,
                    );
                    if !gesture_stack_matches(&source_cell, &destination_cell) {
                        continue;
                    }
                    let count = source_cell
                        .count
                        .min(destination_limit.saturating_sub(destination_cell.count));
                    if count != 0 {
                        gesture_transfer(
                            &mut inventories,
                            *source,
                            *destination,
                            count,
                            *destination_limit,
                        )?;
                    }
                }
            }
            InventoryGesture::RecipeCraft {
                ingredients,
                destination,
                result,
                repetitions,
                destination_limit,
            } => {
                if *repetitions == 0 || result.is_empty() {
                    return Err(InventoryGestureError::InvalidCount);
                }
                for ingredient in ingredients {
                    let actual = gesture_cell(
                        gesture_inventory_mut(&mut inventories, ingredient.slot)?,
                        ingredient.slot.loc,
                    );
                    let required = ingredient
                        .count_per_craft
                        .checked_mul(*repetitions)
                        .ok_or(InventoryGestureError::InvalidCount)?;
                    if !gesture_stack_matches(&actual, &ingredient.expected)
                        || actual.count < required
                    {
                        return Err(InventoryGestureError::InsufficientItems);
                    }
                }
                let result_count = result
                    .count
                    .checked_mul(*repetitions)
                    .ok_or(InventoryGestureError::InvalidCount)?;
                let destination_cell = gesture_cell(
                    gesture_inventory_mut(&mut inventories, *destination)?,
                    destination.loc,
                );
                if !destination_cell.is_empty() && !gesture_stack_matches(result, &destination_cell)
                {
                    return Err(InventoryGestureError::IncompatibleStack);
                }
                if destination_cell.count.saturating_add(result_count) > *destination_limit {
                    return Err(InventoryGestureError::CapacityExceeded);
                }
                for ingredient in ingredients {
                    let required = ingredient.count_per_craft * *repetitions;
                    let _ = gesture_remove(&mut inventories, ingredient.slot, required)?;
                }
                let mut crafted = result.clone();
                crafted.loc = destination.loc;
                crafted.count = result_count;
                let mut combined = crafted.clone();
                combined.count += destination_cell.count;
                gesture_set_cell(&mut inventories, *destination, combined)?;
                effects.push(InventoryGestureEffect::Crafted {
                    destination: *destination,
                    stack: crafted,
                    repetitions: *repetitions,
                });
            }
            InventoryGesture::PlaceRecipe {
                player,
                grid,
                ingredients,
                grid_slots,
                player_sources,
                use_max,
            } => gesture_place_recipe(
                &mut inventories,
                *player,
                *grid,
                ingredients,
                grid_slots,
                player_sources,
                *use_max,
            )?,
        }
    }

    let mut expected_revisions = Vec::new();
    let mut operations = Vec::new();
    for target in target_order {
        let inventory = inventories
            .remove(&target)
            .ok_or(InventoryGestureError::MissingInventory)?;
        let mut locations: Vec<_> = inventory
            .initial
            .keys()
            .chain(inventory.current.keys())
            .copied()
            .collect();
        locations.sort_unstable_by_key(|loc| (loc.inv, loc.slot));
        locations.dedup();
        let target_start = operations.len();
        for loc in locations {
            let before = inventory
                .initial
                .get(&loc)
                .cloned()
                .unwrap_or_else(|| InventoryCell::empty(loc));
            let after = inventory
                .current
                .get(&loc)
                .cloned()
                .unwrap_or_else(|| InventoryCell::empty(loc));
            if before != after {
                operations.push(SemanticInventoryOp {
                    target,
                    before,
                    after,
                });
            }
        }
        if operations.len() != target_start || effect_only_targets.contains(&target) {
            expected_revisions.push((target, inventory.revision));
        }
    }
    Ok(InventoryGestureReduction {
        mutation: InventoryMutation {
            expected_revisions,
            operations,
        },
        effects,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryContainerButton {
    Enchant { option: u8 },
    StonecutterRecipe { recipe: u32 },
    LoomPattern { pattern: u16 },
    LecternPage { page: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryPatternSelection {
    Key(String),
    MenuIndex(u16),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryMenuKind {
    RenameAnvil {
        name: String,
    },
    TakeAnvilResult,
    ContainerButton(InventoryContainerButton),
    SelectTrade {
        offer: u32,
    },
    ExecuteTrade {
        offer: u32,
        repetitions: u16,
    },
    SetBeacon {
        primary_effect: u16,
        secondary_effect: Option<u16>,
    },
    ApplyLoomPattern {
        pattern: InventoryPatternSelection,
    },
    TakeDisplayedResult {
        repetitions: u16,
    },
    TakeGrindstoneResult,
    CraftResult {
        recipe: u32,
        repetitions: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryMenuIntent {
    pub actor: ActorId,
    pub tick: TickStamp,
    pub session: InventoryScreenSession,
    pub sync_id: u8,
    pub window_kind: u16,
    pub expected_revisions: Vec<(InventoryTarget, u64)>,
    pub actor_chunk: ChunkKey,
    pub affected: Vec<ChunkKey>,
    pub kind: InventoryMenuKind,
    pub companion_gestures: Option<Vec<InventoryGesture>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryMenuEffect {
    AnvilNameChanged {
        name: String,
        level_cost: u32,
    },
    AnvilResultTaken {
        level_cost: u32,
        damage_anvil: bool,
    },
    ContainerSelectionChanged {
        selection: InventoryContainerButton,
    },
    TradeSelected {
        offer: u32,
        merchant: InventoryMerchantPresentation,
    },
    TradeCompleted {
        offer: u32,
        repetitions: u16,
        uses_after: u32,
        merchant_experience: u32,
        merchant: InventoryMerchantPresentation,
    },
    BeaconConfigured {
        target: InventoryTarget,
        before: BeaconMenuState,
        primary_effect: u16,
        secondary_effect: Option<u16>,
    },
    LoomPatternApplied {
        pattern: String,
        menu_index: u16,
    },
    GrindstoneResultTaken {
        experience: u32,
    },
    CraftCompleted {
        recipe: u32,
        repetitions: u16,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconMenuState {
    pub revision: u64,
    pub levels: u8,
    pub primary_effect: Option<u16>,
    pub secondary_effect: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkInventoryState {
    #[serde(default)]
    pub menu_admission: Option<(crate::canonical_action::ActionRef, crate::canonical_menu::MenuVirtualInventoryAdmission)>,
    pub snapshot: InventoryStateSnapshot,
    pub chunks: Vec<ChunkKey>,
    pub beacon: Option<BeaconMenuState>,
    pub player_lease: Option<crate::canonical_action::LivePlayerLease>,
}

#[must_use]
pub const fn beacon_choice_valid(
    levels: u8,
    primary_effect: u16,
    secondary_effect: Option<u16>,
) -> bool {
    let required = match primary_effect {
        1 | 3 => 1,
        11 | 8 => 2,
        5 => 3,
        _ => return false,
    };
    if levels < required || levels > 4 {
        return false;
    }
    match secondary_effect {
        None => true,
        Some(secondary) => levels == 4 && (secondary == 10 || secondary == primary_effect),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryTradeOfferPresentation {
    pub first_cost: InventoryCell,
    pub second_cost: Option<InventoryCell>,
    pub result: InventoryCell,
    pub uses: u32,
    pub max_uses: u32,
    pub reward_experience: bool,
    pub merchant_experience: u32,
    pub special_price: i32,
    pub demand: i32,
    pub price_multiplier_bits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMerchantPresentation {
    pub merchant: GlobalEntityId,
    pub tier: u8,
    pub experience: u32,
    pub regular_villager: bool,
    pub can_restock: bool,
    pub last_trader: Option<GlobalPlayerId>,
    pub last_trader_name: Option<String>,
    pub new_trading_ui: bool,
    pub economy_trade: bool,
    pub offers: Vec<InventoryTradeOfferPresentation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryMenuRule {
    pub kind: InventoryMenuKind,
    pub client_gestures: Vec<InventoryGesture>,
    pub gestures: Vec<InventoryGesture>,
    pub effect: InventoryMenuEffect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryMenuMetadata {
    pub session: InventoryScreenSession,
    pub sync_id: u8,
    pub window_kind: u16,
    pub expected_revisions: Vec<(InventoryTarget, u64)>,
    pub rules: Vec<InventoryMenuRule>,
}

#[must_use]
pub fn inventory_menu_targets(metadata: &InventoryMenuMetadata) -> Vec<InventoryTarget> {
    let mut targets: Vec<_> = metadata
        .expected_revisions
        .iter()
        .map(|(target, _)| *target)
        .collect();
    targets.sort_unstable();
    targets.dedup();
    targets
}

pub fn rename_inventory_menu_targets(
    metadata: &mut InventoryMenuMetadata,
    renames: &[(InventoryTarget, InventoryTarget)],
) {
    let replacements: HashMap<_, _> = renames.iter().copied().collect();
    for (target, _) in &mut metadata.expected_revisions {
        *target = replacements.get(target).copied().unwrap_or(*target);
    }
    for rule in &mut metadata.rules {
        for gesture in rule.client_gestures.iter_mut().chain(&mut rule.gestures) {
            gesture.rename_targets(renames);
        }
        if let InventoryMenuEffect::BeaconConfigured { target, .. } = &mut rule.effect {
            *target = replacements.get(target).copied().unwrap_or(*target);
        }
    }
}

pub fn beacon_menu_metadata(
    screen: &InventoryScreenSnapshot,
    payment: &InventoryStateSnapshot,
    state: BeaconMenuState,
    valid_payment: bool,
) -> Result<InventoryMenuMetadata, InventoryMenuError> {
    if !matches!(payment.target, InventoryTarget::Block { .. }) || state.levels > 4 {
        return Err(InventoryMenuError::InvalidChoice);
    }
    let payment_slot = screen
        .slots
        .iter()
        .find(|slot| slot.screen_slot == 0 && slot.slot.target == payment.target)
        .ok_or(InventoryMenuError::InvalidChoice)?;
    let actual = payment
        .cells
        .iter()
        .find(|cell| cell.loc == payment_slot.slot.loc)
        .cloned()
        .unwrap_or_else(|| InventoryCell::empty(payment_slot.slot.loc));
    let mut rules = Vec::new();
    if valid_payment && !actual.is_empty() {
        for primary in [1_u16, 3, 11, 8, 5] {
            if !beacon_choice_valid(state.levels, primary, None) {
                continue;
            }
            let secondaries = if state.levels == 4 {
                [None, Some(10), Some(primary)]
            } else {
                [None, None, None]
            };
            for secondary in secondaries
                .into_iter()
                .take(if state.levels == 4 { 3 } else { 1 })
            {
                if !beacon_choice_valid(state.levels, primary, secondary) {
                    continue;
                }
                if rules.iter().any(|rule: &InventoryMenuRule| {
                    rule.kind
                        == InventoryMenuKind::SetBeacon {
                            primary_effect: primary,
                            secondary_effect: secondary,
                        }
                }) {
                    continue;
                }
                let kind = InventoryMenuKind::SetBeacon {
                    primary_effect: primary,
                    secondary_effect: secondary,
                };
                let consume = InventoryGesture::Remove {
                    source: payment_slot.slot,
                    count: 1,
                    kind: InventoryRemovalKind::Consume,
                };
                let effect = InventoryMenuEffect::BeaconConfigured {
                    target: payment.target,
                    before: state,
                    primary_effect: primary,
                    secondary_effect: secondary,
                };
                for client_gestures in [Vec::new(), vec![consume.clone()]] {
                    rules.push(InventoryMenuRule {
                        kind: kind.clone(),
                        client_gestures,
                        gestures: vec![consume.clone()],
                        effect: effect.clone(),
                    });
                }
            }
        }
    }
    Ok(InventoryMenuMetadata {
        session: screen.session,
        sync_id: screen.sync_id,
        window_kind: screen.window_kind,
        expected_revisions: vec![(payment.target, payment.revision)],
        rules,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryMenuReduction {
    pub mutation: InventoryMutationIntent,
    pub inventory_effects: Vec<InventoryGestureEffect>,
    pub accepted_effect: InventoryMenuEffect,
    pub intent: InventoryMenuIntent,
}

impl InventoryMenuReduction {
    pub fn into_action(
        self,
        sequence: PlayerSeq,
        ledger: &InvLedger,
    ) -> Result<InventoryOp, InventoryGestureError> {
        let mut action = self.mutation.into_action(sequence, ledger)?;
        action.menu_effects.push(self.accepted_effect);
        Ok(action)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryMenuError {
    ActorMismatch,
    SessionMismatch,
    SyncMismatch,
    WindowMismatch,
    StaleRevision,
    DuplicateRevision,
    UnsupportedChoice,
    InvalidChoice,
    InvalidConservation,
    MissingStateTransition,
    Gesture(InventoryGestureError),
}

fn inventory_menu_gestures_conserve(
    kind: &InventoryMenuKind,
    gestures: &[InventoryGesture],
) -> bool {
    let mut preview_changes = 0_u8;
    gestures.iter().all(|gesture| match gesture {
        InventoryGesture::ExactSet {
            destination,
            before,
            after,
        } => {
            if before.item == after.item && before.count == after.count {
                true
            } else if matches!(kind, InventoryMenuKind::RenameAnvil { .. })
                && matches!(
                    destination.target,
                    InventoryTarget::Virtual {
                        owner: ActorId::Player(_),
                        ..
                    }
                )
                && destination.loc == InvLoc::new(INV_MAIN, 2)
                && before.loc == destination.loc
                && after.loc == destination.loc
                && valid_cell(before)
                && valid_cell(after)
            {
                preview_changes = preview_changes.saturating_add(1);
                preview_changes == 1
            } else {
                false
            }
        }
        InventoryGesture::CreativeClone { .. }
        | InventoryGesture::CreativeSet { .. }
        | InventoryGesture::CreativeDrop { .. }
        | InventoryGesture::DrainToPlayer { .. }
        | InventoryGesture::ClearResult { .. } => false,
        InventoryGesture::DropSelectedHand { .. }
        | InventoryGesture::RemoveBy { .. }
        | InventoryGesture::SwapSelectedHand { .. }
        | InventoryGesture::SelectHotbar { .. }
        | InventoryGesture::ExpectIdentity { .. }
        | InventoryGesture::Expect { .. }
        | InventoryGesture::Transfer { .. }
        | InventoryGesture::TransferBy { .. }
        | InventoryGesture::ExchangeCursor { .. }
        | InventoryGesture::Swap { .. }
        | InventoryGesture::Remove { .. }
        | InventoryGesture::QuickMove { .. }
        | InventoryGesture::QuickCraft { .. }
        | InventoryGesture::PickupAll { .. }
        | InventoryGesture::RecipeCraft { .. }
        | InventoryGesture::PlaceRecipe { .. } => true,
    })
}

fn inventory_menu_effect_matches(kind: &InventoryMenuKind, effect: &InventoryMenuEffect) -> bool {
    match (kind, effect) {
        (
            InventoryMenuKind::RenameAnvil { name },
            InventoryMenuEffect::AnvilNameChanged {
                name: effect_name, ..
            },
        ) => name == effect_name,
        (InventoryMenuKind::TakeAnvilResult, InventoryMenuEffect::AnvilResultTaken { .. })
        | (
            InventoryMenuKind::TakeGrindstoneResult,
            InventoryMenuEffect::GrindstoneResultTaken { .. },
        ) => true,
        (
            InventoryMenuKind::TakeDisplayedResult { repetitions: 1 },
            InventoryMenuEffect::AnvilResultTaken { .. }
            | InventoryMenuEffect::GrindstoneResultTaken { .. },
        ) => true,
        (
            InventoryMenuKind::TakeDisplayedResult { repetitions },
            InventoryMenuEffect::TradeCompleted {
                repetitions: effect_repetitions,
                ..
            }
            | InventoryMenuEffect::CraftCompleted {
                repetitions: effect_repetitions,
                ..
            },
        ) => repetitions == effect_repetitions,
        (
            InventoryMenuKind::ContainerButton(selection),
            InventoryMenuEffect::ContainerSelectionChanged {
                selection: effect_selection,
            },
        ) => selection == effect_selection,
        (
            InventoryMenuKind::SelectTrade { offer },
            InventoryMenuEffect::TradeSelected {
                offer: effect_offer,
                ..
            },
        ) => offer == effect_offer,
        (
            InventoryMenuKind::ExecuteTrade { offer, repetitions },
            InventoryMenuEffect::TradeCompleted {
                offer: effect_offer,
                repetitions: effect_repetitions,
                ..
            },
        ) => offer == effect_offer && repetitions == effect_repetitions,
        (
            InventoryMenuKind::SetBeacon {
                primary_effect,
                secondary_effect,
            },
            InventoryMenuEffect::BeaconConfigured {
                primary_effect: effect_primary,
                secondary_effect: effect_secondary,
                ..
            },
        ) => primary_effect == effect_primary && secondary_effect == effect_secondary,
        (
            InventoryMenuKind::ApplyLoomPattern { pattern },
            InventoryMenuEffect::LoomPatternApplied {
                pattern: effect_pattern,
                menu_index,
            },
        ) => match pattern {
            InventoryPatternSelection::Key(pattern) => pattern == effect_pattern,
            InventoryPatternSelection::MenuIndex(index) => index == menu_index,
        },
        (
            InventoryMenuKind::CraftResult {
                recipe,
                repetitions,
            },
            InventoryMenuEffect::CraftCompleted {
                recipe: effect_recipe,
                repetitions: effect_repetitions,
            },
        ) => recipe == effect_recipe && repetitions == effect_repetitions,
        _ => false,
    }
}

fn normalized_inventory_revisions(
    revisions: &[(InventoryTarget, u64)],
) -> Result<Vec<(InventoryTarget, u64)>, InventoryMenuError> {
    let mut normalized = revisions.to_vec();
    normalized.sort_unstable_by_key(|(target, _)| *target);
    if normalized.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(InventoryMenuError::DuplicateRevision);
    }
    Ok(normalized)
}

pub fn reduce_inventory_menu(
    screen: &InventoryScreenSnapshot,
    snapshots: &[InventoryStateSnapshot],
    metadata: &InventoryMenuMetadata,
    intent: InventoryMenuIntent,
) -> Result<InventoryMenuReduction, InventoryMenuError> {
    if intent.actor != ActorId::Player(intent.session.actor) {
        return Err(InventoryMenuError::ActorMismatch);
    }
    if screen.session != intent.session || metadata.session != intent.session {
        return Err(InventoryMenuError::SessionMismatch);
    }
    if screen.sync_id != intent.sync_id || metadata.sync_id != intent.sync_id {
        return Err(InventoryMenuError::SyncMismatch);
    }
    if screen.window_kind != intent.window_kind || metadata.window_kind != intent.window_kind {
        return Err(InventoryMenuError::WindowMismatch);
    }
    match &intent.kind {
        InventoryMenuKind::RenameAnvil { name }
            if name.chars().count() > 50 || name.chars().any(char::is_control) =>
        {
            return Err(InventoryMenuError::InvalidChoice);
        }
        InventoryMenuKind::ExecuteTrade { repetitions, .. }
        | InventoryMenuKind::TakeDisplayedResult { repetitions }
        | InventoryMenuKind::CraftResult { repetitions, .. }
            if *repetitions == 0 =>
        {
            return Err(InventoryMenuError::InvalidChoice);
        }
        _ => {}
    }
    let expected = normalized_inventory_revisions(&intent.expected_revisions)?;
    if normalized_inventory_revisions(&metadata.expected_revisions)? != expected {
        return Err(InventoryMenuError::StaleRevision);
    }
    let mut actual = snapshots
        .iter()
        .map(|snapshot| (snapshot.target, snapshot.revision))
        .collect::<Vec<_>>();
    actual.sort_unstable_by_key(|(target, _)| *target);
    if actual != expected {
        return Err(InventoryMenuError::StaleRevision);
    }
    let rule = metadata
        .rules
        .iter()
        .find(|rule| {
            rule.kind == intent.kind
                && match &intent.companion_gestures {
                    Some(gestures) => gestures == &rule.client_gestures,
                    None => rule.client_gestures.is_empty(),
                }
        })
        .ok_or_else(|| {
            if metadata.rules.iter().any(|rule| rule.kind == intent.kind) {
                InventoryMenuError::InvalidChoice
            } else {
                InventoryMenuError::UnsupportedChoice
            }
        })?;
    if !inventory_menu_effect_matches(&rule.kind, &rule.effect) {
        return Err(InventoryMenuError::InvalidChoice);
    }
    if !inventory_menu_gestures_conserve(&rule.kind, &rule.gestures) {
        return Err(InventoryMenuError::InvalidConservation);
    }
    let reduced = reduce_inventory_gestures(snapshots, &rule.gestures)
        .map_err(InventoryMenuError::Gesture)?;
    if reduced.mutation.operations.is_empty() {
        return Err(InventoryMenuError::MissingStateTransition);
    }
    if normalized_inventory_revisions(&reduced.mutation.expected_revisions)? != expected {
        return Err(InventoryMenuError::StaleRevision);
    }
    let mut affected = intent.affected.clone();
    affected.extend(screen.affected.iter().copied());
    affected.push(intent.actor_chunk);
    affected.extend(
        expected
            .iter()
            .filter_map(|(target, _)| target.intrinsic_chunk()),
    );
    affected.sort_unstable();
    affected.dedup();
    let mutation = InventoryMutationIntent {
        actor: intent.actor,
        tick: intent.tick,
        payload: InventoryIntentPayload::Gestures(rule.gestures.clone()),
        screen_session: Some(intent.session),
        actor_chunk: intent.actor_chunk,
        affected,
    };
    Ok(InventoryMenuReduction {
        mutation,
        inventory_effects: reduced.effects,
        accepted_effect: rule.effect.clone(),
        intent: intent.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn loc(slot: u16) -> InvLoc {
        InvLoc::new(INV_MAIN, slot)
    }

    fn actor_chunk() -> ChunkKey {
        ChunkKey::new(crate::protocol::WorldId::new(0, 1), 0, 0)
    }

    fn cell(inv: u8, slot: u16, item: u16, count: u8) -> InventoryCell {
        InventoryCell {
            loc: InvLoc::new(inv, slot),
            item,
            count,
            nbt: Vec::new(),
        }
    }

    #[test]
    fn reused_player_slot_keeps_inventory_incarnations_separate() {
        let player = gid(1, 2);
        let previous = InventoryTarget::LeasedPlayer {
            key: uuid::Uuid::from_u128(19),
            player,
            incarnation: 41,
        };
        let current = InventoryTarget::LeasedPlayer {
            key: uuid::Uuid::from_u128(19),
            player,
            incarnation: 42,
        };
        assert!(
            !InventoryTarget::LeasedPlayer {
                key: uuid::Uuid::from_u128(19),
                player,
                incarnation: 0,
            }
            .is_valid()
        );
        let mut ledger = InvLedger::default();
        for (target, item) in [(previous, 5), (current, 7)] {
            ledger
                .install_snapshot(InventoryStateSnapshot {
                    target,
                    revision: 0,
                    cells: vec![cell(INV_MAIN, 0, item, 1)],
                })
                .unwrap();
        }
        ledger
            .apply_mutation(InventoryMutation {
                expected_revisions: vec![(previous, 0)],
                operations: vec![
                    SemanticInventoryOp {
                        target: previous,
                        before: cell(INV_MAIN, 0, 5, 1),
                        after: InventoryCell::empty(loc(0)),
                    },
                    SemanticInventoryOp {
                        target: previous,
                        before: InventoryCell::empty(loc(1)),
                        after: cell(INV_MAIN, 1, 5, 1),
                    },
                ],
            })
            .unwrap();
        assert_eq!(ledger.snapshot(previous).unwrap().cells[0].loc.slot, 1);
        assert_eq!(ledger.snapshot(current).unwrap().cells[0].count, 1);
        assert_eq!(ledger.snapshot(current).unwrap().cells[0].item, 7);
    }

    #[test]
    fn remainder_insertion_preserves_items_and_revision_under_conflict() {
        let target = InventoryTarget::Player(gid(1, 2));
        let consumed = cell(INV_MAIN, 0, 5, 1);
        let snapshot = InventoryStateSnapshot {
            target,
            revision: 4,
            cells: vec![consumed.clone(), cell(INV_MAIN, 1, 7, 63)],
        };
        let remainder = cell(INV_MAIN, 0, 7, 2);
        let ops = plan_inventory_insertion(
            &snapshot,
            Some(&InventoryCell::empty(loc(0))),
            &remainder,
            64,
            &[loc(1), loc(2)],
            |left, right| left.item == right.item && left.nbt == right.nbt,
            |item, loc, count| {
                Some(InventoryCell {
                    loc,
                    count,
                    ..item.clone()
                })
            },
        )
        .unwrap();
        let mut service = InventoryService::new();
        service
            .install(target, snapshot.revision, snapshot.cells.clone())
            .unwrap();
        service
            .mutate(InventoryMutation {
                expected_revisions: vec![(target, snapshot.revision)],
                operations: ops.clone(),
            })
            .unwrap();
        let result = service.snapshot(target).unwrap();
        assert_eq!(
            result
                .cells
                .iter()
                .filter(|cell| cell.item == 7)
                .map(|cell| u16::from(cell.count))
                .sum::<u16>(),
            65
        );
        assert_eq!(result.cells.iter().filter(|cell| cell.item == 5).count(), 0);
        assert!(
            service
                .mutate(InventoryMutation {
                    expected_revisions: vec![(target, snapshot.revision)],
                    operations: ops,
                })
                .is_err()
        );
    }

    #[test]
    fn remote_preview_captures_joint_edit_without_installing_target_truth() {
        let viewer = gid(1, 1);
        let remote = gid(2, 1);
        let viewer_target = InventoryTarget::Player(viewer);
        let remote_target = InventoryTarget::Player(remote);
        let viewer_chunk = actor_chunk();
        let remote_chunk = ChunkKey::new(crate::protocol::WorldId::new(0, 1), 1, 0);
        let viewer_snapshot = InventoryStateSnapshot {
            target: viewer_target,
            revision: 3,
            cells: vec![cell(INV_CURSOR, 0, 5, 1)],
        };
        let remote_snapshot = InventoryStateSnapshot {
            target: remote_target,
            revision: 7,
            cells: Vec::new(),
        };
        let mut viewer_ledger = InvLedger::new();
        viewer_ledger
            .install_owned_snapshot(viewer_snapshot.clone(), vec![viewer_chunk])
            .unwrap();
        let intent = InventoryMutationIntent {
            actor: ActorId::Player(viewer),
            tick: TickStamp(4),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::Transfer {
                source: InventorySlotRef::new(viewer_target, InvLoc::new(INV_CURSOR, 0)),
                destination: InventorySlotRef::new(remote_target, loc(0)),
                count: 1,
                destination_limit: 64,
            }]),
            screen_session: Some(InventoryScreenSession {
                actor: viewer,
                session: 12,
            }),
            actor_chunk: viewer_chunk,
            affected: vec![viewer_chunk, remote_chunk],
        };
        assert!(
            intent
                .clone()
                .into_action(PlayerSeq(0), &viewer_ledger)
                .is_err()
        );
        let action = intent
            .into_action_with_preview(PlayerSeq(0), &viewer_ledger, Some(&remote_snapshot))
            .unwrap();
        assert!(viewer_ledger.clone().apply_op_result(&action).is_err());
        let viewer_leg = JointInventoryLeg {
            chunk: viewer_chunk,
            holders: vec![ServerId(1), ServerId(2)],
            finished: vec![ServerId(2), ServerId(1)],
            ground_ready: true,
            inventories: vec![viewer_snapshot],
        };
        let mut remote_leg = JointInventoryLeg {
            chunk: remote_chunk,
            holders: vec![ServerId(1), ServerId(2)],
            finished: vec![ServerId(1)],
            ground_ready: true,
            inventories: vec![remote_snapshot.clone()],
        };
        assert_eq!(
            preview_joint_inventory_action(&action, &[viewer_leg.clone(), remote_leg.clone()]),
            Err(JointInventoryError::IncompleteManifest)
        );
        remote_leg.finished.push(ServerId(2));
        let joint = preview_joint_inventory_action(&action, &[viewer_leg, remote_leg]).unwrap();
        assert_eq!(joint.operations.len(), 2);
        assert_eq!(
            viewer_ledger.snapshot(viewer_target).unwrap().cells[0].count,
            1
        );
        let mut holder = viewer_ledger;
        holder
            .install_owned_snapshot(remote_snapshot, vec![remote_chunk])
            .unwrap();
        holder.apply_op_result(&action).unwrap();
        assert!(holder.snapshot(viewer_target).unwrap().cells.is_empty());
        assert_eq!(holder.snapshot(remote_target).unwrap().cells[0].count, 1);
    }

    #[test]
    fn a_cross_inventory_transfer_commits_both_sides_atomically() {
        let player = InventoryTarget::Player(gid(1, 1));
        let chest = InventoryTarget::Virtual {
            host: ServerId(1),
            owner: ActorId::Player(gid(1, 1)),
            session: 9,
            kind: 2,
        };
        let snapshots = vec![
            InventoryStateSnapshot {
                target: player,
                revision: 4,
                cells: vec![cell(INV_MAIN, 0, 5, 12)],
            },
            InventoryStateSnapshot {
                target: chest,
                revision: 7,
                cells: vec![cell(INV_MAIN, 3, 5, 60)],
            },
        ];
        let reduction = reduce_inventory_gestures(
            &snapshots,
            &[InventoryGesture::Transfer {
                source: InventorySlotRef::new(player, loc(0)),
                destination: InventorySlotRef::new(chest, loc(3)),
                count: 4,
                destination_limit: 64,
            }],
        )
        .unwrap();
        let mut service = InventoryService::new();
        for snapshot in snapshots {
            service
                .install(snapshot.target, snapshot.revision, snapshot.cells)
                .unwrap();
        }
        service.mutate(reduction.mutation).unwrap();
        assert_eq!(service.snapshot(player).unwrap().cells[0].count, 8);
        assert_eq!(service.snapshot(chest).unwrap().cells[0].count, 64);
    }

    #[test]
    fn quick_craft_distributes_a_cursor_stack_without_duplication() {
        let player = InventoryTarget::Player(gid(1, 1));
        let snapshot = InventoryStateSnapshot {
            target: player,
            revision: 1,
            cells: vec![cell(INV_CURSOR, 0, 8, 9)],
        };
        let destinations = (0..3)
            .map(|slot| (InventorySlotRef::new(player, loc(slot)), 64))
            .collect();
        let reduction = reduce_inventory_gestures(
            std::slice::from_ref(&snapshot),
            &[InventoryGesture::QuickCraft {
                source: InventorySlotRef::new(player, InvLoc::new(INV_CURSOR, 0)),
                destinations,
                mode: QuickCraftMode::Even,
            }],
        )
        .unwrap();
        let mut service = InventoryService::new();
        service
            .install(snapshot.target, snapshot.revision, snapshot.cells)
            .unwrap();
        service.mutate(reduction.mutation).unwrap();
        let resulting = service.snapshot(player).unwrap();
        assert_eq!(resulting.cells.iter().map(|cell| cell.count).sum::<u8>(), 9);
        assert_eq!(resulting.cells.len(), 3);
        assert!(resulting.cells.iter().all(|cell| cell.count == 3));
    }

    #[test]
    fn close_drain_rebases_after_an_earlier_cursor_change() {
        let actor = gid(1, 1);
        let target = InventoryTarget::Player(actor);
        let source = InventorySlotRef::new(target, InvLoc::new(INV_CURSOR, 0));
        let destination = InventorySlotRef::new(target, loc(0));
        let chunk = actor_chunk();
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target,
                    revision: 1,
                    cells: vec![cell(INV_CURSOR, 0, 5, 4)],
                },
                vec![chunk],
            )
            .unwrap();
        let close = InventoryMutationIntent {
            actor: ActorId::Player(actor),
            tick: TickStamp(1),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::DrainToPlayer {
                player: actor,
                source,
                destinations: vec![(destination, 64)],
            }]),
            screen_session: None,
            actor_chunk: chunk,
            affected: vec![chunk],
        }
        .into_action(PlayerSeq(1), &ledger)
        .unwrap();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target,
                    revision: 2,
                    cells: vec![cell(INV_CURSOR, 0, 5, 7)],
                },
                vec![chunk],
            )
            .unwrap();
        let rebased = ledger.rebase_gesture_op(&close).unwrap();
        ledger.apply_op_result(&rebased).unwrap();
        let snapshot = ledger.snapshot(target).unwrap();
        assert_eq!(snapshot.cells, vec![cell(INV_MAIN, 0, 5, 7)]);
    }

    #[test]
    fn close_drain_drops_only_the_overflow_from_a_virtual_input() {
        let actor = gid(1, 1);
        let player = InventoryTarget::Player(actor);
        let grid = InventoryTarget::Virtual {
            host: ServerId(1),
            owner: ActorId::Player(actor),
            session: 9,
            kind: 1,
        };
        let snapshots = vec![
            InventoryStateSnapshot {
                target: player,
                revision: 1,
                cells: vec![cell(INV_MAIN, 0, 5, 60)],
            },
            InventoryStateSnapshot {
                target: grid,
                revision: 1,
                cells: vec![cell(INV_MAIN, 1, 5, 10), cell(INV_MAIN, 2, 99, 1)],
            },
        ];
        let reduction = reduce_inventory_gestures(
            &snapshots,
            &[
                InventoryGesture::DrainToPlayer {
                    player: actor,
                    source: InventorySlotRef::new(grid, loc(1)),
                    destinations: vec![(InventorySlotRef::new(player, loc(0)), 64)],
                },
                InventoryGesture::ClearResult {
                    slot: InventorySlotRef::new(grid, loc(2)),
                },
            ],
        )
        .unwrap();
        assert!(matches!(
            reduction.effects.as_slice(),
            [InventoryGestureEffect::Removed { stack, kind: InventoryRemovalKind::Drop, .. }]
                if stack.count == 6 && stack.item == 5
        ));
        let mut service = InventoryService::new();
        for snapshot in snapshots {
            service
                .install(snapshot.target, snapshot.revision, snapshot.cells)
                .unwrap();
        }
        service.mutate(reduction.mutation).unwrap();
        assert_eq!(service.snapshot(player).unwrap().cells[0].count, 64);
        assert!(service.snapshot(grid).unwrap().cells.is_empty());
    }

    #[test]
    fn quick_craft_session_accumulates_slots_until_one_finish_gesture() {
        let actor = gid(1, 3);
        let target = InventoryTarget::Player(actor);
        let session = InventoryScreenSession { actor, session: 7 };
        let source = InventorySlotRef::new(target, InvLoc::new(INV_CURSOR, 0));
        let first = InventorySlotRef::new(target, loc(1));
        let second = InventorySlotRef::new(target, loc(2));
        let mut sessions = InventoryScreenSessions::new();
        sessions.begin_quick_craft(session, source, QuickCraftMode::Even);
        assert!(sessions.add_quick_craft_slot(session, second, 64));
        assert!(sessions.add_quick_craft_slot(session, first, 64));
        assert!(sessions.add_quick_craft_slot(session, first, 64));
        let leased = InventoryTarget::LeasedPlayer {
            key: uuid::Uuid::from_u128(7),
            player: actor,
            incarnation: 11,
        };
        sessions.rename_targets(&[(target, leased)]);
        assert_eq!(
            sessions.finish_quick_craft(session),
            Some(InventoryGesture::QuickCraft {
                source: InventorySlotRef::new(leased, source.loc),
                destinations: vec![
                    (InventorySlotRef::new(leased, first.loc), 64),
                    (InventorySlotRef::new(leased, second.loc), 64),
                ],
                mode: QuickCraftMode::Even,
            })
        );
        assert!(sessions.finish_quick_craft(session).is_none());
    }

    #[test]
    fn recipe_craft_consumes_all_inputs_before_creating_output() {
        let player = InventoryTarget::Player(gid(1, 1));
        let grid = InventoryTarget::Virtual {
            host: ServerId(1),
            owner: ActorId::Player(gid(1, 1)),
            session: 12,
            kind: 1,
        };
        let first = cell(INV_MAIN, 0, 20, 3);
        let second = cell(INV_MAIN, 1, 21, 2);
        let snapshots = vec![
            InventoryStateSnapshot {
                target: grid,
                revision: 5,
                cells: vec![first.clone(), second.clone()],
            },
            InventoryStateSnapshot {
                target: player,
                revision: 8,
                cells: Vec::new(),
            },
        ];
        let reduction = reduce_inventory_gestures(
            &snapshots,
            &[InventoryGesture::RecipeCraft {
                ingredients: vec![
                    RecipeIngredient {
                        slot: InventorySlotRef::new(grid, first.loc),
                        expected: first,
                        count_per_craft: 1,
                    },
                    RecipeIngredient {
                        slot: InventorySlotRef::new(grid, second.loc),
                        expected: second,
                        count_per_craft: 1,
                    },
                ],
                destination: InventorySlotRef::new(player, loc(4)),
                result: cell(INV_MAIN, 4, 22, 4),
                repetitions: 2,
                destination_limit: 64,
            }],
        )
        .unwrap();
        let mut service = InventoryService::new();
        for snapshot in snapshots {
            service
                .install(snapshot.target, snapshot.revision, snapshot.cells)
                .unwrap();
        }
        service.mutate(reduction.mutation).unwrap();
        let grid = service.snapshot(grid).unwrap();
        assert_eq!(grid.cells.len(), 1);
        assert_eq!(grid.cells[0].count, 1);
        let player = service.snapshot(player).unwrap();
        assert_eq!(player.cells[0].item, 22);
        assert_eq!(player.cells[0].count, 8);
    }

    #[test]
    fn tick_owner_reduces_a_drop_against_its_current_ledger_snapshot() {
        let actor = gid(1, 1);
        let target = InventoryTarget::Player(actor);
        let mut ledger = InvLedger::new();
        ledger
            .install_snapshot(InventoryStateSnapshot {
                target,
                revision: 14,
                cells: vec![cell(INV_MAIN, 2, 31, 5)],
            })
            .unwrap();
        let action = InventoryMutationIntent {
            actor: actor.into(),
            tick: TickStamp(90),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::Remove {
                source: InventorySlotRef::new(target, loc(2)),
                count: 2,
                kind: InventoryRemovalKind::Drop,
            }]),
            screen_session: None,
            actor_chunk: actor_chunk(),
            affected: Vec::new(),
        }
        .into_action(PlayerSeq(4), &ledger)
        .unwrap();
        assert_eq!(action.expected_revisions, vec![(target, 14)]);
        assert_eq!(action.changes[0].before.count, 5);
        assert_eq!(action.changes[0].after.count, 3);
        let InventoryGestureEffect::Removed { stack, kind, .. } = &action.effects[0] else {
            panic!("drop did not retain its accepted side effect");
        };
        assert_eq!((stack.item, stack.count), (31, 2));
        assert_eq!(*kind, InventoryRemovalKind::Drop);
    }

    fn op(kind: InvOpKind, src: u16, dst: u16, item: u16, count: u8) -> InventoryOp {
        capture_inv_op(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            kind,
            loc(src),
            loc(dst),
            item,
            count,
            actor_chunk(),
            Vec::new(),
        )
    }

    #[test]
    fn move_replays_source_to_destination() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Move, 0, 1, 5, 10)),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 22)));
        assert_eq!(cells.cell(loc(1)), Some((5, 10)));
    }

    #[test]
    fn move_rejects_on_source_mismatch_without_mutation() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        cells.seed(loc(1), 5, 4);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Move, 0, 1, 9, 10)),
            InvVerdict::Rejected
        );
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Move, 0, 1, 5, 64)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 32)));
        assert_eq!(cells.cell(loc(1)), Some((5, 4)));
        assert_eq!(cells.total(5), 36);
    }

    #[test]
    fn move_rejects_when_destination_holds_other_item() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        cells.seed(loc(1), 7, 2);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Move, 0, 1, 5, 4)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 32)));
        assert_eq!(cells.cell(loc(1)), Some((7, 2)));
    }

    #[test]
    fn move_half_requires_exact_half() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 33);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::MoveHalf, 0, 1, 5, 16)),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 17)));
        assert_eq!(cells.cell(loc(1)), Some((5, 16)));
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::MoveHalf, 0, 2, 5, 9)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 17)));
        assert_eq!(cells.cell(loc(2)), None);
    }

    #[test]
    fn split_moves_single_item_to_empty_slot() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Split, 0, 1, 5, 1)),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 7)));
        assert_eq!(cells.cell(loc(1)), Some((5, 1)));
    }

    #[test]
    fn split_rejects_into_occupied_slot() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        cells.seed(loc(1), 5, 1);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Split, 0, 1, 5, 1)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.total(5), 9);
    }

    #[test]
    fn swap_exchanges_stacks_and_validates_source() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        cells.seed(loc(1), 7, 3);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Swap, 0, 1, 5, 8)),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(0)), Some((7, 3)));
        assert_eq!(cells.cell(loc(1)), Some((5, 8)));
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Swap, 0, 1, 9, 3)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.cell(loc(0)), Some((7, 3)));
    }

    #[test]
    fn consume_and_drop_never_duplicate_on_conflict() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 4);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Consume, 0, 0, 5, 1)),
            InvVerdict::Applied
        );
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Consume, 0, 0, 5, 4)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.total(5), 3);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Drop, 0, 0, 9, 1)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.total(5), 3);
    }

    #[test]
    fn pickup_stacks_only_onto_same_item_with_room() {
        let mut cells = MemCells::new();
        cells.seed(loc(1), 5, 60);
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Pickup, 0, 1, 5, 4)),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(1)), Some((5, 64)));
        assert_eq!(
            replay(&mut cells, &op(InvOpKind::Pickup, 0, 1, 5, 1)),
            InvVerdict::Rejected
        );
        assert_eq!(cells.total(5), 64);
    }

    #[test]
    fn set_overwrites_without_duplication() {
        let mut cells = MemCells::new();
        cells.seed(loc(3), 5, 64);
        let set = capture_inv_set(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(7),
            loc(3),
            7,
            2,
            Vec::new(),
            actor_chunk(),
            Vec::new(),
        );
        assert_eq!(replay(&mut cells, &set), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(3)), Some((7, 2)));
        assert_eq!(cells.total(5), 0);
    }

    #[test]
    fn ledger_rejects_stale_place_precondition() {
        let mut ledger = InvLedger::new();
        let at = InvLoc::new(INV_MAIN, 2);
        assert_eq!(
            ledger.check_place(gid(1, 1), at, 40, 64, 63),
            InvVerdict::Applied
        );
        assert_eq!(
            ledger.check_place(gid(1, 1), at, 40, 64, 63),
            InvVerdict::Rejected
        );
        assert_eq!(
            ledger.check_place(gid(1, 1), at, 40, 63, 62),
            InvVerdict::Applied
        );
        assert_eq!(ledger.get(gid(1, 1), at), Some((40, 62)));
    }

    #[test]
    fn ledger_replay_then_conflicting_replay_rejects() {
        let mut ledger = InvLedger::new();
        let first = op(InvOpKind::Move, 0, 1, 5, 10);
        ledger.record(gid(1, 1), loc(0), 5, 32);
        assert_eq!(ledger.apply_op(&first), InvVerdict::Applied);
        let conflict = op(InvOpKind::Move, 0, 2, 5, 30);
        assert_eq!(ledger.apply_op(&conflict), InvVerdict::Rejected);
        assert_eq!(ledger.get(gid(1, 1), loc(2)), None);
        let total: u32 = [0, 1]
            .iter()
            .filter_map(|slot| ledger.get(gid(1, 1), loc(*slot)))
            .map(|(_, count)| u32::from(count))
            .sum();
        assert_eq!(total, 32);
    }

    #[test]
    fn inv_ops_sort_deterministically() {
        let tick = TickStamp(11);
        let mut first = op(InvOpKind::Move, 0, 1, 5, 2);
        first.player_gid = Some(gid(2, 1));
        first.actor = first.player_gid.unwrap().into();
        let mut second = op(InvOpKind::Move, 0, 1, 5, 2);
        second.player_gid = Some(gid(1, 1));
        second.actor = second.player_gid.unwrap().into();
        let mut arrival = [first.clone(), second.clone()];
        sort_inv_ops_for_tick(tick, &mut arrival);
        let mut other = [second, first];
        sort_inv_ops_for_tick(tick, &mut other);
        assert_eq!(arrival, other);
    }

    #[test]
    fn precondition_rule_is_exact_match() {
        assert!(inv_precondition_met(40, 63, 40, 63));
        assert!(!inv_precondition_met(40, 62, 40, 63));
        assert!(!inv_precondition_met(41, 63, 40, 63));
        assert!(inv_stack_has(40, 63, 40, 1));
        assert!(!inv_stack_has(40, 0, 40, 1));
    }

    #[test]
    fn semantic_batch_rejects_stale_edit_without_partial_application() {
        let target = gid(1, 7);
        let first = InventoryCell {
            loc: InvLoc::new(INV_MAIN, 0),
            item: 5,
            count: 12,
            nbt: vec![1],
        };
        let second = InventoryCell {
            loc: InvLoc::new(INV_OFFHAND, 0),
            item: 7,
            count: 1,
            nbt: vec![2],
        };
        let mut service = InventoryService::new();
        let inventory = InventoryTarget::Player(target);
        service
            .install(target, 9, vec![first.clone(), second.clone()])
            .unwrap();
        let error = service.mutate(InventoryMutation {
            expected_revisions: vec![(inventory, 9)],
            operations: vec![
                SemanticInventoryOp {
                    target: inventory,
                    before: first.clone(),
                    after: InventoryCell {
                        count: 4,
                        ..first.clone()
                    },
                },
                SemanticInventoryOp {
                    target: inventory,
                    before: InventoryCell {
                        count: 2,
                        ..second.clone()
                    },
                    after: InventoryCell::empty(second.loc),
                },
            ],
        });

        assert_eq!(error, Err(InventoryMutationError::PreconditionFailed));
        let snapshot = service.snapshot(target).unwrap();
        assert_eq!(snapshot.revision, 9);
        assert_eq!(snapshot.cells, vec![first, second]);
    }

    #[tokio::test]
    async fn subscriber_observes_one_atomic_cross_inventory_delta() {
        let target = gid(2, 3);
        let source = InventoryCell {
            loc: InvLoc::new(INV_MAIN, 8),
            item: 40,
            count: 2,
            nbt: vec![9],
        };
        let destination = InventoryCell::empty(InvLoc::new(INV_OFFHAND, 0));
        let mut service = InventoryService::new();
        let inventory = InventoryTarget::Player(target);
        service.install(target, 14, vec![source.clone()]).unwrap();
        let (_, mut changes) = service.subscribe(target, 1).unwrap();
        let delta = service
            .mutate(InventoryMutation {
                expected_revisions: vec![(inventory, 14)],
                operations: vec![
                    SemanticInventoryOp {
                        target: inventory,
                        before: source.clone(),
                        after: InventoryCell::empty(source.loc),
                    },
                    SemanticInventoryOp {
                        target: inventory,
                        before: destination.clone(),
                        after: InventoryCell {
                            loc: destination.loc,
                            item: source.item,
                            count: source.count,
                            nbt: source.nbt.clone(),
                        },
                    },
                ],
            })
            .unwrap();

        assert_eq!(changes.recv().await.unwrap(), delta);
        let snapshot = service.snapshot(target).unwrap();
        assert_eq!(snapshot.revision, 15);
        assert_eq!(snapshot.cells.len(), 1);
        assert_eq!(snapshot.cells[0].loc, destination.loc);
        assert_eq!(snapshot.cells[0].count, 2);
    }

    #[test]
    fn multi_inventory_transfer_is_atomic_across_revisions() {
        let source_target = InventoryTarget::Player(gid(1, 1));
        let destination_target = InventoryTarget::Player(gid(2, 1));
        let source = InventoryCell {
            loc: loc(0),
            item: 40,
            count: 4,
            nbt: Vec::new(),
        };
        let destination = InventoryCell::empty(loc(0));
        let mut service = InventoryService::new();
        service
            .install(source_target, 7, vec![source.clone()])
            .unwrap();
        service.install(destination_target, 9, Vec::new()).unwrap();
        let mutation = InventoryMutation {
            expected_revisions: vec![(source_target, 7), (destination_target, 9)],
            operations: vec![
                SemanticInventoryOp {
                    target: source_target,
                    before: source.clone(),
                    after: InventoryCell::empty(source.loc),
                },
                SemanticInventoryOp {
                    target: destination_target,
                    before: destination,
                    after: source.clone(),
                },
            ],
        };
        let mut stale = mutation.clone();
        stale.expected_revisions[1].1 = 8;
        assert_eq!(
            service.mutate(stale),
            Err(InventoryMutationError::StaleRevision)
        );
        assert_eq!(
            service.snapshot(source_target).unwrap().cells,
            vec![source.clone()]
        );
        assert!(
            service
                .snapshot(destination_target)
                .unwrap()
                .cells
                .is_empty()
        );

        let delta = service.mutate(mutation).unwrap();
        assert_eq!(delta.revisions.len(), 2);
        let total: u32 = [source_target, destination_target]
            .into_iter()
            .flat_map(|target| service.snapshot(target).unwrap().cells)
            .map(|cell| u32::from(cell.count))
            .sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn ledger_rejects_stale_cross_inventory_transfer_without_partial_apply() {
        let source_target = InventoryTarget::Player(gid(1, 1));
        let destination_target = InventoryTarget::Player(gid(2, 1));
        let source = InventoryCell {
            loc: loc(0),
            item: 40,
            count: 4,
            nbt: Vec::new(),
        };
        let mut ledger = InvLedger::new();
        ledger
            .install_snapshot(InventoryStateSnapshot {
                target: source_target,
                revision: 7,
                cells: vec![source.clone()],
            })
            .unwrap();
        ledger
            .install_snapshot(InventoryStateSnapshot {
                target: destination_target,
                revision: 9,
                cells: Vec::new(),
            })
            .unwrap();
        let operation = capture_inventory_mutation(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(10),
            vec![(source_target, 7), (destination_target, 8)],
            vec![
                SemanticInventoryOp {
                    target: source_target,
                    before: source.clone(),
                    after: InventoryCell::empty(source.loc),
                },
                SemanticInventoryOp {
                    target: destination_target,
                    before: InventoryCell::empty(source.loc),
                    after: source.clone(),
                },
            ],
            actor_chunk(),
            Vec::new(),
        );
        assert_eq!(
            ledger.apply_op_result(&operation),
            Err(InventoryApplyError::Rejected)
        );
        assert_eq!(ledger.snapshot(source_target).unwrap().cells, vec![source]);
        assert!(
            ledger
                .snapshot(destination_target)
                .unwrap()
                .cells
                .is_empty()
        );
        assert_eq!(ledger.snapshot(source_target).unwrap().revision, 7);
        assert_eq!(ledger.snapshot(destination_target).unwrap().revision, 9);
    }

    #[test]
    fn rejected_source_change_reconciles_transitive_slot_dependencies() {
        let player = gid(1, 9);
        let chunk = crate::protocol::ChunkKey::new(crate::protocol::WorldId::new(0, 4), 0, 0);
        let mut pickup = capture_inv_op(
            player,
            PlayerSeq(1),
            TickStamp(20),
            InvOpKind::Pickup,
            loc(0),
            loc(0),
            5,
            4,
            chunk,
            vec![chunk],
        );
        pickup.changes = vec![SemanticInventoryOp {
            target: InventoryTarget::Player(player),
            before: InventoryCell::empty(loc(0)),
            after: InventoryCell {
                loc: loc(0),
                item: 5,
                count: 4,
                nbt: Vec::new(),
            },
        }];
        let moved = capture_inv_op(
            player,
            PlayerSeq(2),
            TickStamp(20),
            InvOpKind::MoveHalf,
            loc(0),
            loc(1),
            5,
            2,
            chunk,
            vec![chunk],
        );
        let split = capture_inv_op(
            player,
            PlayerSeq(3),
            TickStamp(20),
            InvOpKind::Split,
            loc(1),
            loc(2),
            5,
            1,
            chunk,
            vec![chunk],
        );
        let mut actions = vec![pickup, moved, split];
        link_inventory_dependencies(&mut actions);
        let rejected = crate::completion::ActionKey::inventory(&actions[0]);
        assert_eq!(
            dependent_inventory_actions(&actions, rejected),
            actions
                .iter()
                .map(crate::completion::ActionKey::inventory)
                .collect::<Vec<_>>()
        );

        let snapshot = InventoryStateSnapshot {
            target: InventoryTarget::Player(player),
            revision: 8,
            cells: Vec::new(),
        };
        let correction = corrective_inventory_delta(&actions[0], &[snapshot]);
        assert_eq!(correction.operations.len(), 1);
        assert_eq!(correction.operations[0].before.count, 4);
        assert!(correction.operations[0].after.is_empty());
    }

    #[test]
    fn player_block_entity_and_virtual_transfers_conserve_items() {
        let world = crate::protocol::WorldId::new(0, 77);
        let chunk = ChunkKey::new(world, 2, 3);
        let player = InventoryTarget::Player(gid(1, 4));
        let chest = InventoryTarget::Block {
            chunk,
            position: BlockPos {
                x: 33,
                y: 64,
                z: 49,
            },
        };
        let entity = InventoryTarget::Entity(
            EntityRef {
                origin: crate::identity::ServerId(2),
                local_id: 5,
                generation: 1,
                chunk,
            }
            .into(),
        );
        let crafting = InventoryTarget::Virtual {
            host: crate::identity::ServerId(1),
            owner: ActorId::Player(gid(1, 4)),
            session: 90,
            kind: 1,
        };
        let stack = |count| InventoryCell {
            loc: loc(0),
            item: 12,
            count,
            nbt: Vec::new(),
        };
        let mut service = InventoryService::new();
        service
            .install_owned(chest, 1, vec![stack(6)], vec![chunk])
            .unwrap();
        service
            .install_owned(player, 1, Vec::new(), vec![chunk])
            .unwrap();
        service
            .install_owned(entity, 1, Vec::new(), vec![chunk])
            .unwrap();
        service
            .install_owned(crafting, 1, Vec::new(), vec![chunk])
            .unwrap();

        let transfer = |from, from_revision, to, to_revision, count| InventoryMutation {
            expected_revisions: vec![(from, from_revision), (to, to_revision)],
            operations: vec![
                SemanticInventoryOp {
                    target: from,
                    before: stack(count),
                    after: InventoryCell::empty(loc(0)),
                },
                SemanticInventoryOp {
                    target: to,
                    before: InventoryCell::empty(loc(0)),
                    after: stack(count),
                },
            ],
        };
        service.mutate(transfer(chest, 1, player, 1, 6)).unwrap();
        service.mutate(transfer(player, 2, entity, 1, 6)).unwrap();
        service.mutate(transfer(entity, 2, crafting, 1, 6)).unwrap();
        let total: u32 = [player, chest, entity, crafting]
            .into_iter()
            .flat_map(|target| service.snapshot(target).unwrap().cells)
            .map(|cell| u32::from(cell.count))
            .sum();
        assert_eq!(total, 6);
        assert_eq!(service.remove(crafting).unwrap().cells, vec![stack(6)]);
        assert!(service.snapshot(crafting).is_none());
    }

    #[test]
    fn lost_pickup_rebases_later_quick_craft_to_corrected_counts() {
        let world = crate::protocol::WorldId::new(0, 78);
        let chunk = ChunkKey::new(world, 0, 0);
        let actor = gid(1, 9);
        let player = InventoryTarget::Player(actor);
        let dropped = InventoryTarget::Entity(
            EntityRef {
                origin: crate::identity::ServerId(2),
                local_id: 6,
                generation: 1,
                chunk,
            }
            .into(),
        );
        let cursor = InvLoc::new(INV_CURSOR, 0);
        let first = InvLoc::new(INV_MAIN, 0);
        let second = InvLoc::new(INV_MAIN, 1);
        let stack_at = |loc, count| InventoryCell {
            loc,
            item: 12,
            count,
            nbt: Vec::new(),
        };
        let mut accepted = InvLedger::new();
        accepted
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target: player,
                    revision: 1,
                    cells: vec![stack_at(cursor, 3)],
                },
                vec![chunk],
            )
            .unwrap();
        accepted
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target: dropped,
                    revision: 1,
                    cells: vec![stack_at(first, 5)],
                },
                vec![chunk],
            )
            .unwrap();
        let pickup = InventoryMutationIntent {
            actor: actor.into(),
            tick: TickStamp(90),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::Transfer {
                source: InventorySlotRef::new(dropped, first),
                destination: InventorySlotRef::new(player, cursor),
                count: 5,
                destination_limit: 64,
            }]),
            screen_session: None,
            actor_chunk: chunk,
            affected: vec![chunk],
        }
        .into_action(PlayerSeq(0), &accepted)
        .unwrap();
        let mut optimistic = accepted.clone();
        optimistic.apply_op_result(&pickup).unwrap();
        let quick_craft = InventoryMutationIntent {
            actor: actor.into(),
            tick: TickStamp(90),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::QuickCraft {
                source: InventorySlotRef::new(player, cursor),
                destinations: vec![
                    (InventorySlotRef::new(player, first), 64),
                    (InventorySlotRef::new(player, second), 64),
                ],
                mode: QuickCraftMode::Even,
            }]),
            screen_session: None,
            actor_chunk: chunk,
            affected: vec![chunk],
        }
        .into_action(PlayerSeq(1), &optimistic)
        .unwrap();

        let rebased = accepted.rebase_gesture_op(&quick_craft).unwrap();
        let delta = accepted.apply_op_result(&rebased).unwrap();
        let snapshot = accepted.snapshot(player).unwrap();
        assert_eq!(
            snapshot
                .cells
                .iter()
                .map(|cell| u16::from(cell.count))
                .sum::<u16>(),
            3
        );
        assert_eq!(
            snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == first)
                .map(|cell| cell.count),
            Some(1)
        );
        assert_eq!(
            snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == second)
                .map(|cell| cell.count),
            Some(1)
        );
        assert_eq!(
            snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == cursor)
                .map(|cell| cell.count),
            Some(1)
        );
        assert!(
            delta
                .operations
                .iter()
                .any(|change| change.after.loc == first)
        );
        assert_eq!(accepted.snapshot(dropped).unwrap().cells[0].count, 5);
    }

    #[test]
    fn concurrent_admin_move_and_consume_converge_without_duplication() {
        let target = gid(1, 7);
        let editor = gid(2, 3);
        let source = InventoryCell {
            loc: loc(0),
            item: 40,
            count: 4,
            nbt: Vec::new(),
        };
        let snapshot = InventoryStateSnapshot {
            target: InventoryTarget::Player(target),
            revision: 12,
            cells: vec![source.clone()],
        };
        let transaction = capture_inventory_transaction(
            editor,
            target,
            PlayerSeq(0),
            TickStamp(70),
            12,
            vec![
                SemanticInventoryOp {
                    target: InventoryTarget::Player(target),
                    before: source.clone(),
                    after: InventoryCell::empty(source.loc),
                },
                SemanticInventoryOp {
                    target: InventoryTarget::Player(target),
                    before: InventoryCell::empty(loc(1)),
                    after: InventoryCell {
                        loc: loc(1),
                        ..source.clone()
                    },
                },
            ],
            actor_chunk(),
            Vec::new(),
        );
        let mut consume = capture_inv_op(
            target,
            PlayerSeq(0),
            TickStamp(70),
            InvOpKind::Consume,
            loc(0),
            loc(0),
            40,
            1,
            actor_chunk(),
            Vec::new(),
        );
        consume.player_target = Some(target);
        let mut outcomes = Vec::new();
        for mut operations in [
            vec![transaction.clone(), consume.clone()],
            vec![consume.clone(), transaction.clone()],
        ] {
            sort_inv_ops_for_tick(TickStamp(70), &mut operations);
            let mut ledger = InvLedger::new();
            ledger.install_snapshot(snapshot.clone()).unwrap();
            for operation in &operations {
                let _ = ledger.apply_op(operation);
            }
            let result = ledger.snapshot(target).unwrap();
            let total: u32 = result.cells.iter().map(|cell| u32::from(cell.count)).sum();
            outcomes.push((result, total));
        }
        assert_eq!(outcomes[0], outcomes[1]);
        assert!(outcomes[0].1 <= 4);
        assert!(outcomes[0].1 >= 3);
    }

    #[test]
    fn selected_slot_changes_with_the_inventory_revision() {
        let target = InventoryTarget::Player(gid(1, 9));
        let selected = InvLoc::new(INV_SELECTED, 0);
        let mut ledger = InvLedger::new();
        ledger
            .install_snapshot(InventoryStateSnapshot {
                target,
                revision: 4,
                cells: vec![InventoryCell {
                    loc: selected,
                    item: 1,
                    count: 1,
                    nbt: Vec::new(),
                }],
            })
            .unwrap();
        let snapshot = ledger.snapshot(target).unwrap();
        let mutation = InventoryMutation::select_hotbar(&snapshot, 6).unwrap();
        let action = capture_inventory_mutation(
            gid(1, 9),
            PlayerSeq(3),
            TickStamp(8),
            mutation.expected_revisions,
            mutation.operations,
            actor_chunk(),
            Vec::new(),
        );
        let delta = ledger.apply_op_result(&action).unwrap();
        let after = ledger.snapshot(target).unwrap();
        assert_eq!(delta.revisions, vec![(target, 4, 5)]);
        assert_eq!(
            after
                .cells
                .iter()
                .find(|cell| cell.loc == selected)
                .unwrap()
                .item,
            7
        );
    }

    #[test]
    fn creative_drop_advances_owner_revision_without_mutating_slots() {
        let actor = gid(1, 9);
        let target = InventoryTarget::Player(actor);
        let before = InventoryStateSnapshot {
            target,
            revision: 11,
            cells: vec![InventoryCell {
                loc: loc(2),
                item: 44,
                count: 3,
                nbt: Vec::new(),
            }],
        };
        let dropped = InventoryCell {
            loc: loc(0),
            item: 44,
            count: 16,
            nbt: Vec::new(),
        };
        let reduction = reduce_inventory_gestures(
            std::slice::from_ref(&before),
            &[InventoryGesture::CreativeDrop {
                owner: target,
                stack: dropped.clone(),
            }],
        )
        .unwrap();
        assert!(reduction.mutation.operations.is_empty());
        assert_eq!(reduction.mutation.expected_revisions, vec![(target, 11)]);
        assert_eq!(
            reduction.effects,
            vec![InventoryGestureEffect::CreatedDrop {
                owner: target,
                stack: dropped,
            }]
        );
        let mut service = InventoryService::new();
        service
            .install(target, before.revision, before.cells.clone())
            .unwrap();
        let delta = service.mutate(reduction.mutation).unwrap();
        assert_eq!(delta.revisions, vec![(target, 11, 12)]);
        assert_eq!(service.snapshot(target).unwrap().cells, before.cells);
    }

    #[test]
    fn entity_actor_identity_survives_inventory_capture_and_rebase() {
        let chunk = actor_chunk();
        let actor = ActorId::Entity(crate::protocol::GlobalEntityId {
            origin: crate::identity::ServerId(2),
            local_id: 17,
            generation: 3,
        });
        let target = InventoryTarget::Entity(EntityInventoryId {
            origin: crate::identity::ServerId(2),
            local_id: 17,
            generation: 3,
        });
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target,
                    revision: 1,
                    cells: vec![InventoryCell {
                        loc: loc(0),
                        item: 8,
                        count: 2,
                        nbt: Vec::new(),
                    }],
                },
                vec![chunk],
            )
            .unwrap();
        let intent = InventoryMutationIntent {
            actor,
            tick: TickStamp(9),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::Remove {
                source: InventorySlotRef::new(target, loc(0)),
                count: 1,
                kind: InventoryRemovalKind::Consume,
            }]),
            screen_session: None,
            actor_chunk: chunk,
            affected: vec![chunk],
        };
        let action = intent.into_action(PlayerSeq(0), &ledger).unwrap();
        let rebased = ledger.rebase_gesture_op(&action).unwrap();
        assert_eq!(action.actor, actor);
        assert_eq!(rebased.actor, actor);
        assert_eq!(
            crate::completion::ActionKey::inventory(&action).actor,
            actor
        );
    }

    #[test]
    fn menu_choice_cannot_smuggle_an_untrusted_inventory_removal() {
        let player = gid(1, 4);
        let actor = ActorId::Player(player);
        let chunk = actor_chunk();
        let target = InventoryTarget::Virtual {
            host: ServerId(1),
            owner: actor,
            session: 41,
            kind: 7,
        };
        let session = InventoryScreenSession {
            actor: player,
            session: 41,
        };
        let before = InventoryCell {
            loc: loc(2),
            item: 91,
            count: 1,
            nbt: b"old".to_vec(),
        };
        let after = InventoryCell {
            loc: loc(2),
            item: 91,
            count: 1,
            nbt: b"new".to_vec(),
        };
        let snapshots = vec![InventoryStateSnapshot {
            target,
            revision: 8,
            cells: vec![before.clone()],
        }];
        let screen = InventoryScreenSnapshot {
            session,
            sync_id: 3,
            state_id: 0,
            window_kind: 7,
            container_slots: 3,
            slots: Vec::new(),
            cursor: InventoryCell::empty(loc(0)),
            affected: vec![chunk],
            preview: None,
            preview_incarnation: None,
            preview_lease: None,
            preview_authority: None,
        };
        let kind = InventoryMenuKind::RenameAnvil {
            name: "new".to_string(),
        };
        let metadata = InventoryMenuMetadata {
            session,
            sync_id: 3,
            window_kind: 7,
            expected_revisions: vec![(target, 8)],
            rules: vec![InventoryMenuRule {
                kind: kind.clone(),
                client_gestures: Vec::new(),
                gestures: vec![InventoryGesture::ExactSet {
                    destination: InventorySlotRef::new(target, loc(2)),
                    before,
                    after,
                }],
                effect: InventoryMenuEffect::AnvilNameChanged {
                    name: "new".to_string(),
                    level_cost: 0,
                },
            }],
        };
        let smuggled = InventoryMenuIntent {
            actor,
            tick: TickStamp(12),
            session,
            sync_id: 3,
            window_kind: 7,
            expected_revisions: vec![(target, 8)],
            actor_chunk: chunk,
            affected: Vec::new(),
            kind,
            companion_gestures: Some(vec![InventoryGesture::Remove {
                source: InventorySlotRef::new(target, loc(2)),
                count: 1,
                kind: InventoryRemovalKind::Destroy,
            }]),
        };
        let mut authorized = smuggled.clone();
        authorized.companion_gestures = None;
        let accepted = reduce_inventory_menu(&screen, &snapshots, &metadata, authorized).unwrap();
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(snapshots[0].clone(), vec![chunk])
            .unwrap();
        let action = accepted.into_action(PlayerSeq(0), &ledger).unwrap();
        ledger.apply_op_result(&action).unwrap();
        assert_eq!(ledger.snapshot(target).unwrap().cells[0].nbt, b"new");
        assert_eq!(
            reduce_inventory_menu(&screen, &snapshots, &metadata, smuggled).unwrap_err(),
            InventoryMenuError::InvalidChoice
        );
    }

    #[test]
    fn menu_choice_cannot_execute_against_a_newer_inventory_revision() {
        let player = gid(1, 5);
        let actor = ActorId::Player(player);
        let chunk = actor_chunk();
        let target = InventoryTarget::Virtual {
            host: ServerId(1),
            owner: actor,
            session: 42,
            kind: 12,
        };
        let session = InventoryScreenSession {
            actor: player,
            session: 42,
        };
        let state = InventoryCell {
            loc: loc(0),
            item: 1,
            count: 2,
            nbt: Vec::new(),
        };
        let next = InventoryCell {
            count: 3,
            ..state.clone()
        };
        let kind =
            InventoryMenuKind::ContainerButton(InventoryContainerButton::StonecutterRecipe {
                recipe: 6,
            });
        let result = reduce_inventory_menu(
            &InventoryScreenSnapshot {
                session,
                sync_id: 9,
                state_id: 0,
                window_kind: 12,
                container_slots: 2,
                slots: Vec::new(),
                cursor: InventoryCell::empty(loc(1)),
                affected: vec![chunk],
                preview: None,
                preview_incarnation: None,
                preview_lease: None,
                preview_authority: None,
            },
            &[InventoryStateSnapshot {
                target,
                revision: 10,
                cells: vec![state.clone()],
            }],
            &InventoryMenuMetadata {
                session,
                sync_id: 9,
                window_kind: 12,
                expected_revisions: vec![(target, 9)],
                rules: vec![InventoryMenuRule {
                    kind: kind.clone(),
                    client_gestures: Vec::new(),
                    gestures: vec![InventoryGesture::ExactSet {
                        destination: InventorySlotRef::new(target, loc(0)),
                        before: state,
                        after: next,
                    }],
                    effect: InventoryMenuEffect::ContainerSelectionChanged {
                        selection: InventoryContainerButton::StonecutterRecipe { recipe: 6 },
                    },
                }],
            },
            InventoryMenuIntent {
                actor,
                tick: TickStamp(13),
                session,
                sync_id: 9,
                window_kind: 12,
                expected_revisions: vec![(target, 9)],
                actor_chunk: chunk,
                affected: Vec::new(),
                kind,
                companion_gestures: None,
            },
        );
        assert_eq!(result.unwrap_err(), InventoryMenuError::StaleRevision);
    }

    #[test]
    fn competing_beacon_choices_consume_one_payment_only_once() {
        let player = gid(1, 7);
        let chunk = actor_chunk();
        let target = InventoryTarget::Block {
            chunk,
            position: BlockPos { x: 0, y: 80, z: 0 },
        };
        let session = InventoryScreenSession {
            actor: player,
            session: 9,
        };
        let loc = loc(0);
        let payment = InventoryCell {
            loc,
            item: 932,
            count: 1,
            nbt: Vec::new(),
        };
        let screen = InventoryScreenSnapshot {
            session,
            sync_id: 9,
            state_id: 0,
            window_kind: 9,
            container_slots: 1,
            slots: vec![InventoryScreenSlot {
                screen_slot: 0,
                slot: InventorySlotRef::new(target, loc),
                cell: payment.clone(),
                limit: 1,
            }],
            cursor: InventoryCell::empty(loc),
            affected: vec![chunk],
            preview: None,
            preview_incarnation: None,
            preview_lease: None,
            preview_authority: None,
        };
        let before = InventoryStateSnapshot {
            target,
            revision: 12,
            cells: vec![payment],
        };
        let state = BeaconMenuState {
            revision: 4,
            levels: 1,
            primary_effect: None,
            secondary_effect: None,
        };
        let menu = beacon_menu_metadata(&screen, &before, state, true).unwrap();
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(before.clone(), vec![chunk])
            .unwrap();
        let invalid = reduce_inventory_menu(
            &screen,
            std::slice::from_ref(&before),
            &menu,
            InventoryMenuIntent {
                actor: ActorId::Player(player),
                tick: TickStamp(20),
                session,
                sync_id: 9,
                window_kind: 9,
                expected_revisions: vec![(target, 12)],
                actor_chunk: chunk,
                affected: vec![chunk],
                kind: InventoryMenuKind::SetBeacon {
                    primary_effect: 1,
                    secondary_effect: None,
                },
                companion_gestures: Some(vec![InventoryGesture::Remove {
                    source: InventorySlotRef::new(target, loc),
                    count: 2,
                    kind: InventoryRemovalKind::Consume,
                }]),
            },
        );
        assert_eq!(invalid.unwrap_err(), InventoryMenuError::InvalidChoice);
        assert_eq!(ledger.snapshot(target).unwrap().cells, before.cells);
        let unavailable = reduce_inventory_menu(
            &screen,
            std::slice::from_ref(&before),
            &menu,
            InventoryMenuIntent {
                actor: ActorId::Player(player),
                tick: TickStamp(20),
                session,
                sync_id: 9,
                window_kind: 9,
                expected_revisions: vec![(target, 12)],
                actor_chunk: chunk,
                affected: vec![chunk],
                kind: InventoryMenuKind::SetBeacon {
                    primary_effect: 5,
                    secondary_effect: None,
                },
                companion_gestures: None,
            },
        );
        assert_eq!(
            unavailable.unwrap_err(),
            InventoryMenuError::UnsupportedChoice
        );
        assert_eq!(ledger.snapshot(target).unwrap().cells, before.cells);
        let actions: Vec<_> = [(PlayerSeq(0), 1_u16, true), (PlayerSeq(1), 3, false)]
            .into_iter()
            .map(|(sequence, primary, bedrock)| {
                let reduction = reduce_inventory_menu(
                    &screen,
                    std::slice::from_ref(&before),
                    &menu,
                    InventoryMenuIntent {
                        actor: ActorId::Player(player),
                        tick: TickStamp(20),
                        session,
                        sync_id: 9,
                        window_kind: 9,
                        expected_revisions: vec![(target, 12)],
                        actor_chunk: chunk,
                        affected: vec![chunk],
                        kind: InventoryMenuKind::SetBeacon {
                            primary_effect: primary,
                            secondary_effect: None,
                        },
                        companion_gestures: bedrock.then(|| {
                            vec![InventoryGesture::Remove {
                                source: InventorySlotRef::new(target, loc),
                                count: 1,
                                kind: InventoryRemovalKind::Consume,
                            }]
                        }),
                    },
                )
                .unwrap();
                (sequence, reduction.into_action(sequence, &ledger).unwrap())
            })
            .collect();
        for (sequence, action) in actions {
            let result = ledger.apply_op_result(&action);
            if sequence.0 == 0 {
                assert!(result.is_ok());
            } else {
                assert_eq!(result, Err(InventoryApplyError::Rejected));
            }
        }
        let final_state = ledger.snapshot(target).unwrap();
        assert!(final_state.cells.is_empty());
        assert_eq!(final_state.revision, 13);
    }

    #[test]
    fn beacon_choice_waits_for_earlier_payment_click_to_settle() {
        let player = gid(1, 7);
        let chunk = actor_chunk();
        let player_target = InventoryTarget::Player(player);
        let beacon = InventoryTarget::Block {
            chunk,
            position: BlockPos { x: 0, y: 80, z: 0 },
        };
        let session = InventoryScreenSession {
            actor: player,
            session: 9,
        };
        let payment = cell(INV_MAIN, 0, 932, 1);
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target: player_target,
                    revision: 1,
                    cells: vec![payment.clone()],
                },
                vec![chunk],
            )
            .unwrap();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target: beacon,
                    revision: 1,
                    cells: Vec::new(),
                },
                vec![chunk],
            )
            .unwrap();
        let click = InventoryMutationIntent {
            actor: ActorId::Player(player),
            tick: TickStamp(20),
            payload: InventoryIntentPayload::Gestures(vec![InventoryGesture::Transfer {
                source: InventorySlotRef::new(player_target, loc(0)),
                destination: InventorySlotRef::new(beacon, loc(0)),
                count: 1,
                destination_limit: 1,
            }]),
            screen_session: None,
            actor_chunk: chunk,
            affected: vec![chunk],
        }
        .into_action(PlayerSeq(1), &ledger)
        .unwrap();
        let mut barrier = InventoryCaptureBarrier::default();
        barrier.register(&click);
        assert!(barrier.blocks(ActorId::Player(player), &[beacon]));
        let empty_screen = InventoryScreenSnapshot {
            session,
            sync_id: 9,
            state_id: 0,
            window_kind: 9,
            container_slots: 1,
            slots: vec![InventoryScreenSlot {
                screen_slot: 0,
                slot: InventorySlotRef::new(beacon, loc(0)),
                cell: InventoryCell::empty(loc(0)),
                limit: 1,
            }],
            cursor: InventoryCell::empty(InvLoc::new(INV_CURSOR, 0)),
            affected: vec![chunk],
            preview: None,
            preview_incarnation: None,
            preview_lease: None,
            preview_authority: None,
        };
        let state = BeaconMenuState {
            revision: 1,
            levels: 1,
            primary_effect: None,
            secondary_effect: None,
        };
        let before = ledger.snapshot(beacon).unwrap();
        let unavailable = beacon_menu_metadata(&empty_screen, &before, state, false).unwrap();
        assert!(!unavailable.rules.iter().any(|rule| {
            rule.kind
                == InventoryMenuKind::SetBeacon {
                    primary_effect: 1,
                    secondary_effect: None,
                }
        }));
        ledger.apply_op_result(&click).unwrap();
        barrier.resolve(crate::completion::ActionKey::inventory(&click));
        assert!(!barrier.blocks(ActorId::Player(player), &[beacon]));
        let paid = ledger.snapshot(beacon).unwrap();
        let mut paid_screen = empty_screen;
        paid_screen.slots[0].cell = payment;
        let metadata = beacon_menu_metadata(&paid_screen, &paid, state, true).unwrap();
        let intent = InventoryMenuIntent {
            actor: ActorId::Player(player),
            tick: TickStamp(21),
            session,
            sync_id: 9,
            window_kind: 9,
            expected_revisions: metadata.expected_revisions.clone(),
            actor_chunk: chunk,
            affected: vec![chunk],
            kind: InventoryMenuKind::SetBeacon {
                primary_effect: 1,
                secondary_effect: None,
            },
            companion_gestures: None,
        };
        let operation = reduce_inventory_menu(&paid_screen, &[paid], &metadata, intent)
            .unwrap()
            .into_action(PlayerSeq(2), &ledger)
            .unwrap();
        ledger.apply_op_result(&operation).unwrap();
        assert!(ledger.snapshot(beacon).unwrap().cells.is_empty());
        assert_eq!(operation.menu_effects.len(), 1);
    }

    #[test]
    fn block_virtual_inventory_installs_only_under_its_real_chunk() {
        let chunk = ChunkKey::new(crate::protocol::WorldId::new(0, 9), 3, -2);
        let target = InventoryTarget::BlockVirtual {
            host: ServerId(1),
            chunk,
            position: BlockPos {
                x: 49,
                y: 64,
                z: -31,
            },
            session: 12,
            kind: 7,
        };
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target,
                    revision: 1,
                    cells: vec![cell(INV_MAIN, 0, 5, 3)],
                },
                Vec::new(),
            )
            .unwrap();
        assert_eq!(ledger.target_chunks(target), Some([chunk].as_slice()));
        let invalid = InventoryTarget::BlockVirtual {
            host: ServerId(1),
            chunk,
            position: BlockPos {
                x: 32,
                y: 64,
                z: -31,
            },
            session: 12,
            kind: 7,
        };
        assert_eq!(
            ledger.install_owned_snapshot(
                InventoryStateSnapshot {
                    target: invalid,
                    revision: 1,
                    cells: vec![],
                },
                Vec::new(),
            ),
            Err(InventoryMutationError::InvalidLocation)
        );
        assert_eq!(ledger.snapshot(target).unwrap().cells.len(), 1);
    }

    #[test]
    fn target_rename_preserves_contents_revisions_and_chunks_atomically() {
        let player = GlobalPlayerId::new(ServerId(1), crate::identity::PlayerSlot(7));
        let chunk = ChunkKey::new(crate::protocol::WorldId::new(0, 9), 3, -2);
        let old = InventoryTarget::Player(player);
        let new = InventoryTarget::LeasedPlayer {
            key: uuid::Uuid::from_u128(8),
            player,
            incarnation: 3,
        };
        let occupied = InventoryTarget::LeasedPlayer {
            key: uuid::Uuid::from_u128(9),
            player,
            incarnation: 4,
        };
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target: old,
                    revision: 17,
                    cells: vec![cell(INV_MAIN, 0, 5, 3)],
                },
                vec![chunk],
            )
            .unwrap();
        ledger
            .install_owned_snapshot(
                InventoryStateSnapshot {
                    target: occupied,
                    revision: 2,
                    cells: vec![cell(INV_MAIN, 1, 6, 1)],
                },
                vec![chunk],
            )
            .unwrap();
        let before = ledger.clone();
        assert_eq!(
            ledger.rename_targets(&[(old, occupied)]),
            Err(InventoryMutationError::TargetCollision)
        );
        assert_eq!(ledger.snapshot(old), before.snapshot(old));
        assert_eq!(ledger.snapshot(occupied), before.snapshot(occupied));
        ledger.rename_targets(&[(old, new)]).unwrap();
        assert!(ledger.snapshot(old).is_none());
        assert_eq!(ledger.snapshot(new).unwrap().revision, 17);
        assert_eq!(
            ledger.snapshot(new).unwrap().cells,
            before.snapshot(old).unwrap().cells
        );
        assert_eq!(ledger.target_chunks(new), Some([chunk].as_slice()));
        assert_eq!(ledger.snapshot(occupied), before.snapshot(occupied));
    }
    #[test]
    fn chunk_unit_requires_complete_root_and_returns_exact_inventory() {
        let a = ChunkKey::new(crate::protocol::WorldId::new(0, 9), 3, -2);
        let b = ChunkKey::new(a.world, 4, -2);
        let c = ChunkKey::new(a.world, 5, -2);
        let shared = InventoryTarget::Player(GlobalPlayerId::new(ServerId(1), crate::identity::PlayerSlot(7)));
        let unrelated = InventoryTarget::Player(GlobalPlayerId::new(ServerId(1), crate::identity::PlayerSlot(8)));
        let mut ledger = InvLedger::new();
        ledger.install_owned_snapshot(InventoryStateSnapshot { target: shared, revision: 17, cells: vec![cell(INV_MAIN, 0, 5, 3)] }, vec![a, b]).unwrap();
        ledger.install_owned_snapshot(InventoryStateSnapshot { target: unrelated, revision: 21, cells: vec![cell(INV_MAIN, 1, 6, 1)] }, vec![c]).unwrap();
        let shared_before = ledger.snapshot(shared).unwrap();
        let unrelated_before = ledger.snapshot(unrelated).unwrap();
        assert!(matches!(ledger.take_chunk_unit(&[a].into()), Err(InventoryMutationError::TargetCollision)));
        assert_eq!(ledger.snapshot(shared), Some(shared_before.clone()));
        let mut unit = ledger.take_chunk_unit(&[a, b].into()).unwrap();
        assert!(ledger.snapshot(shared).is_none());
        assert!(ledger.targets_for_chunk(a).is_empty());
        assert_eq!(ledger.snapshot(unrelated), Some(unrelated_before.clone()));
        assert_eq!(unit.snapshot(shared), Some(shared_before.clone()));
        assert_eq!(unit.targets_for_chunk(a), vec![shared]);
        unit.relocate_target(shared, vec![b]).unwrap();
        assert!(unit.targets_for_chunk(a).is_empty());
        ledger.return_chunk_unit(unit).unwrap();
        assert_eq!(ledger.snapshot(shared), Some(shared_before));
        assert_eq!(ledger.snapshot(unrelated), Some(unrelated_before));
        assert_eq!(ledger.targets_for_chunk(b), vec![shared]);
    }

    #[test]
    fn renamed_root_updates_chunk_index_and_collision_preserves_returned_unit() {
        let chunk = ChunkKey::new(crate::protocol::WorldId::new(0, 9), 3, -2);
        let old = InventoryTarget::Player(GlobalPlayerId::new(ServerId(1), crate::identity::PlayerSlot(7)));
        let new = InventoryTarget::Player(GlobalPlayerId::new(ServerId(1), crate::identity::PlayerSlot(8)));
        let mut ledger = InvLedger::new();
        ledger.install_owned_snapshot(InventoryStateSnapshot { target: old, revision: 17, cells: vec![cell(INV_MAIN, 0, 5, 3)] }, vec![chunk]).unwrap();
        ledger.rename_targets(&[(old, new)]).unwrap();
        assert_eq!(ledger.targets_for_chunk(chunk), vec![new]);
        let unit = ledger.take_chunk_unit(&[chunk].into()).unwrap();
        ledger.install_owned_snapshot(InventoryStateSnapshot { target: new, revision: 20, cells: vec![cell(INV_MAIN, 0, 6, 1)] }, vec![chunk]).unwrap();
        let rejected = ledger.return_chunk_unit(unit).unwrap_err();
        assert_eq!(rejected.snapshot(new).unwrap().revision, 17);
        assert_eq!(ledger.snapshot(new).unwrap().revision, 20);
    }

}
