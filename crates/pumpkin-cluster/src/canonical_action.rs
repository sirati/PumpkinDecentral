use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::actor::ActorId;
pub use crate::canonical_inventory::{InventoryGestureReplayIntent, InventorySlotBinding};
use crate::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use crate::inventory::InventoryCell;
use crate::item_action::ActorLivingState;
use crate::protocol::{BlockPos, ChunkKey, GlobalEntityId};
use crate::time::TickStamp;

pub const MAX_LOGICAL_RECORDS: usize = 1_048_576;
pub const MAX_LOGICAL_ACTION_BYTES: usize = 64 * 1024 * 1024;

pub fn check_logical_budget(records: usize, encoded_bytes: usize) -> Result<(), ApplyFailure> {
    if records > MAX_LOGICAL_RECORDS || encoded_bytes > MAX_LOGICAL_ACTION_BYTES {
        Err(ApplyFailure::ResourceLimit)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PlayerKey(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Stakeholder {
    Chunk(ChunkKey),
    Player(PlayerKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlockBox {
    pub min: BlockPos,
    pub max: BlockPos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ChunkPart {
    Whole,
    Block(BlockPos),
    BlockEntity(BlockPos),
    BlockSection(i32),
    LightSection(i32),
    ActorOccupancy,
    ActorTags {
        actor: ActorId,
    },
    Bounds(BlockBox),
    Entity(GlobalEntityId),
    Player(GlobalPlayerId),
    ActorRelation {
        actor: ActorId,
        relation: crate::actor_relation::ActorRelation,
    },
    LivingProperty {
        actor: ActorId,
        property: crate::living_property::LivingProperty,
    },
    VehicleProperty {
        actor: GlobalEntityId,
        property: crate::vehicle_property::VehicleProperty,
    },
    ObjectProperty {
        actor: GlobalEntityId,
        property: crate::object_state::ObjectProperty,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum InventoryId {
    Player {
        inv: u8,
        slot: u16,
    },
    Block {
        inv: u8,
        slot: u16,
    },
    Entity {
        inv: u8,
        slot: u16,
    },
    Virtual {
        host: ServerId,
        kind: u16,
        session: u64,
        inv: u8,
        slot: u16,
    },
}

impl InventoryId {
    #[must_use]
    pub const fn location(self) -> (u8, u16) {
        match self {
            Self::Player { inv, slot }
            | Self::Block { inv, slot }
            | Self::Entity { inv, slot }
            | Self::Virtual { inv, slot, .. } => (inv, slot),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Target {
    Chunk {
        key: ChunkKey,
        part: ChunkPart,
        inventory: Option<InventoryId>,
    },
    Player {
        key: PlayerKey,
        lease: Option<LivePlayerLease>,
        inventory: Option<InventoryId>,
    },
}

impl Target {
    pub fn player_avatar(self) -> Option<(ChunkKey, GlobalPlayerId)> {
        match self {
            Self::Chunk {
                key,
                part: ChunkPart::Player(gid),
                inventory: None,
            }
            | Self::Chunk {
                key,
                part:
                    ChunkPart::ActorRelation {
                        actor: ActorId::Player(gid),
                        ..
                    },
                inventory: None,
            }
            | Self::Chunk {
                key,
                part:
                    ChunkPart::LivingProperty {
                        actor: ActorId::Player(gid),
                        ..
                    },
                inventory: None,
            } => Some((key, gid)),
            _ => None,
        }
    }

    #[must_use]
    pub const fn stakeholder(self) -> Stakeholder {
        match self {
            Self::Chunk { key, .. } => Stakeholder::Chunk(key),
            Self::Player { key, .. } => Stakeholder::Player(key),
        }
    }

    #[must_use]
    pub fn is_valid(self) -> bool {
        match self {
            Self::Chunk {
                part: ChunkPart::ObjectProperty { actor, property },
                inventory,
                ..
            } => {
                inventory.is_none()
                    && actor.generation != 0
                    && property != crate::object_state::ObjectProperty::FrameItem
            }
            Self::Chunk {
                part: ChunkPart::VehicleProperty { actor, .. },
                inventory,
                ..
            } => inventory.is_none() && actor.generation != 0,
            Self::Chunk {
                part: ChunkPart::ActorRelation { actor, relation },
                inventory,
                ..
            } => inventory.is_none() && crate::actor_relation::accepts(actor, relation, &None),
            Self::Chunk {
                part: ChunkPart::LivingProperty { actor, .. },
                inventory,
                ..
            } => inventory.is_none() && !matches!(actor, ActorId::Peer(_)),
            Self::Chunk {
                part: ChunkPart::Block(_),
                inventory: Some(InventoryId::Entity { .. } | InventoryId::Player { .. }),
                ..
            }
            | Self::Chunk {
                part: ChunkPart::Entity(_),
                inventory: Some(InventoryId::Block { .. } | InventoryId::Player { .. }),
                ..
            }
            | Self::Chunk {
                part: ChunkPart::Whole,
                inventory: Some(_),
                ..
            }
            | Self::Chunk {
                part: ChunkPart::BlockEntity(_),
                inventory: Some(_),
                ..
            }
            | Self::Chunk {
                part: ChunkPart::Bounds(_),
                inventory: Some(_),
                ..
            }
            | Self::Chunk {
                part: ChunkPart::Player(_),
                inventory: Some(_),
                ..
            }
            | Self::Player {
                inventory: Some(InventoryId::Block { .. } | InventoryId::Entity { .. }),
                ..
            } => false,
            Self::Player {
                key,
                lease,
                inventory,
            } => match (lease, inventory) {
                (Some(lease), Some(_)) => lease.key == key && lease.incarnation != 0,
                (None, None) => true,
                _ => false,
            },
            Self::Chunk {
                key,
                part: ChunkPart::Block(pos) | ChunkPart::BlockEntity(pos),
                ..
            } => pos.x.div_euclid(16) == key.x() && pos.z.div_euclid(16) == key.z(),
            Self::Chunk {
                key,
                part: ChunkPart::Bounds(bounds),
                ..
            } => {
                bounds.min.x <= bounds.max.x
                    && bounds.min.y <= bounds.max.y
                    && bounds.min.z <= bounds.max.z
                    && bounds.min.x.div_euclid(16) == key.x()
                    && bounds.max.x.div_euclid(16) == key.x()
                    && bounds.min.z.div_euclid(16) == key.z()
                    && bounds.max.z.div_euclid(16) == key.z()
            }
            Self::Chunk { .. } => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActionId {
    pub actor: ActorId,
    pub incarnation: u64,
    pub sequence: u16,
    pub family: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActionRef {
    pub tick: TickStamp,
    pub id: ActionId,
}

impl crate::structural_map::OrderedPrefix<TickStamp> for ActionRef {
    fn ordered_prefix(&self) -> &TickStamp { &self.tick }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LivePlayerLease {
    pub key: PlayerKey,
    pub gid: GlobalPlayerId,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientOrigin {
    JavaBlockSequence(i32),
    JavaClick { session: u64, state_id: i32 },
    BedrockStackRequest { id: i32, session: u64 },
    BedrockLegacyTransaction(i32),
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionClass {
    Infallible,
    Replayable,
    Undoable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKind {
    Block,
    Actor,
    Item,
    Inventory,
    Combat,
    World,
    PlayerAdministration,
    ScheduledTick,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ScheduledTickKind {
    Block,
    Fluid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ScheduledTickOrigin {
    GenesisPrimary,
    Action {
        cause: ActionRef,
        producer: crate::realm_completion::PeerGeneration,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ScheduledTickFact {
    pub origin: ScheduledTickOrigin,
    pub executor: crate::realm_completion::PeerGeneration,
    pub kind: ScheduledTickKind,
    pub id: u16,
    pub position: BlockPos,
    pub due: TickStamp,
    pub priority: i8,
    pub source_order: u64,
}

/// Returns every due callback in a stable order, or refuses the entire page.
/// A caller must reserve capacity for all returned callbacks before draining
/// the queue; truncating this list would permanently lose scheduled work.
pub fn due_scheduled_ticks(
    queue: &[ScheduledTickFact],
    through: TickStamp,
    capacity: usize,
) -> Result<Vec<ScheduledTickFact>, usize> {
    let due_count = queue
        .iter()
        .filter(|tick| !tick.due.is_after(through))
        .count();
    if due_count > capacity {
        return Err(due_count);
    }
    let mut due = queue
        .iter()
        .copied()
        .filter(|tick| !tick.due.is_after(through))
        .collect::<Vec<_>>();
    due.sort_by_key(|tick| {
        (
            core::cmp::Reverse(through.distance_since(tick.due)),
            tick.priority,
            tick.source_order,
            tick.kind,
            tick.position,
            tick.id,
            tick.origin,
        )
    });
    Ok(due)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    ActorTags(BTreeSet<String>),
    Block(u16),
    BlockEntity(Vec<u8>),
    BlockSection(Vec<u16>),
    LightSection { sky: Vec<u8>, block: Vec<u8> },
    ActorOccupancy(crate::actor_occupancy::ActorOccupancyRoot),
    Inventory(InventoryCell),
    Living(Box<ActorLivingState>),
    LivingProperty(crate::living_property::LivingPropertyValue),
    ActorRelation(Option<crate::item_contact::ContactIdentity>),
    VehicleProperty(crate::vehicle_property::VehiclePropertyValue),
    ObjectProperty(crate::object_property::ObjectPropertyValue),
    Scalar(i64),
    ScheduledQueue(Vec<ScheduledTickFact>),
    Entity(Box<crate::entity_body::EntityBody>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cell {
    pub revision: u64,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Condition {
    pub target: Target,
    pub expected: Expected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SpatialEpoch {
    Admission {
        lease: LivePlayerLease,
        admitted_at: TickStamp,
    },
    Action(ActionRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expected {
    Missing,
    IssuerEntityAdmission(Box<crate::issuer_entity_admission::IssuerAdmissionWitness>),
    RevisionEquals(u64),
    ActorOccupancyClear(crate::actor_occupancy::ActorOccupancyWitness),
    HangingOccupancyClear(crate::actor_occupancy::ActorOccupancyWitness),
    EntitiesOccupancyClear(crate::actor_occupancy::ActorOccupancyWitness),
    ActorOccupancyDomain(crate::actor_occupancy::ActorOccupancyWitness),
    SpatialEpochEquals(SpatialEpoch),
    VehicleTerminal(Box<crate::vehicle_terminal::VehicleTerminalIntent>),
    ObjectTerminal(Box<crate::object_tick::ObjectTerminalWitness>),
    EntityPlacement(Box<crate::entity_placement::EntityPlacementIntent>),
    HopperCollector { birth: crate::entity_body::EntityBirth, owner: crate::realm_completion::PeerGeneration },
    EntityBirthOwned {
        birth: crate::entity_body::EntityBirth,
        owner: crate::realm_completion::PeerGeneration,
    },
    EntityBirthKind {
        birth: crate::entity_body::EntityBirth,
        owner: crate::realm_completion::PeerGeneration,
        kind: u16,
    },
    DroppedItemStack {
        birth: crate::entity_body::EntityBirth,
        owner: crate::realm_completion::PeerGeneration,
        epoch: SpatialEpoch,
        item: u16,
        count: u8,
        components: Vec<u8>,
    },
    ActorControlled {
        birth: crate::entity_body::EntityBirth,
        issuer: crate::realm_completion::PeerGeneration,
    },
    Exact(Cell),
    BlockState(u16),
    ScheduledTickFact {
        fact: ScheduledTickFact,
        present: bool,
    },
    ScheduledTickPresenceExcept {
        kind: ScheduledTickKind,
        id: u16,
        position: BlockPos,
        except: ScheduledTickFact,
        present: bool,
    },
    ScheduledTickPresence {
        kind: ScheduledTickKind,
        id: u16,
        position: BlockPos,
        present: bool,
    },
    ScalarAtLeast(i64),
    LivingUnsignedAtLeast(u32),
    InventoryItem {
        item: u16,
        nbt: Vec<u8>,
        minimum: u8,
    },
    ExperienceOrb {
        birth: crate::entity_body::EntityBirth,
        owner: crate::realm_completion::PeerGeneration,
        epoch: SpatialEpoch,
        amount: u32,
    },
    DroppedItem {
        birth: crate::entity_body::EntityBirth,
        condition: crate::entity_body::DroppedItemCondition,
    },
}

impl Expected {
    pub(crate) fn matches(&self, actual: Option<&Cell>) -> bool {
        match self {
            Self::Missing => actual.is_none(),
            Self::IssuerEntityAdmission(witness) => actual.is_none() && witness.valid(),
            Self::RevisionEquals(revision) => actual.is_some_and(|cell| cell.revision == *revision),
            Self::ActorOccupancyClear(witness) => actual.is_some_and(|cell| witness.matches_clear(cell)),
            Self::HangingOccupancyClear(witness) => actual.is_some_and(|cell| witness.matches_hanging_clear(cell)),
            Self::EntitiesOccupancyClear(witness) => actual.is_some_and(|cell| witness.matches_entities_clear(cell)),
            Self::ActorOccupancyDomain(witness) => actual.is_some_and(|cell| witness.matches_domain(cell)),
            Self::SpatialEpochEquals(epoch) => actual.is_some_and(|cell| match &cell.value {
                Value::Entity(body) => body.spatial_epoch == *epoch,
                Value::Living(living) => living.avatar.as_ref().is_some_and(|avatar| avatar.spatial_epoch == *epoch),
                _ => false,
            }),
            Self::ActorControlled { birth, issuer } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::Entity(body) if &body.birth == birth && body.owner == *issuer)
            }),
            Self::VehicleTerminal(intent) => intent.matches(actual),
            Self::ObjectTerminal(witness) => witness.matches(actual),
            Self::EntityPlacement(witness) => witness.matches_source(actual),
            Self::HopperCollector { birth, owner } => actual.is_some_and(|cell| matches!(&cell.value,
                Value::Entity(body) if &body.birth == birth && body.owner == *owner
                    && matches!(&body.payload, crate::entity_body::EntityPayload::Vehicle(vehicle)
                        if vehicle.kind == crate::entity_body::VehicleKind::HopperMinecart && vehicle.hopper_enabled && vehicle.damage_milli <= 40_000))),
            Self::EntityBirthOwned { birth, owner } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::Entity(body) if &body.birth == birth && body.owner == *owner)
            }),
            Self::EntityBirthKind { birth, owner, kind } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::Entity(body) if &body.birth == birth
                    && body.owner == *owner && body.kind == *kind)
            }),
            Self::DroppedItemStack { birth, owner, epoch, item, count, components } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::Entity(body) if &body.birth == birth && body.owner == *owner
                    && body.spatial_epoch == *epoch && matches!(&body.payload, crate::entity_body::EntityPayload::Item(stack)
                        if stack.item == *item && stack.count == *count && &stack.components == components))
            }),
            Self::Exact(expected) => actual == Some(expected),
            Self::BlockState(expected) => actual.is_some_and(|cell| cell.value == Value::Block(*expected)),
            Self::ScheduledTickFact { fact, present } => actual.is_some_and(|cell| matches!(&cell.value,
                Value::ScheduledQueue(queue) if queue.contains(fact) == *present)),
            Self::ScheduledTickPresenceExcept { kind, id, position, except, present } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::ScheduledQueue(queue) if queue.contains(except)
                    && queue.iter().any(|tick| tick != except && tick.kind == *kind && tick.id == *id && tick.position == *position) == *present)
            }),
            Self::ScheduledTickPresence { kind, id, position, present } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::ScheduledQueue(queue)
                    if queue.iter().any(|tick| tick.kind == *kind && tick.id == *id && tick.position == *position) == *present)
            }),
            Self::ScalarAtLeast(minimum) => actual.is_some_and(|cell| matches!(&cell.value, Value::Scalar(value) if *value >= *minimum)),
            Self::LivingUnsignedAtLeast(minimum) => actual.is_some_and(|cell| matches!(&cell.value,
                Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(value)) if *value >= *minimum)),
            Self::ExperienceOrb { birth, owner, epoch, amount } => actual.is_some_and(|cell|
                matches!(&cell.value, Value::Entity(body) if body.birth == *birth && body.owner == *owner
                    && body.spatial_epoch == *epoch && matches!(&body.payload,
                        crate::entity_body::EntityPayload::Experience(orb) if orb.amount == *amount && orb.age < 6000 && orb.count > 0 && orb.health_milli > 0))),
            Self::InventoryItem { item, nbt, minimum } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::Inventory(value) if value.item == *item && value.nbt == *nbt && value.count >= *minimum)
            }),
            Self::DroppedItem { birth, condition } => actual.is_some_and(|cell| {
                matches!(&cell.value, Value::Entity(body) if &body.birth == birth
                    && matches!(&body.payload, crate::entity_body::EntityPayload::Item(item) if item.matches(*condition)))
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityMoveAuthority {
    pub birth: crate::entity_body::EntityBirth,
    pub body_owner: crate::realm_completion::PeerGeneration,
    pub issuer: crate::realm_completion::PeerGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    TickBlockInventory { intent: crate::canonical_block_inventory_tick::BlockInventoryTickIntent },
    TransformMenuInventory { intent: crate::canonical_menu_transform::MenuInventoryTransformIntent },
    MenuVirtualInventoryAdmission { admission: crate::canonical_menu::MenuVirtualInventoryAdmission },
    TradeItems { intent: crate::canonical_merchant::MerchantTradeIntent },
    RetireEntity { intent: crate::canonical_entity_retirement::EntityRetirementIntent },
    CraftItems {
        intent: crate::canonical_recipe::RecipeInventoryIntent,
    },
    CloseInventory {
        intent: crate::canonical_inventory_close::InventoryCloseIntent,
    },
    EntityAdmission {
        target: Target,
        body: crate::entity_body::EntityBody,
        inventory: crate::entity_inventory_admission::EntityInventoryAdmission,
    },
    UpdateVehicleProperty {
        target: Target,
        authority: EntityMoveAuthority,
        value: crate::vehicle_property::VehiclePropertyValue,
    },
    UpdateMobContinuation {
        target: Target,
        authority: EntityMoveAuthority,
        brain: crate::mob_continuation::MobBrain,
    },
    CreativeItems {
        intent: crate::creative_inventory::CreativeInventoryIntent,
    },
    PlayerAvatarLifecycle {
        target: Target,
        intent: crate::avatar_lifecycle::AvatarLifecycleIntent,
    },
    TransformItems {
        intent: crate::canonical_inventory::ItemTransformIntent,
    },
    CrossbowItems {
        intent: crate::crossbow_items::CrossbowItems,
    },
    CollectProjectile { intent: crate::projectile_collection::ProjectileCollectionIntent },
    ProjectileShooterTeleport { intent: crate::projectile_teleport::ProjectileShooterTeleport },
    Put {
        target: Target,
        value: Value,
    },
    Delete {
        target: Target,
    },
    Add {
        target: Target,
        amount: i64,
    },
    ScheduleTick {
        target: Target,
        tick: ScheduledTickFact,
    },
    DrainScheduledTicks {
        target: Target,
        through: TickStamp,
    },
    CompleteScheduledTick {
        target: Target,
        fact: ScheduledTickFact,
    },
    MoveItems {
        source: Target,
        destination: Target,
        count: u8,
        max_stack: u8,
    },
    MovePlayerAvatar {
        source: Target,
        destination: Target,
        avatar: crate::item_action::PlayerAvatarState,
        velocity_micro: [i64; 3],
    },
    ResetPlayerLiving {
        target: Target,
        template: crate::item_action::ActorLivingState,
    },
    RespawnPlayerAvatar {
        source: Target,
        destination: Target,
        avatar: crate::item_action::PlayerAvatarState,
        max_health_attribute: u16,
    },
    SetPlayerCamera {
        target: Target,
        camera: Option<crate::item_contact::ContactObservation>,
    },
    SetPlayerSleeping {
        target: Target,
        sleeping: Option<crate::item_action::PlayerSleepingState>,
    },
    SetPlayerRespawn {
        target: Target,
        respawn: Option<crate::item_action::PlayerRespawnState>,
    },
    MoveEntityBody {
        source: Target,
        destination: Target,
        spatial: crate::entity_body::EntitySpatial,
        authority: Option<EntityMoveAuthority>,
    },
    ChangeDroppedItem {
        target: Target,
        issuer: crate::realm_completion::PeerGeneration,
        body_owner: crate::realm_completion::PeerGeneration,
        birth: crate::entity_body::EntityBirth,
        change: crate::entity_body::DroppedItemChange,
    },
    AdvanceExperienceLifetime {
        target: Target,
        owner: crate::realm_completion::PeerGeneration,
        birth: crate::entity_body::EntityBirth,
    },
    MergeDroppedItems {
        source: Target,
        destination: Target,
        max_stack: u8,
        count: u8,
    },
}

impl Effect {
    pub fn has_variable_write_footprint(&self) -> bool {
        matches!(
            self,
            Self::TransformItems { .. }
                | Self::CollectProjectile { .. }
                | Self::CrossbowItems { .. }
                | Self::CreativeItems { .. }
                | Self::CraftItems { .. }
                | Self::TradeItems { .. }
                | Self::TransformMenuInventory { .. }
                | Self::TickBlockInventory { .. }
                | Self::CloseInventory { .. }
        )
    }

    fn direct_targets(&self) -> impl Iterator<Item = Target> {
        let mut targets = [None, None];
        let mut additional: &[InventorySlotBinding] = &[];
        let mut recipe_targets = Vec::new();
        let mut admission = None;
        let mut entity_admission = None;
        match self {
            Self::TickBlockInventory { intent } => recipe_targets.extend(intent.effect_targets()),
            Self::TransformMenuInventory { intent } => recipe_targets.extend(intent.bindings().into_iter().map(|binding| binding.target)),
            Self::MenuVirtualInventoryAdmission { admission } => recipe_targets.extend(admission.entries().into_iter().map(|(target, _)| target)),
            Self::CraftItems { intent } => recipe_targets = intent.targets(),
            Self::TradeItems { intent } => recipe_targets = intent.targets(),
            Self::RetireEntity { intent } => recipe_targets = intent.targets().collect(),
            Self::CloseInventory { intent } => recipe_targets = intent.targets(),
            Self::EntityAdmission {
                target,
                body,
                inventory,
            } => {
                targets[0] = Some(*target);
                if let Target::Chunk { key, .. } = target {
                    entity_admission = Some((*key, body.id, inventory));
                }
            }
            Self::CreativeItems { intent } => {
                targets[0] = Some(intent.mode);
                additional = &intent.bindings;
            }
            Self::TransformItems { intent } => {
                targets[0] = Some(intent.source.target);
                additional = &intent.remainder_destinations;
            }
            Self::CrossbowItems { intent } => recipe_targets.extend(intent.targets()),
            Self::CollectProjectile { intent } => recipe_targets.extend(intent.effect_targets()),
            Self::ProjectileShooterTeleport { intent } => recipe_targets.extend(intent.targets()),
            Self::PlayerAvatarLifecycle { target, intent } => {
                targets[0] = Some(*target);
                if let crate::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                    lease,
                    inventory,
                    ..
                } = intent
                {
                    admission = Some((*lease, inventory));
                }
            }
            Self::Put { target, .. }
            | Self::UpdateVehicleProperty { target, .. }
            | Self::UpdateMobContinuation { target, .. }
            | Self::Delete { target }
            | Self::Add { target, .. }
            | Self::ScheduleTick { target, .. }
            | Self::DrainScheduledTicks { target, .. }
            | Self::CompleteScheduledTick { target, .. }
            | Self::ResetPlayerLiving { target, .. }
            | Self::SetPlayerCamera { target, .. }
            | Self::SetPlayerSleeping { target, .. }
            | Self::SetPlayerRespawn { target, .. }
            | Self::ChangeDroppedItem { target, .. }
            | Self::AdvanceExperienceLifetime { target, .. } => {
                targets[0] = Some(*target);
            }
            Self::MoveItems {
                source,
                destination,
                ..
            }
            | Self::MovePlayerAvatar {
                source,
                destination,
                ..
            }
            | Self::RespawnPlayerAvatar {
                source,
                destination,
                ..
            }
            | Self::MoveEntityBody {
                source,
                destination,
                ..
            }
            | Self::MergeDroppedItems {
                source,
                destination,
                ..
            } => {
                targets[0] = Some(*source);
                targets[1] = Some(*destination);
            }
        }
        let targets = targets
            .into_iter()
            .flatten()
            .chain(additional.iter().map(|binding| binding.target))
            .chain(recipe_targets)
            .chain(
                admission
                    .into_iter()
                    .flat_map(|(lease, inventory)| inventory.targets(lease)),
            )
            .chain(
                entity_admission
                    .into_iter()
                    .flat_map(|(chunk, entity, inventory)| {
                        inventory.entries(chunk, entity).map(|(target, _)| target)
                    }),
            )
            .collect::<Vec<_>>();
        targets.into_iter()
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> {
        let targets = self.direct_targets().collect::<Vec<_>>();
        let roots = targets.iter().filter_map(|target| crate::derived_actor_occupancy::root_alias(*target)).collect::<BTreeSet<_>>();
        targets.into_iter().chain(roots)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UndoProcedure {
    RestoreWrittenTargets,
    AdjustInventoryCount { target: Target },
    AdjustScalar { target: Target },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryCountRule {
    Half,
    All,
    Exact(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryReplayIntent {
    pub source: Target,
    pub item: u16,
    pub components: Vec<u8>,
    pub count: InventoryCountRule,
    pub ordered_destinations: Vec<InventoryReplayCandidate>,
    pub selector_revision: u64,
    pub destination_policy: InventoryDestinationPolicy,
    pub equipment: Vec<crate::canonical_inventory::InventoryEquipmentBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryDestinationPolicy {
    Ordered,
    MergeThenEmpty { merge_only: Vec<Target> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryReplayCandidate {
    pub target: Target,
    pub max_stack: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryReplayFailure {
    SourceChanged,
    MissingCell(Target),
    NoCapacity,
}

fn transferable_item(value: &Value, hopper: bool) -> Option<InventoryCell> {
    match value {
        Value::Inventory(item) => Some(item.clone()),
        Value::Entity(body) => match &body.payload {
            crate::entity_body::EntityPayload::Item(item)
                if item.matches(if hopper { crate::entity_body::DroppedItemCondition::HopperReady } else { crate::entity_body::DroppedItemCondition::PickupReady }) =>
            {
                Some(InventoryCell {
                    loc: crate::inventory::InvLoc { inv: 0, slot: 0 },
                    item: item.item,
                    count: item.count,
                    nbt: item.components.clone(),
                })
            }
            _ => None,
        },
        _ => None,
    }
}

impl InventoryReplayIntent {
    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(self.source)
            .chain(
                self.ordered_destinations
                    .iter()
                    .map(|candidate| candidate.target),
            )
            .chain(self.equipment.iter().flat_map(|binding| binding.targets()))
    }

    pub fn derive_effects_with_context<S: ActionState>(
        &self,
        values: &BTreeMap<Target, Cell>,
        context: &S,
    ) -> Result<Vec<Effect>, InventoryReplayFailure> {
        let mut effects = self.derive_effects(values)?;
        crate::canonical_inventory::append_equipment_effects(
            values,
            &mut effects,
            &self.equipment,
            context,
        )
        .map_err(|_| InventoryReplayFailure::SourceChanged)?;
        Ok(effects)
    }
    pub fn derive_effects(
        &self,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, InventoryReplayFailure> {
        let Some(source) = values
            .get(&self.source)
            .and_then(|cell| transferable_item(&cell.value, self.hopper_manifest(values)))
        else {
            return Err(InventoryReplayFailure::MissingCell(self.source));
        };
        if source.is_empty() || source.item != self.item || source.nbt != self.components {
            return Err(InventoryReplayFailure::SourceChanged);
        }
        let requested = match self.count {
            InventoryCountRule::Half => source.count.div_ceil(2),
            InventoryCountRule::All => source.count,
            InventoryCountRule::Exact(count) if count <= source.count => count,
            InventoryCountRule::Exact(_) => return Err(InventoryReplayFailure::SourceChanged),
        };
        let mut remaining = requested;
        let mut effects = Vec::new();
        let phases: &[u8] = match &self.destination_policy {
            InventoryDestinationPolicy::Ordered => &[0],
            _ => &[1, 2],
        };
        for phase in phases {
            for candidate in &self.ordered_destinations {
                let Some(Cell {
                    value: Value::Inventory(destination),
                    ..
                }) = values.get(&candidate.target)
                else {
                    return Err(InventoryReplayFailure::MissingCell(candidate.target));
                };
                if *phase == 1 && destination.is_empty()
                    || *phase == 2 && !destination.is_empty()
                    || destination.is_empty()
                        && matches!(&self.destination_policy,
                    InventoryDestinationPolicy::MergeThenEmpty { merge_only } if merge_only.contains(&candidate.target))
                {
                    continue;
                }
                if !destination.is_empty()
                    && (destination.item != source.item || destination.nbt != source.nbt)
                {
                    continue;
                }
                let count = remaining.min(candidate.max_stack.saturating_sub(destination.count));
                if count == 0 {
                    continue;
                }
                effects.push(Effect::MoveItems {
                    source: self.source,
                    destination: candidate.target,
                    count,
                    max_stack: candidate.max_stack,
                });
                remaining -= count;
                if remaining == 0 {
                    break;
                }
            }
            if remaining == 0 {
                break;
            }
        }
        if effects.is_empty()
            || matches!(self.count, InventoryCountRule::Exact(_)) && remaining != 0
        {
            return Err(InventoryReplayFailure::NoCapacity);
        }
        Ok(effects)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SemanticIntent {
    AreaCloud(Box<crate::area_cloud::AreaCloudIntent>),
    MenuInventoryTransform(crate::canonical_menu_transform::MenuInventoryTransformIntent),
    Wake(Box<crate::sleep_action::FrozenWake>),
    Respawn(crate::respawn_action::SemanticRespawn),
    ActorTags(crate::actor_tags::ActorTagsIntent),
    MobBreeding(Box<crate::mob_breeding::MobBreedingIntent>),
    RecipeInventory(crate::canonical_recipe::RecipeInventoryIntent),
    MenuSessionChange(crate::canonical_menu::MenuSessionIntent),
    MerchantTrade(crate::canonical_merchant::MerchantTradeIntent),
    InventoryClose(crate::canonical_inventory_close::InventoryCloseIntent),
    CreativeInventory(crate::creative_inventory::CreativeInventoryIntent),
    CollectExperience(crate::experience_orb::ExperienceCollection),
    ExperienceDamage(crate::experience_orb::ExperienceDamage),
    MergeExperience(crate::experience_orb::ExperienceMerge),
    DropItem(crate::item_drop::DropItemIntent),
    DeathInventory(crate::death_inventory::DeathInventoryIntent),
    ProjectileTick(crate::projectile_tick::ProjectileTickIntent),
    CollectProjectile(crate::projectile_collection::ProjectileCollectionIntent),
    ItemUse(crate::item_use::ItemUseIntent),
    VehicleTick(crate::vehicle_tick::VehicleTickIntent),
    World(crate::world_action::WorldReplayIntent),
    InventoryMove(InventoryReplayIntent),
    Pickup {
        contact: crate::item_contact::PickupContact,
        inventory: InventoryReplayIntent,
    },
    MergeDroppedItems {
        contact: crate::item_contact::MergeContact,
        max_stack: u8,
    },
    InventoryGestures(InventoryGestureReplayIntent),
    Combat(crate::combat_action::CombatIntent),
    Object(crate::object_action::ObjectIntent),
    ObjectTick(crate::object_tick::ObjectTickIntent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    #[serde(default)]
    pub edit_authority: Option<crate::inventory_edit_authority::InventoryEditAuthority>,
    pub id: ActionId,
    pub tick: TickStamp,
    pub class: ActionClass,
    pub chunk_transition: bool,
    pub actor_chunk: Option<ChunkKey>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub player_leases: Vec<LivePlayerLease>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub player_chunks: Vec<(PlayerKey, ChunkKey)>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub stakeholders: Vec<Stakeholder>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub dependencies: Vec<ActionRef>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub conditions: Vec<Condition>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub effects: Vec<Effect>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub counterfactual_conditions: Vec<Target>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub counterfactual_effects: Vec<Target>,
    pub semantic_intent: Option<SemanticIntent>,
    pub factual_result: Option<AppliedAction>,
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub undo: Vec<UndoProcedure>,
    pub kind: ActionKind,
    pub client_origin: ClientOrigin,
    #[serde(default)]
    #[serde(deserialize_with = "crate::bounded_vec::deserialize")]
    pub presentation: Vec<crate::world_presentation::WorldPresentationIntent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionProposal {
    #[serde(skip)]
    pub command: Option<(
        crate::realm_completion::PeerGeneration,
        u64,
        crate::player_command_control::PlayerCommandRequest,
    )>,
    pub id: ActionId,
    pub tick: TickStamp,
    pub lease: Option<LivePlayerLease>,
    pub client_origin: ClientOrigin,
    pub screen_generation: u64,
    pub claimed_targets: Vec<Target>,
    pub claim: ClaimedAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClaimedAction {
    EntityAdmissionRequest(crate::mob_initialization::EntityAdmissionRequest),
    ItemUse(crate::item_use::ItemUseClaim),
    PlayerCommand,
    Inventory(crate::canonical_inventory::InventoryGestureProposal),
    InventoryMenu(crate::canonical_recipe::InventoryMenuProposal),
    InventoryMenuSession(crate::canonical_menu::MenuSessionProposal),
    InventoryClose(crate::canonical_inventory_close::InventoryCloseProposal),
    Block(crate::block_action::BlockUseClaim),
    WorldInteraction(WorldInteractionIntent),
    PlayerAdministration(crate::player_admin::PlayerAdministrativeAction),
    Combat(crate::combat_action::AttackClaim),
    Respawn(crate::combat_action::RespawnClaim),
    Wake(crate::sleep_action::WakeClaim),
    Dismount(crate::vehicle_dismount::DismountClaim),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldInteractionIntent {
    pub owner_install_generation: u64,
    pub actor_chunk: ChunkKey,
    pub target_chunk: ChunkKey,
    pub position: BlockPos,
    pub face: u8,
    pub cursor_million: [i32; 3],
    pub hand: u8,
    pub sneaking: bool,
}

impl WorldInteractionIntent {
    #[must_use]
    pub fn valid(&self) -> bool {
        self.owner_install_generation != 0
            && self.face < 6
            && self.hand < 2
            && self
                .cursor_million
                .iter()
                .all(|coordinate| (0..=1_000_000).contains(coordinate))
            && self.position.x.div_euclid(16) == self.target_chunk.x()
            && self.position.z.div_euclid(16) == self.target_chunk.z()
            && self.actor_chunk.world == self.target_chunk.world
    }
}

impl ActionProposal {
    pub fn player_command(
        lease: LivePlayerLease,
        origin: crate::realm_completion::PeerGeneration,
        transport_epoch: u64,
        request: crate::player_command_control::PlayerCommandRequest,
        tick: TickStamp,
    ) -> Self {
        Self {
            command: Some((origin, transport_epoch, request)),
            id: ActionId {
                actor: ActorId::Player(lease.gid),
                incarnation: lease.incarnation,
                sequence: 0,
                family: 7,
            },
            tick,
            lease: Some(lease),
            client_origin: ClientOrigin::None,
            screen_generation: 0,
            claimed_targets: Vec::new(),
            claim: ClaimedAction::PlayerCommand,
        }
    }

    pub fn entity_command(
        identity: crate::item_contact::ContactIdentity,
        origin: crate::realm_completion::PeerGeneration,
        transport_epoch: u64,
        request: crate::player_command_control::PlayerCommandRequest,
        tick: TickStamp,
    ) -> Option<Self> {
        let crate::item_contact::ContactIdentity::Entity { id, birth } = identity else {
            return None;
        };
        if !matches!(&request.target,
            crate::player_command_control::CommandTarget::Entity { identity: supplied, .. }
                if supplied == &crate::item_contact::ContactIdentity::Entity { id, birth: birth.clone() })
        {
            return None;
        }
        Some(Self {
            command: Some((origin, transport_epoch, request)),
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: u64::from(id.generation),
                sequence: 0,
                family: 7,
            },
            tick,
            lease: None,
            client_origin: ClientOrigin::None,
            screen_generation: 0,
            claimed_targets: Vec::new(),
            claim: ClaimedAction::PlayerCommand,
        })
    }

    pub fn inventory(
        lease: LivePlayerLease,
        sequence: PlayerSeq,
        tick: TickStamp,
        client_origin: ClientOrigin,
        intent: crate::canonical_inventory::InventoryGestureProposal,
    ) -> Self {
        Self {
            command: None,
            id: ActionId {
                actor: ActorId::Player(lease.gid),
                incarnation: lease.incarnation,
                sequence: sequence.0,
                family: 2,
            },
            tick,
            lease: Some(lease),
            client_origin,
            screen_generation: intent.screen_session.map_or(0, |session| session.session),
            claimed_targets: Vec::new(),
            claim: ClaimedAction::Inventory(intent),
        }
    }

    pub fn inventory_menu(
        lease: LivePlayerLease,
        sequence: PlayerSeq,
        tick: TickStamp,
        client_origin: ClientOrigin,
        intent: crate::canonical_recipe::InventoryMenuProposal,
    ) -> Self {
        Self {
            command: None,
            id: ActionId {
                actor: ActorId::Player(lease.gid),
                incarnation: lease.incarnation,
                sequence: sequence.0,
                family: 2,
            },
            tick,
            lease: Some(lease),
            client_origin,
            screen_generation: intent.session.session,
            claimed_targets: Vec::new(),
            claim: ClaimedAction::InventoryMenu(intent),
        }
    }

    pub fn inventory_menu_session(
        lease: LivePlayerLease,
        sequence: PlayerSeq,
        tick: TickStamp,
        client_origin: ClientOrigin,
        intent: crate::canonical_menu::MenuSessionProposal,
    ) -> Self {
        let screen_generation = match &intent.operation {
            crate::canonical_menu::MenuSessionOperation::Open { state, .. } => state.session,
            crate::canonical_menu::MenuSessionOperation::Select { session, .. } => *session,
        };
        Self {
            command: None,
            id: ActionId { actor: ActorId::Player(lease.gid), incarnation: lease.incarnation,
                sequence: sequence.0, family: 2 },
            tick, lease: Some(lease), client_origin, screen_generation,
            claimed_targets: Vec::new(), claim: ClaimedAction::InventoryMenuSession(intent),
        }
    }

    pub fn inventory_close(
        lease: LivePlayerLease,
        sequence: PlayerSeq,
        tick: TickStamp,
        client_origin: ClientOrigin,
        intent: crate::canonical_inventory_close::InventoryCloseProposal,
    ) -> Self {
        Self {
            command: None,
            id: ActionId {
                actor: ActorId::Player(lease.gid),
                incarnation: lease.incarnation,
                sequence: sequence.0,
                family: 2,
            },
            tick,
            lease: Some(lease),
            client_origin,
            screen_generation: intent.session.map_or(0, |session| session.session),
            claimed_targets: Vec::new(),
            claim: ClaimedAction::InventoryClose(intent),
        }
    }

    #[must_use]
    pub fn world_interaction(
        id: ActionId,
        tick: TickStamp,
        lease: LivePlayerLease,
        client_origin: ClientOrigin,
        intent: WorldInteractionIntent,
    ) -> Self {
        Self {
            command: None,
            id,
            tick,
            lease: Some(lease),
            client_origin,
            screen_generation: 0,
            claimed_targets: vec![Target::Chunk {
                key: intent.target_chunk,
                part: ChunkPart::Block(intent.position),
                inventory: None,
            }],
            claim: ClaimedAction::WorldInteraction(intent),
        }
    }

    #[must_use]
    pub fn combat(claim: crate::combat_action::AttackClaim, lease: LivePlayerLease) -> Self {
        let part = match claim.target {
            ActorId::Player(id) => ChunkPart::Player(id),
            ActorId::Entity(id) => ChunkPart::Entity(id),
            ActorId::Peer(_) => ChunkPart::Whole,
        };
        Self {
            command: None,
            id: ActionId {
                actor: ActorId::Player(claim.attacker),
                incarnation: lease.incarnation,
                sequence: claim.sequence.0,
                family: 4,
            },
            tick: claim.tick,
            lease: Some(lease),
            client_origin: ClientOrigin::None,
            screen_generation: 0,
            claimed_targets: vec![Target::Chunk {
                key: claim.target_chunk,
                part,
                inventory: None,
            }],
            claim: ClaimedAction::Combat(claim),
        }
    }

    #[must_use]
    pub fn player_administration(
        action: crate::player_admin::PlayerAdministrativeAction,
        lease: LivePlayerLease,
    ) -> Self {
        Self {
            command: None,
            id: ActionId {
                actor: ActorId::Player(action.target),
                incarnation: lease.incarnation,
                sequence: action.seq.0,
                family: 7,
            },
            tick: action.tick,
            lease: Some(lease),
            client_origin: ClientOrigin::None,
            screen_generation: 0,
            claimed_targets: action
                .affected
                .iter()
                .copied()
                .map(|key| Target::Chunk {
                    key,
                    part: ChunkPart::Player(action.target),
                    inventory: None,
                })
                .collect(),
            claim: ClaimedAction::PlayerAdministration(action),
        }
    }

    #[must_use]
    pub fn block(
        intent: crate::block_action::BlockActionIntent,
        lease: Option<LivePlayerLease>,
        effective_frame: crate::controller::ControllerFrame,
        eye_height: f64,
    ) -> Self {
        let sequence = intent.input_sequence.map_or(0, |sequence| sequence.0);
        Self {
            command: None,
            id: ActionId {
                actor: ActorId::Player(intent.actor),
                incarnation: lease.map_or(0, |lease| lease.incarnation),
                sequence,
                family: 0,
            },
            tick: intent.tick,
            lease,
            client_origin: intent
                .client_sequence
                .map_or(ClientOrigin::None, ClientOrigin::JavaBlockSequence),
            screen_generation: 0,
            claimed_targets: vec![Target::Chunk {
                key: intent.target_chunk,
                part: ChunkPart::Block(intent.position),
                inventory: None,
            }],
            claim: ClaimedAction::Block(crate::block_action::BlockUseClaim {
                intent,
                effective_frame,
                eye_height,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActionInput {
    Proposal(ActionProposal),
    Complete(Action),
}

impl ActionInput {
    #[must_use]
    pub const fn id(&self) -> ActionId {
        match self {
            Self::Proposal(proposal) => proposal.id,
            Self::Complete(action) => action.id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TruthView {
    Local,
    Ground,
}

#[derive(Debug, Clone, Default)]
pub struct OwnedStakeholderSet {
    pub local: BTreeMap<Target, Cell>,
    pub ground: BTreeMap<Target, Cell>,
}

pub trait ActionState {
    fn world_tool_companion(&self, _location: crate::world_command::BlockLocation, _current: u16) -> Result<Option<crate::world_command::ToolCompanion>, ApplyFailure> { Err(ApplyFailure::InvalidAction) }

    fn world_tool_light_plans(&self, _location: crate::world_command::BlockLocation, _operation: crate::world_command::ToolOperation, _current: u16) -> Result<Vec<crate::world_action::WorldLightPlan>, ApplyFailure> { Err(ApplyFailure::InvalidAction) }

    fn world_tool_attributes(&self) -> Result<crate::world_command::ToolAttributes, ApplyFailure> { Err(ApplyFailure::InvalidAction) }
    fn world_tool_transform(&self, _operation: crate::world_command::ToolOperation, _current: u16, _item: u16) -> Result<Option<u16>, ApplyFailure> { Err(ApplyFailure::InvalidAction) }
    fn world_tool_eye_height(&self, _living: &crate::item_action::ActorLivingState) -> Result<f64, ApplyFailure> { Err(ApplyFailure::InvalidAction) }

    fn wake_rest_stat(&self) -> Result<(u16, u32), ApplyFailure> { Err(ApplyFailure::InvalidAction) }

    fn validate_wake_beds(&self, _world: crate::protocol::WorldId, _sleeping: crate::item_action::PlayerSleepingState,
        beds: &[crate::sleep_action::BedRelease]) -> Result<(), ApplyFailure> {
        if beds.is_empty() { Ok(()) } else { Err(ApplyFailure::InvalidAction) }
    }

    fn validate_morning(&self, _world: crate::protocol::WorldId, _morning: crate::world_time::MorningTransition, _source: ActionRef) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }

    fn actor_occupancy_cut(&self, target: Target, _value: &Value) -> Result<(crate::realm_completion::PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        Err(ApplyFailure::MissingStakeholder(target.stakeholder()))
    }
    fn actor_collision_shape(&self, target: Target, _value: &Value) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
        Err(ApplyFailure::Type(target))
    }
    fn validate_world_border(&self, _witness: crate::world_time::WorldBorderWitness, _source: ActionRef) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }

    fn combat_parent_damaged(&self, _parent: ActionRef, _victim: &crate::item_contact::ContactIdentity) -> Option<bool> {
        None
    }
    fn combat_parent_charge(&self, _parent: ActionRef) -> Option<u64> {
        None
    }
    fn entity_equipment_compatible(
        &self,
        _kind: u16,
        _slots: &[(
            crate::canonical_inventory::InventoryEquipmentSlot,
            InventoryCell,
        )],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn entity_inventory_storage_capacity(
        &self,
        _kind: u16,
        _facets: &[(crate::mob_species_property::MobSpeciesProperty, crate::mob_species_property::MobSpeciesPropertyValue)],
        _storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }

    fn entity_inventory_layout(
        &self,
        _body: &crate::entity_body::EntityBody,
    ) -> Result<Vec<crate::inventory::InvLoc>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn entity_inventory_limit(
        &self,
        _body: &crate::entity_body::EntityBody,
        _item: &InventoryCell,
        _target: Target,
    ) -> Result<u8, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn fall_damage_factor(&self, _state: u16, _sneaking: bool) -> Result<u16, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn environmental_contact(&self, _intent: &crate::combat_action::EnvironmentalDamageIntent, _values: &BTreeMap<Target,Cell>)
        -> Result<crate::combat_action::EnvironmentalContactFacts,ApplyFailure> { Err(ApplyFailure::InvalidAction) }
    fn environmental_hazard(
        &self,
        _kind: crate::combat_action::EnvironmentalHazardKind,
        _block_state: u16,
    ) -> Result<Option<crate::combat_action::EnvironmentalHazardFacts>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn status_effect_facts(
        &self,
        _effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<crate::combat_action::CombatStatusFacts, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn status_effect_modifiers(
        &self,
        _effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<Vec<crate::combat_action::CombatAttributeModifier>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn validate_block_inventory_layout(
        &self,
        _block: Target,
        _state: u16,
        _block_entity: &Cell,
        _bindings: &[crate::canonical_inventory::InventorySlotBinding],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        Vec::new()
    }
    fn derive_item_use(
        &self,
        _intent: &crate::item_use::ItemUseIntent,
        _id: ActionId,
        _tick: TickStamp,
        _values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn mob_breeding_advancements(
        &self,
        _kind: u16,
        _credit: &crate::mob_breeding::MobBreedingCredit,
        _state: &crate::player_advancement::PlayerAdvancementState,
    ) -> Result<crate::player_advancement::PlayerAdvancementState, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn crossbow_items(
        &self,
        _intent: &crate::crossbow_items::CrossbowItems,
        _id: ActionId,
        _tick: TickStamp,
        _values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<std::collections::BTreeMap<Target, InventoryCell>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn vehicle_activator(&self, _state: u16) -> Result<Option<bool>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn vehicle_drop_item(&self, _body: &crate::entity_body::EntityBody)
        -> Result<InventoryCell, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn issued_item_birth(&self, _identity: &crate::entity_origin::EntityOriginIdentity,
        _cause: crate::canonical_action::ActionRef, _owner: crate::realm_completion::PeerGeneration,
        spatial: crate::entity_body::EntitySpatial, _cell: &crate::inventory::InventoryCell, _pickup_delay: u16)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        let position = spatial.position.map(f64::from_bits);
        if position.iter().any(|value| !value.is_finite() || value.abs() > 30_000_000.0) { return Err(ApplyFailure::InvalidAction); }
        Err(ApplyFailure::MissingStakeholder(crate::canonical_action::Stakeholder::Chunk(crate::protocol::ChunkKey::new(
            spatial.world, (position[0] / 16.0).floor() as i32, (position[2] / 16.0).floor() as i32))))
    }
    fn dropped_item_body(
        &self,
        _template: &crate::entity_body::EntityBody,
        _source: &InventoryCell,
    ) -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn item_cooldown_group(&self, _source: &InventoryCell) -> Result<String, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn item_transform(
        &self,
        _source: &InventoryCell,
        _operation: &crate::canonical_inventory::ItemTransformOperation,
        _id: ActionId,
        _tick: TickStamp,
    ) -> Result<crate::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn menu_inventory_transform(&self, _kind: crate::canonical_menu_transform::MenuInventoryTransformKind,
        _selection: &crate::canonical_menu::MenuSelection, _inputs: &[InventoryCell])
        -> Result<crate::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn derive_block_inventory_tick(&self, _intent: &crate::canonical_block_inventory_tick::BlockInventoryTickIntent,
        _values: &BTreeMap<Target, Cell>) -> Result<crate::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn machine_genesis(&self, _chunk: ChunkKey) -> Result<[u8; 32], ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn machine_tick_footprint(&self, _identity: crate::action_identity::MachineActionId,
        _values: &BTreeMap<Target, Cell>) -> Result<crate::action_command::ActionFootprint, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn derive_machine_tick(&self, _identity: crate::action_identity::MachineActionId,
        _values: &BTreeMap<Target, Cell>) -> Result<Vec<AppliedWrite>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn mending_repair(
        &self,
        _source: &InventoryCell,
        _experience: u32,
    ) -> Result<(crate::canonical_inventory::ItemTransformOutput, u32), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn derive_cloud_potion(&self, _intent: &crate::area_cloud::AreaCloudIntent, _victim: &crate::item_contact::ContactObservation,
        _id: ActionId, _tick: TickStamp, _values: &BTreeMap<Target, Cell>) -> Result<crate::area_cloud::CloudPotionOutcome, ApplyFailure> { Err(ApplyFailure::InvalidAction) }
    fn derive_projectile_tick(
&self, _intent: &crate::projectile_tick::ProjectileTickIntent,
        _id: ActionId, _tick: TickStamp, _values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn death_inventory_rule(&self, action: ActionRef, witness: crate::world_rules::WorldRulesWitness, keep_inventory: bool, target: Target)
        -> Result<(), ApplyFailure> {
        let rules = self.world_rules(witness, action.tick)?;
        let keep = match self.read(TruthView::Local, target).map(|cell| cell.value) {
            Some(Value::Living(living)) if matches!(action.id.actor, ActorId::Player(_))
                && living.belongs_to(action.id.actor) && living.avatar.is_some() => rules.keep_inventory,
            Some(Value::Entity(body)) if action.id.actor == ActorId::Entity(body.id) => match body.payload {
                crate::entity_body::EntityPayload::Living(living) if living.stand.is_some() => !rules.do_entity_drops,
                crate::entity_body::EntityPayload::Living(living) if living.mob.is_some() => !rules.do_mob_loot,
                _ => return Err(ApplyFailure::InvalidAction),
            },
            _ => return Err(ApplyFailure::InvalidAction),
        };
        if keep == keep_inventory { Ok(()) } else { Err(ApplyFailure::InvalidAction) }
    }
    fn recipe_known(&self, _id: &str) -> bool { false }
    fn action_source_slot(&self, _reference: ActionRef) -> Option<u64> { None }
    fn player_advancement_outcome(&self, _event: &crate::player_advancement::AdvancementEvent,
        _state: &crate::player_advancement::PlayerAdvancementState, _achieved_millis: u64)
        -> Result<crate::player_advancement::AdvancementOutcome, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }

    fn entity_placement_matches(&self, _intent: &crate::entity_placement::EntityPlacementIntent) -> bool { false }
    fn falling_damage_kind(&self, _state: u16) -> Option<crate::combat_action::DamageKind> { None }
    fn primed_tnt_block(&self, _state: u16) -> bool {
        false
    }
    fn primed_tnt_identity(&self) -> Option<(u16, u16)> {
        None
    }
    fn world_rules(&self, _witness: crate::world_rules::WorldRulesWitness, _tick: TickStamp)
        -> Result<crate::world_rules::WorldRulesSnapshot, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn death_inventory_drop(&self, template: &crate::entity_body::EntityBody, source: &InventoryCell)
        -> Result<Option<crate::entity_body::EntityBody>, ApplyFailure> {
        self.dropped_item_body(template, source).map(Some)
    }
    fn resolve_world_respawn(
        &self,
        _plan: crate::world_respawn::WorldRespawnPlan,
        _values: &BTreeMap<Target, Cell>,
    ) -> Result<Option<crate::world_respawn::WorldRespawnResolution>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }

    fn scheduled_executor(
        &self,
        chunk: ChunkKey,
    ) -> Result<crate::realm_completion::PeerGeneration, ApplyFailure> {
        Err(ApplyFailure::MissingStakeholder(Stakeholder::Chunk(chunk)))
    }

    fn derive_world_light(
        &self,
        _plan: crate::world_action::WorldLightPlan,
        _values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn block_segment_obstructs(
        &self,
        _state: u16,
        _position: BlockPos,
        _start: [u64; 3],
        _end: [u64; 3],
    ) -> Result<bool, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn read(&self, view: TruthView, target: Target) -> Option<Cell>;
    fn borrow(&self, view: TruthView, target: Target) -> Option<std::borrow::Cow<'_, Cell>> {
        self.read(view, target).map(std::borrow::Cow::Owned)
    }
    fn issuer_entity_admission_authority(&self, _reference: ActionRef,
        witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::MissingStakeholder(witness.admission.target().stakeholder()))
    }
    fn issuer_entity_initialization_matches(&self,
        witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::MissingStakeholder(witness.admission.target().stakeholder()))
    }
    fn validate_live_player_lease(&self, _lease: LivePlayerLease) -> bool {
        true
    }
    fn available(&self, _view: TruthView, _target: Target) -> bool {
        true
    }
    fn inventory_limit(
        &self,
        _source: &InventoryCell,
        _destination: Target,
    ) -> Result<u8, ApplyFailure> {
        Ok(64)
    }
    fn inventory_equipment_facts(
        &self,
        _cells: &[(
            crate::canonical_inventory::InventoryEquipmentSlot,
            InventoryCell,
        )],
    ) -> Result<crate::combat_action::CombatEquipmentFacts, ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn commit(
        &mut self,
        context: ActionContext,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionContext {
    pub id: ActionId,
    pub tick: TickStamp,
    pub actor_chunk: Option<ChunkKey>,
}

impl ActionState for OwnedStakeholderSet {
    fn borrow(&self, view: TruthView, target: Target) -> Option<std::borrow::Cow<'_, Cell>> {
        match view {
            TruthView::Local => self.local.get(&target),
            TruthView::Ground => self.ground.get(&target),
        }
        .map(std::borrow::Cow::Borrowed)
    }
    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        match view {
            TruthView::Local => self.local.get(&target).cloned(),
            TruthView::Ground => self.ground.get(&target).cloned(),
        }
    }

    fn commit(
        &mut self,
        _context: ActionContext,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        let state = match view {
            TruthView::Local => &mut self.local,
            TruthView::Ground => &mut self.ground,
        };
        let mut occupancy = BTreeMap::new();
        for write in writes.iter().filter(|write| matches!(write.target, Target::Chunk { part: ChunkPart::ActorOccupancy, inventory: None, .. })) {
            let current = state.get(&write.target).ok_or(ApplyFailure::Condition(write.target))?;
            if Some(current) != write.before.as_ref() { return Err(ApplyFailure::Condition(write.target)); }
            let Value::ActorOccupancy(prior) = &current.value else { return Err(ApplyFailure::Type(write.target)); };
            let Some(Cell { revision, value: Value::ActorOccupancy(next) }) = &write.after else { return Err(ApplyFailure::Type(write.target)); };
            let root = next.resolve_against(prior).ok_or(ApplyFailure::Condition(write.target))?;
            let Some(Cell { value: Value::ActorOccupancy(inverse), .. }) = &write.before else { return Err(ApplyFailure::Type(write.target)); };
            if inverse.resolve_against(&root).as_ref() != Some(prior) { return Err(ApplyFailure::Condition(write.target)); }
            occupancy.insert(write.target, Cell { revision: *revision, value: Value::ActorOccupancy(root) });
        }
        for write in writes {
            match &write.after {
                Some(cell) => {
                    state.insert(write.target, occupancy.remove(&write.target).unwrap_or_else(|| cell.clone()));
                }
                None => {
                    state.remove(&write.target);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedWrite {
    pub target: Target,
    pub before: Option<Cell>,
    pub after: Option<Cell>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedAction {
    pub id: ActionId,
    pub tick: TickStamp,
    pub actor_chunk: Option<ChunkKey>,
    pub view: TruthView,
    pub writes: Vec<AppliedWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyFailure {
    ResourceLimit,
    InvalidAction,
    Capacity,
    MissingStakeholder(Stakeholder),
    Condition(Target),
    Type(Target),
    Inventory(Target),
    Overflow(Target),
    InventoryConservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantFailure {
    ChangedSinceApply(Target),
    Commit,
}

impl Action {
    pub fn logical_record_count(&self) -> Result<usize, ApplyFailure> {
        crate::logical_records::count(self)
    }

    pub fn validate_resource_budget(&self) -> Result<(), ApplyFailure> {
        let records = self.logical_record_count()?;
        if records > MAX_LOGICAL_RECORDS { return Err(ApplyFailure::ResourceLimit); }
        let bytes = postcard::experimental::serialized_size(self).map_err(|_| ApplyFailure::InvalidAction)?;
        check_logical_budget(records, bytes)
    }

    pub fn death_inventory_intent(&self) -> Option<&crate::death_inventory::DeathInventoryIntent> {
        match &self.semantic_intent {
            Some(SemanticIntent::DeathInventory(intent)) => Some(intent),
            Some(SemanticIntent::Respawn(intent)) => intent.death_inventory.as_ref(),
            _ => None,
        }
    }

    pub fn menu_session_intent(&self) -> Option<&crate::canonical_menu::MenuSessionIntent> {
        match &self.semantic_intent {
            Some(SemanticIntent::MenuSessionChange(intent)) => Some(intent),
            Some(SemanticIntent::World(intent)) => {
                let mut menus = intent.steps.iter().filter_map(|step| match step {
                    crate::world_action::WorldReplayStep::MenuSession(menu) => Some(menu), _ => None,
                });
                let menu = menus.next()?;
                menus.next().is_none().then_some(menu)
            }
            _ => None,
        }
    }

    pub fn world_border_witnesses(&self) -> Vec<crate::world_time::WorldBorderWitness> {
        match &self.semantic_intent {
            Some(SemanticIntent::Respawn(intent)) => intent.preferred.iter().chain(std::iter::once(&intent.fallback)).map(|plan| plan.border).collect(),
            _ => Vec::new(),
        }
    }

    pub fn morning_witnesses(&self) -> Vec<(crate::protocol::WorldId, crate::world_time::MorningTransition)> {
        match &self.semantic_intent {
            Some(SemanticIntent::Wake(intent)) => intent.morning.map(|morning| (intent.chunk.world, morning)).into_iter().collect(),
            _ => Vec::new(),
        }
    }

    pub fn source_slot_witnesses(&self) -> Vec<u64> {
        let block_tick_slots = match &self.semantic_intent {
            Some(SemanticIntent::World(world)) => world.steps.iter().filter_map(|step| match step {
                crate::world_action::WorldReplayStep::BlockInventoryTick(intent) => Some(intent.source_slot),
                _ => None,
            }).collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        self.world_rule_witnesses().into_iter().map(|witness| witness.slot)
            .chain(self.world_border_witnesses().into_iter().map(|witness| witness.slot))
            .chain(crate::issuer_entity_admission::witness(self).map(|witness| witness.source_slot)).chain(block_tick_slots).collect()
    }

    pub fn world_rule_witnesses(&self) -> Vec<crate::world_rules::WorldRulesWitness> {
        let mut witnesses = Vec::new();
        if let Some(intent) = self.death_inventory_intent() {
            witnesses.push(intent.rules);
        }
        if let Some(SemanticIntent::Combat(intent)) = &self.semantic_intent {
            witnesses.extend(intent.environment.iter().filter_map(|environment| environment.world_rules));
        }
        if let Some(SemanticIntent::ProjectileTick(intent)) = &self.semantic_intent {
            witnesses.push(intent.rules);
        }
        if let Some(SemanticIntent::AreaCloud(intent)) = &self.semantic_intent { witnesses.push(intent.rules); }
        if let Some(SemanticIntent::World(intent)) = &self.semantic_intent {
            witnesses.extend(intent.steps.iter().filter_map(|step| match step {
                crate::world_action::WorldReplayStep::WorldFluidRules { witness, .. } => Some(*witness),
                _ => None,
            }));
        }
        for condition in &self.conditions {
            match &condition.expected {
                Expected::IssuerEntityAdmission(witness) => {
                    if let crate::issuer_entity_admission::IssuerAdmissionAuthority::Primary { rules } = &witness.authority { witnesses.push(*rules); }
                }
                Expected::EntityPlacement(intent) => witnesses.push(intent.rules),
                Expected::VehicleTerminal(intent) => witnesses.push(intent.rules),
                Expected::ObjectTerminal(intent) => witnesses.push(intent.rules),
                _ => {}
            }
        }
        witnesses
    }

    pub fn captured_entity_source(
        &self,
    ) -> Option<(
        Target,
        &crate::entity_body::EntityBirth,
        crate::realm_completion::PeerGeneration,
    )> {
        if !matches!(self.id.actor, ActorId::Entity(_)) {
            return None;
        }
        if let Some(source) =
            self.conditions
                .iter()
                .find_map(|condition| match &condition.expected {
                    Expected::ActorControlled { birth, issuer } => {
                        Some((condition.target, birth, *issuer))
                    }
                    _ => None,
                })
        {
            return Some(source);
        }
        if let Some(source) = self.effects.iter().find_map(|effect| match effect {
            Effect::UpdateVehicleProperty {
                target, authority, ..
            } => crate::vehicle_property::body_target(*target)
                .map(|(body, _)| (body, &authority.birth, authority.body_owner)),
            Effect::UpdateMobContinuation {
                target, authority, ..
            } => crate::living_property::body_target(*target)
                .map(|(body, _)| (body, &authority.birth, authority.body_owner)),
            Effect::MoveEntityBody {
                source,
                authority: Some(authority),
                ..
            } => Some((*source, &authority.birth, authority.body_owner)),
            Effect::ChangeDroppedItem {
                target,
                birth,
                body_owner,
                ..
            } => Some((*target, birth, *body_owner)),
            Effect::AdvanceExperienceLifetime {
                target,
                birth,
                owner,
            } => Some((*target, birth, *owner)),
            _ => None,
        }) {
            return Some(source);
        }
        match (
            &self.class,
            self.conditions.as_slice(),
            self.effects.as_slice(),
        ) {
            (
                ActionClass::Infallible,
                [],
                [
                    Effect::ChangeDroppedItem {
                        target,
                        birth,
                        body_owner,
                        change: crate::entity_body::DroppedItemChange::AdvanceLifetime,
                        ..
                    },
                ],
            ) => Some((*target, birth, *body_owner)),
            _ => None,
        }
    }

    #[must_use]
    pub fn targets(&self) -> BTreeSet<Target> {
        self.conditions
            .iter()
            .map(|condition| condition.target)
            .chain(self.effects.iter().flat_map(Effect::targets))
            .collect()
    }

    #[must_use]
    pub fn from_break(
        update: &crate::protocol::BreakBlockUpdate,
        incarnation: u64,
        resulting_state: u16,
        client_origin: ClientOrigin,
    ) -> Self {
        let target = Target::Chunk {
            key: update.chunk,
            part: ChunkPart::Block(update.pos),
            inventory: None,
        };
        let mut stakeholders = vec![
            Stakeholder::Chunk(update.actor_chunk),
            Stakeholder::Chunk(update.chunk),
        ];
        stakeholders.sort_unstable();
        stakeholders.dedup();
        Self {
            id: ActionId {
                actor: ActorId::Player(update.gid),
                incarnation,
                sequence: update.seq.0,
                family: 0,
            },
            edit_authority: None,
            tick: update.tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(update.actor_chunk),
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: Vec::new(),
            conditions: vec![Condition {
                target,
                expected: Expected::BlockState(update.expected_old_state),
            }],
            effects: vec![Effect::Put {
                target,
                value: Value::Block(resulting_state),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Block,
            client_origin,
            presentation: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_player_administration(
        lease: LivePlayerLease,
        sequence: PlayerSeq,
        tick: TickStamp,
        old_chunk: ChunkKey,
        new_chunk: ChunkKey,
        before: Cell,
        mut after: ActorLivingState,
    ) -> Option<Self> {
        let actor = lease.gid;
        let incarnation = lease.incarnation;
        let Value::Living(old) = &before.value else {
            return None;
        };
        let belongs = |state: &ActorLivingState, chunk: ChunkKey| {
            let Some(avatar) = state.avatar.as_ref() else {
                return false;
            };
            let position = avatar.position.map(f64::from_bits);
            state.belongs_to(ActorId::Player(actor))
                && avatar.world == chunk.world
                && (position[0].floor() as i32).div_euclid(16) == chunk.x()
                && (position[2].floor() as i32).div_euclid(16) == chunk.z()
        };
        if incarnation == 0 || !belongs(old, old_chunk) || !belongs(&after, new_chunk) {
            return None;
        }
        let accepted_avatar = after.avatar.as_mut()?;
        accepted_avatar.head_yaw = if accepted_avatar.yaw == old.avatar.as_ref()?.yaw {
            old.avatar.as_ref()?.head_yaw
        } else {
            accepted_avatar.yaw
        };
        accepted_avatar.movement_tick = tick;
        accepted_avatar.movement_sequence = sequence;
        let old_target = Target::Chunk {
            key: old_chunk,
            part: ChunkPart::Player(actor),
            inventory: None,
        };
        let new_target = Target::Chunk {
            key: new_chunk,
            part: ChunkPart::Player(actor),
            inventory: None,
        };
        let mut stakeholders = vec![Stakeholder::Chunk(old_chunk), Stakeholder::Chunk(new_chunk)];
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let crossing_chunk = old_target != new_target;
        let old_avatar = old.avatar.as_ref()?;
        let mut new_avatar = after.avatar.as_ref()?.clone();
        let avatar_only = old.food == after.food
            && old.combat == after.combat
            && old.use_state == after.use_state
            && old.cooldowns == after.cooldowns
            && old_avatar.game_mode == new_avatar.game_mode
            && old.effects == after.effects
            && old.hidden_effects == after.hidden_effects
            && old_avatar.fall_distance == new_avatar.fall_distance
            && old_avatar.on_fire == new_avatar.on_fire;
        if crossing_chunk && !avatar_only {
            return None;
        }
        let move_avatar = avatar_only && *old_avatar != new_avatar;
        let reset_living = !avatar_only
            && old_avatar.world == new_avatar.world
            && old_avatar.position == new_avatar.position
            && old_avatar.yaw == new_avatar.yaw
            && old_avatar.pitch == new_avatar.pitch;
        if !move_avatar && !reset_living {
            return None;
        }
        if move_avatar {
            new_avatar.spatial_epoch = SpatialEpoch::Action(ActionRef {
                tick,
                id: ActionId {
                    actor: ActorId::Player(actor),
                    incarnation,
                    sequence: sequence.0,
                    family: 7,
                },
            });
        }
        let conditions = Vec::new();
        let effects = if move_avatar {
            vec![Effect::MovePlayerAvatar {
                source: old_target,
                destination: new_target,
                avatar: new_avatar.clone(),
                velocity_micro: after.combat.velocity_micro,
            }]
        } else {
            vec![Effect::ResetPlayerLiving {
                target: old_target,
                template: after,
            }]
        };
        let action = Self {
            id: ActionId {
                actor: ActorId::Player(actor),
                incarnation,
                sequence: sequence.0,
                family: 7,
            },
            edit_authority: None,
            tick,
            class: ActionClass::Infallible,
            chunk_transition: crossing_chunk,
            actor_chunk: Some(old_chunk),
            player_leases: vec![lease],
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: Vec::new(),
            conditions,
            effects,
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate().ok()?;
        Some(action)
    }

    pub fn from_player_respawn(
        id: ActionId,
        lease: LivePlayerLease,
        tick: TickStamp,
        source_chunk: ChunkKey,
        destination_chunk: ChunkKey,
        original_dead: &ActorLivingState,
        mut avatar: crate::item_action::PlayerAvatarState,
        max_health_attribute: u16,
    ) -> Option<Self> {
        if id.actor != ActorId::Player(lease.gid)
            || id.incarnation != lease.incarnation
            || !original_dead.belongs_to(id.actor)
            || original_dead.combat.health_milli != 0
            || original_dead.combat.timing.death_cause.is_none()
            || !original_dead.combat.timing.death_inventory_processed
            || original_dead.avatar.as_ref().is_none_or(|avatar| avatar.mount.is_some())
            || original_dead.menu.session != 0
            || !original_dead.menu.valid()
        {
            return None;
        }
        avatar.spatial_epoch = SpatialEpoch::Action(ActionRef { tick, id });
        avatar.movement_tick = tick;
        let source = Target::Chunk {
            key: source_chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let destination = Target::Chunk {
            key: destination_chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let mut stakeholders = vec![
            Stakeholder::Chunk(source_chunk),
            Stakeholder::Chunk(destination_chunk),
        ];
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let mut conditions: Vec<Condition> = [
            crate::living_property::LivingProperty::Health,
            crate::living_property::LivingProperty::DeathCause,
            crate::living_property::LivingProperty::DeathInventoryProcessed,
            crate::living_property::LivingProperty::MenuSession,
        ]
        .into_iter()
        .map(|property| Condition {
            target: Target::Chunk {
                key: source_chunk,
                part: ChunkPart::LivingProperty {
                    actor: id.actor,
                    property,
                },
                inventory: None,
            },
            expected: Expected::Exact(Cell {
                revision: original_dead
                    .property_versions
                    .get(&property)
                    .copied()
                    .unwrap_or(0),
                value: Value::LivingProperty(property.read(original_dead)),
            }),
        })
        .collect();
        conditions.push(Condition {
            target: source,
            expected: Expected::SpatialEpochEquals(original_dead.avatar.as_ref()?.spatial_epoch),
        });
        conditions.push(Condition {
            target: Target::Chunk { key: source_chunk, part: ChunkPart::ActorRelation {
                actor: id.actor, relation: crate::actor_relation::ActorRelation::Mount,
            }, inventory: None },
            expected: Expected::Exact(Cell {
                revision: original_dead.avatar.as_ref()?.relation_versions.get(&crate::actor_relation::ActorRelation::Mount).copied().unwrap_or(0),
                value: Value::ActorRelation(None),
            }),
        });
        let action = Self {
            id,
            tick,
            class: ActionClass::Undoable,
            chunk_transition: source_chunk != destination_chunk,
            edit_authority: None,
            actor_chunk: Some(source_chunk),
            player_leases: vec![lease],
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: vec![original_dead.combat.timing.death_cause?],
            conditions,
            effects: vec![Effect::RespawnPlayerAvatar {
                source,
                destination,
                avatar,
                max_health_attribute,
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate().ok()?;
        Some(action)
    }

    pub fn from_player_stance(
        id: ActionId,
        lease: LivePlayerLease,
        tick: TickStamp,
        chunk: ChunkKey,
        stance: crate::item_action::PlayerStance,
        fall_distance: f32,
        epoch: SpatialEpoch,
    ) -> Option<Self> {
        if id.actor != ActorId::Player(lease.gid)
            || id.incarnation != lease.incarnation
            || !fall_distance.is_finite()
            || fall_distance < 0.0
        {
            return None;
        }
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::LivingProperty {
                actor: id.actor,
                property: crate::living_property::LivingProperty::PlayerStance,
            },
            inventory: None,
        };
        let dependencies = match epoch {
            SpatialEpoch::Action(reference) => vec![reference],
            _ => Vec::new(),
        };
        let action = Self {
            id,
            tick,
            class: ActionClass::Infallible,
            chunk_transition: false,
            edit_authority: None,
            actor_chunk: Some(chunk),
            player_leases: vec![lease],
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            dependencies,
            conditions: Vec::new(),
            effects: vec![
                Effect::Put {
                    target,
                    value: Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::PlayerStance(stance),
                    ),
                },
                Effect::Put {
                    target: Target::Chunk {
                        key: chunk,
                        part: ChunkPart::LivingProperty {
                            actor: id.actor,
                            property: crate::living_property::LivingProperty::FallDistance,
                        },
                        inventory: None,
                    },
                    value: Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::Unsigned(
                            fall_distance.to_bits(),
                        ),
                    ),
                },
            ],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate().ok()?;
        Some(action)
    }

    pub fn from_player_presentation(
        id: ActionId,
        lease: LivePlayerLease,
        tick: TickStamp,
        chunk: ChunkKey,
        before: Cell,
        change: crate::item_action::PlayerPresentationChange,
        mut conditions: Vec<Condition>,
        mut effects: Vec<Effect>,
    ) -> Result<Self, ApplyFailure> {
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let Value::Living(living) = &before.value else {
            return Err(ApplyFailure::Type(target));
        };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
        if id.actor != ActorId::Player(lease.gid)
            || id.incarnation != lease.incarnation
            || !living.belongs_to(id.actor)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut leases = vec![lease];
        conditions.push(Condition {
            target,
            expected: Expected::SpatialEpochEquals(avatar.spatial_epoch),
        });
        let effect = match change {
            crate::item_action::PlayerPresentationChange::Camera(camera) => {
                if let Some(observation) = &camera {
                    if !observation.valid() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    conditions.push(Condition {
                        target: observation.target(),
                        expected: Expected::SpatialEpochEquals(observation.epoch),
                    });
                    match &observation.identity {
                        crate::item_contact::ContactIdentity::Player(camera_lease) => {
                            leases.push(*camera_lease)
                        }
                        crate::item_contact::ContactIdentity::Entity { birth, .. } => conditions
                            .push(Condition {
                                target: observation.target(),
                                expected: Expected::EntityBirthOwned {
                                    birth: birth.clone(),
                                    owner: observation.owner,
                                },
                            }),
                    }
                }
                Effect::SetPlayerCamera { target, camera }
            }
            crate::item_action::PlayerPresentationChange::Sleeping(sleeping) => {
                conditions.push(Condition {
                    target,
                    expected: Expected::Exact(before),
                });
                Effect::SetPlayerSleeping { target, sleeping }
            }
            crate::item_action::PlayerPresentationChange::Respawn(respawn) => {
                Effect::SetPlayerRespawn { target, respawn }
            }
        };
        effects.push(effect);
        leases.sort_unstable();
        leases.dedup();
        let mut stakeholders = conditions
            .iter()
            .map(|condition| condition.target.stakeholder())
            .chain(
                effects
                    .iter()
                    .flat_map(Effect::targets)
                    .map(Target::stakeholder),
            )
            .collect::<Vec<_>>();
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let action = Self {
            id,
            tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(chunk),
            player_leases: leases,
            edit_authority: None,
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: Vec::new(),
            conditions,
            effects,
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate()?;
        Ok(action)
    }

    pub fn from_player_statistics(
        id: ActionId,
        lease: LivePlayerLease,
        tick: TickStamp,
        chunk: ChunkKey,
        epoch: SpatialEpoch,
        increments: &[(u16, u32, u32)],
    ) -> Result<Self, ApplyFailure> {
        if id.actor != ActorId::Player(lease.gid)
            || id.incarnation != lease.incarnation
            || increments.is_empty()
            || increments.len() > 128
            || increments.iter().any(|(category, key, amount)| {
                *category > 8 || *key > i32::MAX as u32 || *amount == 0
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let effects = increments
            .iter()
            .map(|(category, key, amount)| Effect::Add {
                target: Target::Chunk {
                    key: chunk,
                    part: ChunkPart::LivingProperty {
                        actor: id.actor,
                        property: crate::living_property::LivingProperty::Statistic {
                            category: *category,
                            key: *key,
                        },
                    },
                    inventory: None,
                },
                amount: i64::from(*amount),
            })
            .collect();
        let action = Self {
            id,
            tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(chunk),
            player_leases: vec![lease],
            edit_authority: None,
            player_chunks: Vec::new(),
            stakeholders: vec![target.stakeholder()],
            dependencies: match epoch {
                SpatialEpoch::Action(reference) => vec![reference],
                _ => Vec::new(),
            },
            conditions: vec![Condition {
                target,
                expected: Expected::SpatialEpochEquals(epoch),
            }],
            effects,
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            client_origin: ClientOrigin::None,
            presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }

    #[must_use]
    pub fn from_player_chunk_transition(
        update: &crate::protocol::PlayerChunkTransition,
        lease: LivePlayerLease,
        before: Cell,
    ) -> Option<Self> {
        let Value::Living(previous) = &before.value else {
            return None;
        };
        let movement = update.snapshot.movement;
        let position = movement.pos;
        if lease.gid != update.gid
            || lease.incarnation == 0
            || lease.incarnation != update.incarnation
            || movement.gid != update.gid
            || movement.incarnation != update.incarnation
            || movement.tick != update.tick
            || movement.seq != update.seq
            || movement.world != update.to.world
            || update.from == update.to
            || !position.iter().all(|coordinate| coordinate.is_finite())
            || !movement.vel.iter().all(|coordinate| coordinate.is_finite())
            || (position[0].floor() as i32).div_euclid(16) != update.to.x()
            || (position[2].floor() as i32).div_euclid(16) != update.to.z()
        {
            return None;
        }
        let prior_avatar = previous.avatar.as_ref()?;
        if !previous.belongs_to(ActorId::Player(update.gid))
            || prior_avatar.world != update.from.world
            || (f64::from_bits(prior_avatar.position[0]).floor() as i32).div_euclid(16)
                != update.from.x()
            || (f64::from_bits(prior_avatar.position[2]).floor() as i32).div_euclid(16)
                != update.from.z()
        {
            return None;
        }
        let mut avatar = prior_avatar.clone();
        avatar.spatial_epoch = SpatialEpoch::Action(ActionRef {
            tick: update.tick,
            id: ActionId {
                actor: ActorId::Player(update.gid),
                incarnation: lease.incarnation,
                sequence: update.seq.0,
                family: 8,
            },
        });
        avatar.world = movement.world;
        avatar.position = position.map(f64::to_bits);
        avatar.yaw = movement.yaw.to_bits();
        avatar.head_yaw = movement.head_yaw.to_bits();
        avatar.pitch = movement.pitch.to_bits();
        avatar.on_ground = movement.on_ground;
        avatar.movement_tick = movement.tick;
        avatar.movement_sequence = movement.seq;
        let mut stakeholders = vec![
            Stakeholder::Chunk(update.from),
            Stakeholder::Chunk(update.to),
        ];
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let action = Self {
            id: ActionId {
                actor: ActorId::Player(update.gid),
                incarnation: lease.incarnation,
                sequence: update.seq.0,
                family: 8,
            },
            tick: update.tick,
            class: ActionClass::Infallible,
            chunk_transition: true,
            actor_chunk: Some(update.from),
            edit_authority: None,
            player_leases: vec![lease],
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: Vec::new(),
            conditions: Vec::new(),
            effects: vec![Effect::MovePlayerAvatar {
                source: Target::Chunk {
                    key: update.from,
                    part: ChunkPart::Player(update.gid),
                    inventory: None,
                },
                destination: Target::Chunk {
                    key: update.to,
                    part: ChunkPart::Player(update.gid),
                    inventory: None,
                },
                avatar,
                velocity_micro: movement
                    .vel
                    .map(|component| (component * 1_000_000.0).round() as i64),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate().ok()?;
        Some(action)
    }

    pub fn validate(&self) -> Result<(), ApplyFailure> {
        self.validate_resource_budget()?;
        let issuer_admission = self.conditions.iter().any(|condition| matches!(condition.expected, Expected::IssuerEntityAdmission(_)));
        if issuer_admission
            && !crate::issuer_entity_admission::witness(self).is_some_and(|witness| witness.guards_valid(self)) {
            return Err(ApplyFailure::InvalidAction);
        }
        let counterfactual_conditions = self.counterfactual_conditions.iter().copied().collect::<BTreeSet<_>>();
        let counterfactual_effects = self.counterfactual_effects.iter().copied().collect::<BTreeSet<_>>();
        let condition_targets = self.conditions.iter().map(|condition| condition.target).collect::<BTreeSet<_>>();
        let emitted_targets = self.effects.iter().flat_map(Effect::targets).collect::<BTreeSet<_>>();
        if let Some(witness) = crate::entity_placement::witness(self) {
            if !witness.guards_valid(self) { return Err(ApplyFailure::InvalidAction); }
        }
        if crate::object_tick::terminal_witness(self).is_some() && !crate::object_tick::valid_terminal_action(self) {
            return Err(ApplyFailure::InvalidAction);
        }
        if crate::vehicle_terminal::witness(self).is_some() && !crate::vehicle_terminal::valid_terminal_action(self) {
            return Err(ApplyFailure::InvalidAction);
        }
        if self
            .edit_authority
            .as_ref()
            .is_some_and(|authority| !authority.valid_for(self))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.presentation.len() > 4096
            || self.presentation.iter().any(|intent| {
                matches!(intent, crate::world_presentation::WorldPresentationIntent::WorldEvent { .. })
                    || !intent.valid()
                    || !self
                        .stakeholders
                        .contains(&Stakeholder::Chunk(intent.chunk()))
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if (self.class == ActionClass::Infallible && !self.conditions.is_empty() && !issuer_admission)
            || (self.class == ActionClass::Replayable && self.semantic_intent.is_none())
            || (self.class == ActionClass::Undoable && self.semantic_intent.is_some())
            || (self.chunk_transition
                && self.class != ActionClass::Infallible
                && !matches!(&self.semantic_intent, Some(SemanticIntent::Respawn(_)))
                && !(self.class == ActionClass::Undoable
                    && matches!(
                        self.effects.as_slice(),
                        [Effect::RespawnPlayerAvatar { .. }]
                    )))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if matches!(self.id.actor, ActorId::Entity(_))
            && self.class != ActionClass::Infallible
            && self
                .conditions
                .iter()
                .filter(|condition| matches!(condition.expected, Expected::ActorControlled { .. }))
                .count()
                != 1
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.effects.iter().any(|effect| {
            matches!(
                effect,
                Effect::Put {
                    value: Value::ActorRelation(_),
                    ..
                }
            )
        }) && !crate::actor_relation::reciprocal(self)
            && !crate::vehicle_tick::permits_activator_detach(self)
            && !(crate::object_tick::valid_terminal_action(self) && crate::actor_relation::owner_detach_all(self))
            && !crate::vehicle_terminal::valid_terminal_action(self)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut lease_keys = BTreeSet::new();
        let mut lease_gids = BTreeSet::new();
        let live_leases = self.player_leases.iter().copied().collect::<BTreeSet<_>>();
        if self.player_leases.iter().any(|lease| {
            lease.incarnation == 0 || !lease_keys.insert(lease.key) || !lease_gids.insert(lease.gid)
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        let player_targets = self
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(self.effects.iter().flat_map(Effect::targets))
            .chain(self.counterfactual_conditions.iter().copied())
            .chain(self.counterfactual_effects.iter().copied())
            .filter_map(|target| match target {
                Target::Player { key, .. } => Some(key),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let live_targets = self
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(self.effects.iter().flat_map(Effect::targets))
            .chain(self.counterfactual_conditions.iter().copied())
            .chain(self.counterfactual_effects.iter().copied())
            .filter_map(|target| match target {
                Target::Player {
                    key,
                    inventory: Some(_),
                    ..
                } => Some(key),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let avatar_targets = self
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(self.effects.iter().flat_map(Effect::targets))
            .chain(self.counterfactual_conditions.iter().copied())
            .chain(self.counterfactual_effects.iter().copied())
            .filter_map(|target| match target {
                Target::Chunk {
                    part: ChunkPart::Player(gid),
                    inventory: None,
                    ..
                }
                | Target::Chunk {
                    part:
                        ChunkPart::LivingProperty {
                            actor: ActorId::Player(gid),
                            ..
                        },
                    inventory: None,
                    ..
                }
                | Target::Chunk {
                    part: ChunkPart::ActorTags { actor: ActorId::Player(gid) },
                    inventory: None,
                    ..
                } => Some(gid),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let live_target_leases_match = self
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(self.effects.iter().flat_map(Effect::targets))
            .chain(self.counterfactual_conditions.iter().copied())
            .chain(self.counterfactual_effects.iter().copied())
            .all(|target| match target {
                Target::Player {
                    key,
                    lease,
                    inventory: Some(_),
                } => lease
                    .is_some_and(|lease| lease.key == key && live_leases.contains(&lease)),
                _ => true,
            });
        let located_players = self
            .player_chunks
            .iter()
            .map(|(player, _)| *player)
            .collect::<BTreeSet<_>>();
        if !live_target_leases_match
            || !live_targets.is_subset(&lease_keys)
            || !avatar_targets.is_subset(&lease_gids)
            || self.player_leases.iter().any(|lease| {
                !player_targets.contains(&lease.key) && !avatar_targets.contains(&lease.gid)
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if located_players != live_targets
            || self
                .player_chunks
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
            || self.player_chunks.iter().any(|(player, chunk)| {
                !self.stakeholders.contains(&Stakeholder::Player(*player))
                    || !self.stakeholders.contains(&Stakeholder::Chunk(*chunk))
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.stakeholders.is_empty()
            || self.id.family >= 0x80
            || self.stakeholders.windows(2).any(|pair| pair[0] >= pair[1])
            || self.undo.first() != Some(&UndoProcedure::RestoreWrittenTargets)
            || self.undo.len() > 257
            || self.undo[1..]
                .iter()
                .enumerate()
                .any(|(index, procedure)| self.undo[1..index + 1].contains(procedure))
            || self.conditions.len() > MAX_LOGICAL_RECORDS
            || self.effects.len() > MAX_LOGICAL_RECORDS
            || self.counterfactual_conditions.len() > MAX_LOGICAL_RECORDS
            || self.counterfactual_effects.len() > MAX_LOGICAL_RECORDS
            || self
                .counterfactual_conditions
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != self.counterfactual_conditions.len()
            || self
                .counterfactual_effects
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != self.counterfactual_effects.len()
            || self.dependencies.len() > MAX_LOGICAL_RECORDS
            || self
                .dependencies
                .iter()
                .any(|dependency| dependency.tick == self.tick && dependency.id == self.id)
            || self.factual_result.as_ref().is_some_and(|result| {
                result.id != self.id
                    || result.tick != self.tick
                    || result.view != TruthView::Local
                    || result.actor_chunk != self.actor_chunk
                    || result.writes.iter().any(|write| {
                        !emitted_targets.contains(&write.target)
                    })
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut touched = BTreeSet::new();
        let mut condition_fingerprints: BTreeMap<(Target, [u8; 32]), Vec<&Expected>> = BTreeMap::new();
        let mut occupancy_poses = BTreeMap::new();
        for condition in &self.conditions {
            if let Expected::ActorOccupancyClear(witness) | Expected::HangingOccupancyClear(witness) | Expected::EntitiesOccupancyClear(witness) | Expected::ActorOccupancyDomain(witness) = &condition.expected {
                if !witness.valid()
                    || !matches!(condition.target, Target::Chunk {
                    key, part: ChunkPart::ActorOccupancy, inventory: None,
                } if key == witness.key)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                for pose in &witness.poses {
                    if occupancy_poses
                        .insert(pose.entry.actor, pose)
                        .is_some_and(|prior| prior != pose)
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
            }
            if let Expected::LivingUnsignedAtLeast(minimum) = condition.expected {
                if minimum == 0
                    || minimum > i32::MAX as u32
                    || !matches!(
                        condition.target,
                        Target::Chunk {
                            part: ChunkPart::LivingProperty {
                                property: crate::living_property::LivingProperty::Health,
                                ..
                            },
                            inventory: None,
                            ..
                        }
                    )
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if matches!(condition.expected, Expected::RevisionEquals(_))
                && !matches!(
                    condition.target,
                    Target::Chunk {
                        part: ChunkPart::BlockSection(_)
                            | ChunkPart::LightSection(_)
                            | ChunkPart::ActorOccupancy,
                        inventory: None,
                        ..
                    }
                )
            {
                return Err(ApplyFailure::InvalidAction);
            }
            if let Expected::ScheduledTickPresence { position, .. } | Expected::ScheduledTickPresenceExcept { position, .. } = condition.expected {
                if !matches!(condition.target, Target::Chunk { key, part: ChunkPart::Whole, inventory: None }
                    if position.x.div_euclid(16) == key.x() && position.z.div_euclid(16) == key.z())
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if let Expected::ScheduledTickPresenceExcept { kind, id, position, except, .. } = condition.expected {
                if except.kind != kind || except.id != id || except.position != position
                    || !self.conditions.contains(&Condition { target: condition.target, expected: Expected::ScheduledTickFact { fact: except, present: true } })
                    || !self.effects.iter().any(|effect| matches!(effect, Effect::CompleteScheduledTick { target, fact } if *target == condition.target && *fact == except))
                { return Err(ApplyFailure::InvalidAction); }
            }
            if let Expected::ScheduledTickFact { fact, .. } = condition.expected {
                if !matches!(condition.target, Target::Chunk { key, part: ChunkPart::Whole, inventory: None }
                    if fact.position.x.div_euclid(16) == key.x() && fact.position.z.div_euclid(16) == key.z())
                    || !(-3..=3).contains(&fact.priority)
                    || fact.executor.generation == 0
                    || matches!(fact.origin, ScheduledTickOrigin::Action { producer, .. } if producer.generation == 0)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if let Expected::EntityBirthOwned { birth, owner }
            | Expected::HopperCollector { birth, owner }
            | Expected::EntityBirthKind { birth, owner, .. }
            | Expected::DroppedItemStack { birth, owner, .. } = &condition.expected
            {
                if owner.generation == 0
                    || birth.source.generation == 0
                    || birth.uuid.is_nil()
                    || !matches!(condition.target, Target::Chunk { part: ChunkPart::Entity(entity), inventory: None, .. } if birth.source.server == entity.origin)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if let Expected::DroppedItemStack {
                item,
                count,
                components,
                epoch,
                ..
            } = &condition.expected
            {
                if *item == 0
                    || !(1..=64).contains(count)
                    || components.is_empty()
                    || components.len() > 65536
                    || !matches!(epoch, SpatialEpoch::Action(reference) if !reference.tick.is_after(self.tick))
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            let encoded = match &condition.expected {
                Expected::Exact(Cell { revision, value: Value::ActorOccupancy(root) }) => {
                    postcard::to_allocvec(&(revision, root.entries.commitment(), root.entries.len()))
                }
                expected => postcard::to_allocvec(expected),
            }.map_err(|_| ApplyFailure::InvalidAction)?;
            let fingerprint: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(&encoded).into();
            let bucket = condition_fingerprints.entry((condition.target, fingerprint)).or_default();
            if bucket.contains(&&condition.expected) {
                return Err(ApplyFailure::InvalidAction);
            }
            bucket.push(&condition.expected);
            if let Expected::ActorControlled { birth, issuer } = &condition.expected {
                if issuer.generation == 0
                    || !matches!(condition.target, Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: None } if self.id.actor == crate::actor::ActorId::Entity(entity) && self.id.incarnation == u64::from(entity.generation) && self.actor_chunk == Some(key) && birth.source.server == entity.origin)
                    || birth.source.generation == 0
                    || birth.uuid.is_nil()
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            touched.insert(condition.target);
        }
        let conditioned = touched.clone();
        for procedure in self.undo.iter().skip(1) {
            let target = match procedure {
                UndoProcedure::RestoreWrittenTargets => return Err(ApplyFailure::InvalidAction),
                UndoProcedure::AdjustInventoryCount { target } => {
                    if !matches!(
                        target,
                        Target::Chunk {
                            inventory: Some(_),
                            ..
                        } | Target::Player {
                            inventory: Some(_),
                            ..
                        }
                    ) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    *target
                }
                UndoProcedure::AdjustScalar { target } => {
                    if !matches!(
                        target,
                        Target::Player {
                            inventory: None,
                            ..
                        }
                    ) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    *target
                }
            };
            if !self
                .effects
                .iter()
                .any(|effect| effect.targets().any(|affected| affected == target))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(
            SemanticIntent::InventoryMove(intent)
            | SemanticIntent::Pickup {
                inventory: intent, ..
            },
        ) = &self.semantic_intent
        {
            if self.class != ActionClass::Replayable
                || self.chunk_transition
                || self.effects.iter().any(|effect| {
                    !matches!(effect, Effect::MoveItems { .. })
                    && !matches!(effect, Effect::Put { target, value: Value::LivingProperty(_) }
                        if intent.equipment.iter().any(|binding| binding.property_targets().contains(target)))
                })
                || intent.equipment.iter().any(|binding| {
                    !binding.is_valid()
                        || binding.targets().any(|target| {
                            !counterfactual_conditions.contains(&target)
                                || !counterfactual_effects.contains(&target)
                        })
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let destinations = intent
                .ordered_destinations
                .iter()
                .map(|candidate| candidate.target)
                .collect::<BTreeSet<_>>();
            if let InventoryDestinationPolicy::MergeThenEmpty { merge_only } =
                &intent.destination_policy
            {
                if merge_only.iter().copied().collect::<BTreeSet<_>>().len() != merge_only.len()
                    || merge_only
                        .iter()
                        .any(|target| !destinations.contains(target))
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if !matches!(self.kind, ActionKind::Inventory | ActionKind::Item)
                || intent.ordered_destinations.is_empty()
                || intent.ordered_destinations.len() > 4096
                || destinations.len() != intent.ordered_destinations.len()
                || intent
                    .ordered_destinations
                    .iter()
                    .any(|candidate| candidate.max_stack == 0 || candidate.max_stack > 99)
                || destinations.contains(&intent.source)
                || matches!(
                    intent.source,
                    Target::Chunk {
                        part: ChunkPart::Entity(_),
                        inventory: None,
                        ..
                    }
                ) && !self.conditions.iter().any(|condition| {
                    condition.target == intent.source
                        && (matches!(condition.expected, Expected::DroppedItem {
                                condition: crate::entity_body::DroppedItemCondition::PickupReady, .. })
                            || matches!(condition.expected, Expected::DroppedItem {
                                condition: crate::entity_body::DroppedItemCondition::HopperReady, .. })
                                && matches!(&self.semantic_intent, Some(SemanticIntent::Pickup { contact, inventory })
                                    if crate::item_lifecycle::hopper_pickup_guard(self, contact, inventory)))
                })
                || matches!(intent.count, InventoryCountRule::Exact(0))
                || !matches!(
                    intent.source,
                    Target::Chunk {
                        part: ChunkPart::Entity(_),
                        inventory: None,
                        ..
                    } | Target::Chunk {
                        inventory: Some(_),
                        ..
                    } | Target::Player {
                        inventory: Some(_),
                        ..
                    }
                )
                || destinations.iter().any(|target| {
                    !matches!(
                        target,
                        Target::Chunk {
                            inventory: Some(_),
                            ..
                        } | Target::Player {
                            inventory: Some(_),
                            ..
                        }
                    )
                })
                || !counterfactual_conditions.contains(&intent.source)
                || !counterfactual_effects.contains(&intent.source)
                || destinations.iter().any(|target| {
                    !counterfactual_conditions.contains(target)
                        || !counterfactual_effects.contains(target)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::MergeDroppedItems { contact, max_stack }) =
            &self.semantic_intent
        {
            if !contact.valid()
                || self.id.actor != contact.source.actor()
                || self.tick != contact.tick
                || self.class != ActionClass::Replayable
                || self.kind != ActionKind::Item
                || !(1..=64).contains(max_stack)
                || self.chunk_transition
                || self.actor_chunk != Some(contact.source.chunk)
                || !matches!(self.effects.as_slice(), [Effect::MergeDroppedItems { source, destination, max_stack: limit, .. }]
                    if *source == contact.source.target() && *destination == contact.destination.target() && limit == max_stack)
            {
                return Err(ApplyFailure::InvalidAction);
            }
            for participant in [&contact.source, &contact.destination] {
                let crate::item_contact::ContactIdentity::Entity { birth, .. } =
                    &participant.identity
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                if !self.conditions.iter().any(|condition| condition.target == participant.target()
                    && matches!(&condition.expected, Expected::DroppedItemStack { birth: observed, owner, epoch, .. }
                        if observed == birth && *owner == participant.owner && *epoch == participant.epoch)) {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        if let Some(SemanticIntent::Pickup { contact, inventory }) = &self.semantic_intent {
            let hopper = crate::item_lifecycle::hopper_pickup_guard(self, contact, inventory);
            let picker_health = Target::Chunk {
                key: contact.picker.chunk,
                part: ChunkPart::LivingProperty {
                    actor: contact.picker.actor(),
                    property: crate::living_property::LivingProperty::Health,
                },
                inventory: None,
            };
            if (!hopper && !contact.valid())
                || !hopper && !self.conditions.iter().any(|condition| condition.target == picker_health
                    && condition.expected == Expected::LivingUnsignedAtLeast(1))
                || self.tick != contact.tick
                || self.id.actor != contact.picker.actor()
                || self.id.incarnation != match &contact.picker.identity {
                    crate::item_contact::ContactIdentity::Player(lease) => lease.incarnation,
                    crate::item_contact::ContactIdentity::Entity { id, .. } => u64::from(id.generation),
                }
                || self.class != ActionClass::Replayable
                || self.kind != ActionKind::Item
                || inventory.source != contact.item.target()
                || ![&contact.picker, &contact.item].into_iter().all(|participant| {
                    self.conditions.iter().any(|condition| condition.target == participant.target()
                        && condition.expected == Expected::SpatialEpochEquals(participant.epoch))
                })
                || !self.conditions.iter().any(|condition| {
                    condition.target == contact.item.target()
                        && matches!((&condition.expected, &contact.item.identity),
                            (Expected::DroppedItem { birth, condition }, crate::item_contact::ContactIdentity::Entity { birth: expected, .. }) if birth == expected && *condition == if hopper { crate::entity_body::DroppedItemCondition::HopperReady } else { crate::entity_body::DroppedItemCondition::PickupReady })
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::CreativeInventory(intent)) = &self.semantic_intent {
            if !intent.valid() || self.effects.iter().filter(|effect| matches!(effect, Effect::CreativeItems { intent: effect_intent } if effect_intent == intent)).count() != 1
                || self.effects.iter().any(|effect| !matches!(effect, Effect::CreativeItems { intent: effect_intent } if effect_intent == intent)
                    && !matches!(effect, Effect::Put { target, value: Value::LivingProperty(_) }
                        if intent.equipment.iter().any(|binding| binding.property_targets().contains(target))))
            { return Err(ApplyFailure::InvalidAction); }
        }
        if let Some(SemanticIntent::MenuSessionChange(intent)) = &self.semantic_intent {
            let previous = self.conditions.iter().find_map(|condition| {
                if condition.target != intent.target { return None; }
                match &condition.expected {
                    Expected::Exact(Cell { value: Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::MenuSession(state)), .. }) => Some(state),
                    _ => None,
                }
            }).ok_or(ApplyFailure::InvalidAction)?;
            let expected = match &intent.operation {
                crate::canonical_menu::MenuSessionOperation::Open { state, .. }
                    if previous.session == 0 && !previous.closed_sessions.iter().any(|closed| closed.session == state.session) => {
                    let mut next = state.clone();
                    next.last_closed_session = previous.last_closed_session;
                    next.last_close = previous.last_close;
                    next.closed_sessions = previous.closed_sessions.clone();
                    next
                }
                crate::canonical_menu::MenuSessionOperation::Select { session, selection }
                    if previous.session == *session => {
                    let mut next = previous.clone();
                    next.selection = selection.clone();
                    next
                }
                _ => return Err(ApplyFailure::InvalidAction),
            };
            if !intent.valid() || self.kind != ActionKind::Inventory || self.class != ActionClass::Replayable
                || self.id.actor != ActorId::Player(intent.lease.gid)
                || self.id.incarnation != intent.lease.incarnation
                || !self.player_leases.contains(&intent.lease)
                || self.effects.len() != 1 + usize::from(intent.admission().is_some())
                || !matches!(&self.effects[0], Effect::Put { target, value: Value::LivingProperty(
                    crate::living_property::LivingPropertyValue::MenuSession(state)) }
                    if *target == intent.target && state.valid() && *state == expected)
                || !self.conditions.iter().any(|condition| condition.target == intent.target)
                || !self.conditions.iter().any(|condition| condition.target == intent.health_target()
                    && condition.expected == Expected::LivingUnsignedAtLeast(1))
                || intent.targets().iter().any(|target| !condition_targets.contains(target)
                    || !counterfactual_conditions.contains(target))
                || !counterfactual_effects.contains(&intent.target) {
                return Err(ApplyFailure::InvalidAction);
            }
            if let Some(admission) = intent.admission() {
                if self.effects.get(1) != Some(&Effect::MenuVirtualInventoryAdmission { admission: admission.clone() })
                    || admission.entries().iter().any(|(target, _)| !self.conditions.iter().any(|condition|
                        condition.target == *target && condition.expected == Expected::Missing)
                        || !counterfactual_effects.contains(target)) {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if let crate::canonical_menu::MenuSessionOperation::Open { state, viewer } = &intent.operation {
                if !self.conditions.iter().any(|condition| condition.target == viewer.target()
                    && condition.expected == Expected::SpatialEpochEquals(viewer.epoch)) {
                    return Err(ApplyFailure::InvalidAction);
                }
                if let crate::canonical_menu::MenuSource::Merchant { contact, kind }
                    | crate::canonical_menu::MenuSource::Entity { contact, kind } = &state.source {
                    let crate::item_contact::ContactIdentity::Entity { birth, .. } = &contact.identity
                        else { return Err(ApplyFailure::InvalidAction); };
                    if !self.conditions.iter().any(|condition| condition.target == contact.target()
                        && condition.expected == Expected::SpatialEpochEquals(contact.epoch))
                        || !self.conditions.iter().any(|condition| condition.target == contact.target()
                            && condition.expected == Expected::EntityBirthKind { birth: birth.clone(), owner: contact.owner, kind: *kind }) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
            }
        }
        if let Some(SemanticIntent::RecipeInventory(intent)) = &self.semantic_intent {
            if !intent.valid()
                || self.effects
                    != vec![Effect::CraftItems {
                        intent: intent.clone(),
                    }]
                || intent.targets().iter().any(|target| {
                    !self
                        .conditions
                        .iter()
                        .any(|condition| condition.target == *target)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::InventoryGestures(intent)) = &self.semantic_intent {
            if !matches!(self.kind, ActionKind::Inventory | ActionKind::Item)
                || !intent.is_valid()
                || intent.entity_equipment.iter().any(|binding| !self.conditions.iter().any(|condition| {
                    condition.target == binding.living
                        && matches!(&condition.expected, Expected::EntityBirthKind { birth, kind, .. }
                            if *birth == binding.birth && *kind == binding.kind)
                }))
                || intent.gestures.iter().any(|gesture| matches!(gesture, crate::inventory::InventoryGesture::SelectHotbar { slot, .. }
                    if !matches!(slot.target, crate::inventory::InventoryTarget::Player(gid) | crate::inventory::InventoryTarget::LeasedPlayer { player: gid, .. }
                        if self.id.actor == ActorId::Player(gid))))
                || intent.gestures.iter().any(|gesture| matches!(gesture, crate::inventory::InventoryGesture::SwapSelectedHand { player }
                    if !matches!(player, crate::inventory::InventoryTarget::Player(gid) | crate::inventory::InventoryTarget::LeasedPlayer { player: gid, .. }
                        if self.id.actor == ActorId::Player(*gid))))
                || intent.targets().any(|target| {
                    !counterfactual_conditions.contains(&target)
                        || !counterfactual_effects.contains(&target)
                })
                || self.effects.iter().any(|effect| {
                    !matches!(effect, Effect::Put { target, value: Value::Inventory(_) }
                        if intent.bindings.iter().any(|binding| binding.target == *target))
                        && !matches!(effect, Effect::Put { target, value: Value::LivingProperty(_) }
                            if intent.property_targets().contains(target))
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::World(intent)) = &self.semantic_intent {
            if self.class != ActionClass::Replayable
                || !intent.valid()
                || !intent.guards_valid(self)
                || intent
                    .read_targets()
                    .iter()
                    .any(|target| !counterfactual_conditions.contains(target))
                || intent
                    .effect_targets()
                    .iter()
                    .any(|target| !counterfactual_effects.contains(target))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::MergeExperience(intent)) = &self.semantic_intent {
            let crate::item_contact::ContactIdentity::Entity {
                id: source_id,
                birth: source_birth,
            } = &intent.contact.source.identity
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            if !intent.contact.valid()
                || self.id.actor != intent.contact.source.actor()
                || self.id.incarnation != source_id.generation
                || self.tick != intent.contact.tick
                || intent.contact.producer != intent.contact.source.owner
                || self.class != ActionClass::Replayable
                || self.kind != ActionKind::Item
                || self.chunk_transition
                || intent.targets().into_iter().any(|target| {
                    !counterfactual_conditions.contains(&target)
                        || !counterfactual_effects.contains(&target)
                })
                || !self.conditions.iter().any(|condition| {
                    condition.target == intent.contact.source.target()
                        && condition.expected
                            == (Expected::ActorControlled {
                                birth: source_birth.clone(),
                                issuer: intent.contact.producer,
                            })
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            for participant in [&intent.contact.source, &intent.contact.destination] {
                let crate::item_contact::ContactIdentity::Entity { birth, .. } =
                    &participant.identity
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                if !self.conditions.iter().any(|condition| {
                    condition.target == participant.target()
                        && condition.expected
                            == (Expected::ExperienceOrb {
                                birth: birth.clone(),
                                owner: participant.owner,
                                epoch: participant.epoch,
                                amount: intent.amount,
                            })
                }) {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        if let Some(SemanticIntent::ExperienceDamage(intent)) = &self.semantic_intent {
            let Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            } = intent.target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            if self.id.actor != ActorId::Entity(id)
                || self.id.incarnation != u64::from(id.generation)
                || self.actor_chunk != Some(key)
                || self.kind != ActionKind::Item
                || self.class != ActionClass::Replayable
                || self.chunk_transition
                || intent.amount == 0
                || intent.owner.generation == 0
                || !counterfactual_conditions.contains(&intent.target)
                || !counterfactual_effects.contains(&intent.target)
                || !self.conditions.iter().any(|condition| {
                    condition.target == intent.target
                        && condition.expected
                            == (Expected::ActorControlled {
                                birth: intent.birth.clone(),
                                issuer: intent.owner,
                            })
                })
                || !self.conditions.iter().any(|condition| {
                    condition.target == intent.target
                        && condition.expected
                            == (Expected::ExperienceOrb {
                                birth: intent.birth.clone(),
                                owner: intent.owner,
                                epoch: intent.epoch,
                                amount: intent.amount,
                            })
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::CollectExperience(intent)) = &self.semantic_intent {
            let crate::item_contact::ContactIdentity::Player(lease) =
                &intent.contact.picker.identity
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let crate::item_contact::ContactIdentity::Entity { birth, .. } =
                &intent.contact.item.identity
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            if !intent.valid() || self.id.actor != ActorId::Player(lease.gid) || self.id.incarnation != lease.incarnation
                || !self.conditions.iter().any(|condition| condition.target == intent.health_target()
                    && condition.expected == Expected::LivingUnsignedAtLeast(1))
                || !self.player_leases.contains(lease) || self.actor_chunk != Some(intent.contact.picker.chunk)
                || self.tick != intent.contact.picker.simulation_tick || self.class != ActionClass::Replayable
                || self.kind != ActionKind::Item || self.chunk_transition
                || intent.targets().any(|target| !counterfactual_conditions.contains(&target)
                    || !counterfactual_effects.contains(&target))
                || !self.conditions.iter().any(|condition| condition.target == intent.contact.picker.target()
                    && condition.expected == Expected::SpatialEpochEquals(intent.contact.picker.epoch))
                || !self.conditions.iter().any(|condition| condition.target == intent.contact.item.target()
                    && matches!(&condition.expected, Expected::ExperienceOrb { birth: expected, owner: expected_owner, epoch, amount }
                        if expected == birth && *expected_owner == intent.contact.item.owner && *epoch == intent.contact.item.epoch && *amount == intent.amount)) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(intent) = self.death_inventory_intent() {
            if !intent.valid(self.id, self.tick) || self.class != ActionClass::Replayable || !(self.kind == ActionKind::Item || self.kind == ActionKind::PlayerAdministration && matches!(&self.semantic_intent, Some(SemanticIntent::Respawn(_))))
                || self.chunk_transition && !matches!(&self.semantic_intent, Some(SemanticIntent::Respawn(_))) || self.actor_chunk != Some(intent.identity.chunk)
                || intent.targets().iter().any(|target| !counterfactual_conditions.contains(target))
                || intent.effect_targets().iter().any(|target| !counterfactual_effects.contains(target))
                || intent.entity_equipment.as_ref().is_some_and(|binding| !self.conditions.iter().any(|condition|
                    condition.target == intent.identity.target() && condition.expected == (Expected::EntityBirthKind {
                        birth: binding.birth.clone(), owner: intent.identity.owner, kind: binding.kind })))
                || !self.conditions.iter().any(|condition| condition.target == intent.identity.target()
                    && condition.expected == Expected::SpatialEpochEquals(intent.identity.epoch))
                || !self.dependencies.contains(&intent.death_cause)
                || !self.conditions.iter().any(|condition| condition.target == intent.property(crate::living_property::LivingProperty::Health)
                    && matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(0)), .. })))
                || !self.conditions.iter().any(|condition| condition.target == intent.property(crate::living_property::LivingProperty::DeathCause)
                    && matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::ActionReference(Some(cause))), .. }) if *cause == intent.death_cause))
                || !self.conditions.iter().any(|condition| condition.target == intent.property(crate::living_property::LivingProperty::DeathInventoryProcessed)
                    && matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Boolean(processed)), .. })
                        if !*processed || matches!(&self.semantic_intent, Some(SemanticIntent::Respawn(_)))))
                || intent.drops.iter().any(|drop| !self.conditions.iter().any(|condition| condition.target == drop.destination && condition.expected == Expected::Missing)) {
                return Err(ApplyFailure::InvalidAction);
            }
            match &intent.identity.identity {
                crate::item_contact::ContactIdentity::Player(lease) if self.player_leases.contains(lease) && self.id.incarnation == lease.incarnation => {},
                crate::item_contact::ContactIdentity::Entity { birth, id } if self.id.incarnation == id.generation
                    && self.conditions.iter().any(|condition| condition.target == intent.identity.target()
                        && condition.expected == (Expected::ActorControlled { birth: birth.clone(), issuer: intent.identity.owner })) => {},
                _ => return Err(ApplyFailure::InvalidAction),
            }
        }
        if let Some(SemanticIntent::ProjectileTick(intent)) = &self.semantic_intent {
            let crate::item_contact::ContactIdentity::Entity { birth, .. } = &intent.projectile.identity else { return Err(ApplyFailure::InvalidAction); };
            if !intent.valid(self.id, self.tick) || self.kind != ActionKind::Item || self.class != ActionClass::Replayable
                || self.chunk_transition || self.actor_chunk != Some(intent.projectile.chunk)
                || intent.targets().iter().any(|target| !counterfactual_conditions.contains(target) || !counterfactual_effects.contains(target))
                || !self.conditions.iter().any(|condition| condition.target == intent.projectile.target()
                    && condition.expected == (Expected::ActorControlled { birth: birth.clone(), issuer: intent.projectile.owner }))
                || !self.conditions.iter().any(|condition| condition.target == intent.projectile.target()
                    && condition.expected == Expected::SpatialEpochEquals(intent.projectile.epoch))
                || intent.victims.iter().any(|victim| !self.conditions.iter().any(|condition| condition.target == victim.target()
                    && condition.expected == Expected::SpatialEpochEquals(victim.epoch)) || match &victim.identity {
                        crate::item_contact::ContactIdentity::Player(lease) => !self.player_leases.contains(lease),
                        crate::item_contact::ContactIdentity::Entity { birth, .. } => !self.conditions.iter().any(|condition| condition.target == victim.target()
                            && condition.expected == (Expected::EntityBirthOwned { birth: birth.clone(), owner: victim.owner })),
                    }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::DropItem(intent)) = &self.semantic_intent {
            let belongs = match intent.source.target {
                Target::Player {
                    lease: Some(lease), ..
                } => self.id.actor == ActorId::Player(lease.gid),
                Target::Chunk {
                    part: ChunkPart::Entity(entity),
                    ..
                } => self.id.actor == ActorId::Entity(entity),
                _ => false,
            };
            if !belongs || !intent.valid(self.id, self.tick) || self.kind != ActionKind::Item
                || self.class != ActionClass::Replayable || self.chunk_transition
                || intent.targets().any(|target| !counterfactual_conditions.contains(&target)
                    || !counterfactual_effects.contains(&target))
                || !self.conditions.iter().any(|condition| condition.target == intent.destination
                    && matches!(condition.expected, Expected::Missing))
                || intent.selected.is_none() && !self.conditions.iter().any(|condition| condition.target == intent.source.target
                    && matches!(&condition.expected, Expected::InventoryItem { item, nbt, minimum }
                        if matches!(&intent.body.payload, crate::entity_body::EntityPayload::Item(body)
                            if body.item == *item && body.components == *nbt && *minimum > 0))) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::VehicleTick(intent)) = &self.semantic_intent {
            if !intent.activation_valid() { return Err(ApplyFailure::InvalidAction); }
            if intent.activation.as_ref().is_some_and(|activation| activation.detach.is_some())
                && !crate::vehicle_tick::valid_activator_detach(self) { return Err(ApplyFailure::InvalidAction); }
            let Target::Chunk {
                key,
                part: ChunkPart::Entity(entity),
                inventory: None,
            } = intent.body
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let footprint = intent.targets();
            if self.class != ActionClass::Replayable || self.kind != ActionKind::Actor
                || self.id.actor != ActorId::Entity(entity) || self.id.incarnation != entity.generation
                || self.actor_chunk != Some(key) || intent.owner.generation == 0
                || !self.conditions.iter().any(|condition| condition.target == intent.body
                    && condition.expected == (Expected::ActorControlled { birth: intent.birth.clone(), issuer: intent.owner }))
                || !self.conditions.iter().any(|condition| condition.target == intent.body
                    && condition.expected == Expected::SpatialEpochEquals(intent.epoch))
                || self.effects.iter().any(|effect| !matches!(effect, Effect::Put { target, value: Value::VehicleProperty(value) }
                    if footprint.contains(target) && matches!(target, Target::Chunk { part: ChunkPart::VehicleProperty { property, .. }, .. } if property.accepts(value)))
                    && !(crate::vehicle_tick::permits_activator_detach(self)
                        && intent.activation.as_ref().and_then(|activation| activation.detach.as_ref()).is_some_and(|plan| plan.effects.contains(effect)))) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::ItemUse(intent)) = &self.semantic_intent {
            if let Some(launch) = &intent.launch {
                if launch.shooter.actor() != self.id.actor
                    || launch.shooter.simulation_tick != self.tick
                    || launch.shooter.target() != intent.living
                    || !self.conditions.iter().any(|condition| {
                        condition.target == intent.living
                            && condition.expected
                                == Expected::SpatialEpochEquals(launch.shooter.epoch)
                    })
                    || launch.ammunition.iter().any(|binding| {
                        match (intent.source.target, binding.target) {
                            (
                                Target::Player {
                                    lease: Some(source),
                                    ..
                                },
                                Target::Player {
                                    lease: Some(ammo), ..
                                },
                            ) => source != ammo,
                            (
                                Target::Chunk {
                                    part: ChunkPart::Entity(source),
                                    ..
                                },
                                Target::Chunk {
                                    part: ChunkPart::Entity(ammo),
                                    ..
                                },
                            ) => source != ammo,
                            _ => true,
                        }
                    })
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            let belongs = match intent.source.target {
                Target::Player {
                    lease: Some(lease), ..
                } => {
                    self.id.actor == ActorId::Player(lease.gid)
                        && matches!(intent.living, Target::Chunk { part: ChunkPart::Player(gid), inventory: None, .. } if gid == lease.gid)
                }
                Target::Chunk {
                    part: ChunkPart::Entity(entity),
                    ..
                } => {
                    self.id.actor == ActorId::Entity(entity)
                        && matches!(intent.living, Target::Chunk { part: ChunkPart::Entity(id), inventory: None, .. } if id == entity)
                }
                _ => false,
            };
            if !intent.valid()
                || !belongs
                || self.kind != ActionKind::Item
                || self.class != ActionClass::Replayable
                || self.chunk_transition
                || intent.targets().any(|target| {
                    !counterfactual_conditions.contains(&target)
                        || !counterfactual_effects.contains(&target)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::Object(intent)) = &self.semantic_intent {
            if self.kind != ActionKind::Actor || self.class != ActionClass::Replayable
                || !intent.guards_valid(self) || self.chunk_transition
                || intent.targets().any(|target| !counterfactual_conditions.contains(&target))
                || self.counterfactual_effects != vec![intent.facet(intent.property())]
                || self.effects.iter().any(|effect| !matches!(effect, Effect::Put { target, value: Value::ObjectProperty(_) } if *target == intent.facet(intent.property()))) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::ObjectTick(intent)) = &self.semantic_intent {
            if !intent.valid(self.id)
                || self.class != ActionClass::Replayable
                || self.kind != ActionKind::Actor
                || self.chunk_transition
                || self.actor_chunk != Some(intent.chunk)
                || !self.player_leases.is_empty()
                || !self.player_chunks.is_empty()
                || self.stakeholders != vec![Stakeholder::Chunk(intent.chunk)]
                || !self.conditions.iter().any(|condition| {
                    condition.target == intent.body_target()
                        && condition.expected
                            == (Expected::ActorControlled {
                                birth: intent.birth.clone(),
                                issuer: intent.owner,
                            })
                })
                || !self.conditions.iter().any(|condition| {
                    condition.target == intent.body_target()
                        && matches!(condition.expected, Expected::SpatialEpochEquals(_))
                })
                || self.effects.iter().any(|effect| {
                    !matches!(effect, Effect::Put { target, value: Value::ObjectProperty(_) }
                    if intent.targets().contains(target))
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::Respawn(intent)) = &self.semantic_intent {
            if self.class != ActionClass::Replayable
                || self.kind != ActionKind::PlayerAdministration
                || !intent.is_valid()
                || !intent.guards_valid(self)
                || intent.targets()?.iter().any(|target| !counterfactual_conditions.contains(target))
                || intent.effect_targets()?.iter().any(|target| !counterfactual_effects.contains(target))
                || self.effects.iter().flat_map(Effect::direct_targets).any(|target| !intent.effect_targets().is_ok_and(|targets| targets.contains(&target)))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::Wake(intent)) = &self.semantic_intent {
            if self.class != ActionClass::Replayable || self.kind != ActionKind::PlayerAdministration
                || self.chunk_transition || !intent.matches_action(self) { return Err(ApplyFailure::InvalidAction); }
        }
        if let Some(SemanticIntent::ActorTags(intent)) = &self.semantic_intent {
            if self.class != ActionClass::Replayable
                || self.kind != ActionKind::PlayerAdministration
                || self.chunk_transition
                || !intent.valid(self.id)
                || intent.targets().iter().any(|target| !counterfactual_conditions.contains(target) || !counterfactual_effects.contains(target))
                || self.effects.iter().flat_map(Effect::direct_targets).any(|target| !intent.targets().contains(&target))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::MobBreeding(intent)) = &self.semantic_intent {
            if self.class != ActionClass::Replayable
                || self.kind != ActionKind::Actor
                || self.chunk_transition
                || !intent.is_valid(self.tick)
                || !intent.guards_valid(self)
                || intent.resolution_targets().iter().any(|target| {
                    !counterfactual_conditions.contains(target)
                })
                || intent.effect_targets().iter().any(|target| {
                    !counterfactual_effects.contains(target)
                })
                || self
                    .effects
                    .iter()
                    .flat_map(Effect::direct_targets)
                    .any(|target| !intent.effect_targets().contains(&target))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::AreaCloud(intent)) = &self.semantic_intent {
            let targets = intent.targets();
            if self.class != ActionClass::Replayable || self.kind != ActionKind::Actor || self.chunk_transition
                || !intent.valid(self.id, self.tick) || !intent.guards_valid(self)
                || targets.iter().any(|target| !counterfactual_conditions.contains(target) || !counterfactual_effects.contains(target))
                || self.effects.iter().flat_map(Effect::direct_targets).any(|target| !targets.contains(&target)) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let Some(SemanticIntent::Combat(intent)) = &self.semantic_intent {
            let resolution_targets = intent.resolution_targets().into_iter().collect::<BTreeSet<_>>();
            if self.kind != ActionKind::Combat
                || !intent.is_valid(self.tick)
                || !intent.guards_valid(self)
                || resolution_targets.iter().any(|target| {
                    !counterfactual_conditions.contains(target)
                        || !counterfactual_effects.contains(target)
                }) || self.effects.iter().any(|effect| {
                !matches!(effect,
                    Effect::Put { target, value: Value::LivingProperty(_) }
                    if resolution_targets.contains(target))
                    && !matches!(effect, Effect::Put { target, value: Value::VehicleProperty(_) }
                        if resolution_targets.contains(target))
                    && !matches!(effect, Effect::TransformItems { intent: transform }
                        if intent.permits_item_transform(transform))
            }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        for effect in &self.effects {
            touched.extend(effect.targets());
        }
        touched.extend(self.counterfactual_conditions.iter().copied());
        touched.extend(self.counterfactual_effects.iter().copied());
        let required = touched
            .iter()
            .map(|target| target.stakeholder())
            .chain(self.actor_chunk.map(Stakeholder::Chunk))
            .chain(
                self.player_chunks
                    .iter()
                    .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
            )
            .collect::<BTreeSet<_>>();
        if required.iter().copied().collect::<Vec<_>>() != self.stakeholders {
            return Err(ApplyFailure::InvalidAction);
        }
        for target in touched {
            if !target.is_valid() {
                return Err(ApplyFailure::InvalidAction);
            }
            let stakeholder = target.stakeholder();
            if self.stakeholders.binary_search(&stakeholder).is_err() {
                return Err(ApplyFailure::MissingStakeholder(stakeholder));
            }
        }
        for effect in &self.effects {
            for target in effect.direct_targets() {
                if matches!(
                    target,
                    Target::Chunk {
                        part: ChunkPart::ActorOccupancy,
                        ..
                    }
                ) {
                    return Err(ApplyFailure::InvalidAction);
                }
                if self.class != ActionClass::Infallible
                    && !conditioned.contains(&target)
                    && !matches!(effect, Effect::ScheduleTick { .. })
                {
                    return Err(ApplyFailure::Condition(target));
                }
            }
            match effect {
                Effect::RetireEntity { intent } => {
                    if !intent.guards_valid(self) { return Err(ApplyFailure::InvalidAction); }
                }
                Effect::EntityAdmission {
                    target,
                    body,
                    inventory,
                } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Entity(entity),
                        inventory: None,
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    let reference = ActionRef {
                        tick: self.tick,
                        id: self.id,
                    };
                    let standalone = self.id.actor == ActorId::Entity(*entity)
                        && self.id.incarnation == entity.generation
                        && self.class == ActionClass::Infallible
                        && self.effects.len() == 1
                        && self.conditions.is_empty()
                        && self.actor_chunk == Some(*key);
                    let breeding = self.class == ActionClass::Replayable
                        && matches!(&self.semantic_intent, Some(SemanticIntent::MobBreeding(intent))
                            if intent.guards_valid(self) && intent.permits_admission(*target, body, inventory));
                    let terminal = crate::object_tick::permits_terminal_admission(self, *target, body, inventory);
                    let placement = crate::entity_placement::permits_admission(self, *target, body, inventory);
                    let issuer = crate::issuer_entity_admission::permits_admission(self, *target, body, inventory);
                    if (!standalone && !breeding && !terminal && !issuer && !placement)
                        || self.chunk_transition
                        || !body.belongs_to(*entity, *key)
                        || !body.valid_at(self.tick)
                        || body.birth.action != reference
                        || body.spatial_epoch != SpatialEpoch::Action(reference)
                        || body.birth.source.generation == 0
                        || body.owner.generation == 0
                        || !inventory.valid()
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::Put { target, value } => {
                    if matches!(value, Value::LivingProperty(crate::living_property::LivingPropertyValue::MenuSession(_)))
                        && !self.menu_session_intent().is_some_and(|intent| intent.target == *target && intent.guards_valid(self)) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if let Value::Living(state) = value {
                        let actor = match target {
                            Target::Chunk {
                                part: ChunkPart::Player(id),
                                inventory: None,
                                ..
                            } => ActorId::Player(*id),
                            _ => return Err(ApplyFailure::Type(*target)),
                        };
                        if !state.belongs_to(actor) {
                            return Err(ApplyFailure::Type(*target));
                        }
                    }
                    match (target, value) {
                        (
                            Target::Chunk {
                                key,
                                part: ChunkPart::Entity(id),
                                inventory: None,
                            },
                            Value::Entity(body),
                        ) if body.belongs_to(*id, *key) && body.valid_at(self.tick) => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::Block(_),
                                inventory: None,
                                ..
                            },
                            Value::Block(_),
                        ) => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::BlockEntity(_),
                                inventory: None,
                                ..
                            },
                            Value::BlockEntity(bytes),
                        ) if !bytes.is_empty() && bytes.len() <= 65_536 => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::LightSection(_),
                                inventory: None,
                                ..
                            },
                            Value::LightSection { sky, block },
                        ) if sky.len() == 2_048 && block.len() == 2_048 => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::Player(_),
                                inventory: None,
                                ..
                            },
                            Value::Living(_),
                        )
                        | (
                            Target::Player {
                                inventory: None, ..
                            },
                            Value::Scalar(_),
                        ) => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::LivingProperty { actor, property },
                                inventory: None,
                                ..
                            },
                            Value::LivingProperty(value),
                        ) if property.accepts(value, *actor) => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::ObjectProperty { property, .. },
                                inventory: None,
                                ..
                            },
                            Value::ObjectProperty(value),
                        ) if property.accepts(value)
                            && (matches!(&self.semantic_intent, Some(SemanticIntent::Object(intent)) if intent.facet(intent.property()) == *target)
                                || matches!(&self.semantic_intent, Some(SemanticIntent::ObjectTick(intent)) if intent.targets().contains(target))
                                || matches!(&self.semantic_intent, Some(SemanticIntent::AreaCloud(intent)) if intent.targets().contains(target))) =>
                            {}
                        (
                            Target::Chunk { part: ChunkPart::ActorTags { .. }, inventory: None, .. },
                            Value::ActorTags(tags),
                        ) if crate::actor_tags::valid(tags)
                            && matches!(&self.semantic_intent, Some(SemanticIntent::ActorTags(intent)) if intent.target() == *target) => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::ActorRelation { actor, relation },
                                inventory: None,
                                ..
                            },
                            Value::ActorRelation(value),
                        ) if crate::actor_relation::accepts(*actor, *relation, value) => {}
                        (
                            Target::Chunk {
                                part: ChunkPart::VehicleProperty { property, .. },
                                inventory: None,
                                ..
                            },
                            Value::VehicleProperty(value),
                        ) if property.accepts(value) => {}
                        (
                            Target::Chunk {
                                inventory: Some(id),
                                ..
                            },
                            Value::Inventory(cell),
                        )
                        | (
                            Target::Player {
                                inventory: Some(id),
                                ..
                            },
                            Value::Inventory(cell),
                        ) if id.location() == (cell.loc.inv, cell.loc.slot) && cell.count <= 99 => {
                        }
                        _ => return Err(ApplyFailure::Type(*target)),
                    }
                }
                Effect::CreativeItems { intent } => {
                    let ActorId::Player(gid) = self.id.actor else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if !intent.valid() || self.kind != ActionKind::Inventory || self.class != ActionClass::Replayable
                        || !matches!(intent.mode, Target::Chunk { part: ChunkPart::LivingProperty { actor: ActorId::Player(player), property: crate::living_property::LivingProperty::GameMode }, .. } if player == gid)
                        || !self.conditions.iter().any(|condition| condition.target == intent.mode
                            && matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(1)), .. })))
                        || intent.bindings.iter().any(|binding| !matches!(binding.target, Target::Player { lease: Some(lease), .. }
                            if lease.gid == gid && lease.incarnation == self.id.incarnation && self.player_leases.contains(&lease)))
                        || intent.targets().any(|target| !counterfactual_conditions.contains(&target)
                            || !counterfactual_effects.contains(&target))
                    { return Err(ApplyFailure::InvalidAction); }
                }
                Effect::MenuVirtualInventoryAdmission { admission } => {
                    if !self.menu_session_intent().is_some_and(|intent|
                        intent.admission().as_ref() == Some(admission) && intent.guards_valid(self)) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::TickBlockInventory { intent } => {
                    if self.kind != ActionKind::World || self.class != ActionClass::Replayable || !intent.guards_valid(self)
                        || !matches!(&self.semantic_intent, Some(SemanticIntent::World(world))
                            if world.steps.iter().filter(|step| matches!(step, crate::world_action::WorldReplayStep::BlockInventoryTick(actual) if actual == intent)).count() == 1)
                        || intent.targets().iter().any(|target| !counterfactual_conditions.contains(target))
                        || intent.effect_targets().iter().any(|target| !counterfactual_effects.contains(target)) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::TransformMenuInventory { intent } => {
                    if !intent.valid() || self.kind != ActionKind::Inventory || self.class != ActionClass::Replayable
                        || self.id.actor != ActorId::Player(intent.lease.gid) || self.id.incarnation != intent.lease.incarnation
                        || !self.player_leases.contains(&intent.lease)
                        || self.semantic_intent.as_ref() != Some(&SemanticIntent::MenuInventoryTransform(intent.clone()))
                        || self.effects != vec![Effect::TransformMenuInventory { intent: intent.clone() }]
                        || intent.targets().iter().any(|target| !condition_targets.contains(target) || !counterfactual_conditions.contains(target))
                        || intent.bindings().iter().any(|binding| !counterfactual_effects.contains(&binding.target))
                        || !self.conditions.iter().any(|condition| condition.target == intent.viewer.target()
                            && condition.expected == Expected::SpatialEpochEquals(intent.viewer.epoch))
                        || !self.conditions.iter().any(|condition| condition.target == intent.property_target(crate::living_property::LivingProperty::Health)
                            && condition.expected == Expected::LivingUnsignedAtLeast(1)) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::TradeItems { intent } => {
                    if !intent.valid() || self.kind != ActionKind::Inventory || self.class != ActionClass::Replayable
                        || self.id.actor != ActorId::Player(intent.menu.lease.gid)
                        || self.id.incarnation != intent.menu.lease.incarnation
                        || !self.player_leases.contains(&intent.menu.lease)
                        || self.semantic_intent.as_ref() != Some(&SemanticIntent::MerchantTrade(intent.clone()))
                        || self.effects != vec![Effect::TradeItems { intent: intent.clone() }]
                        || intent.targets().iter().any(|target| !condition_targets.contains(target)
                            || !counterfactual_conditions.contains(target) || !counterfactual_effects.contains(target)) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    for participant in [&intent.menu.viewer, &intent.menu.merchant] {
                        if !self.conditions.iter().any(|condition| condition.target == participant.target()
                            && condition.expected == Expected::SpatialEpochEquals(participant.epoch)) {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                    if intent.health_targets().iter().any(|target| !self.conditions.iter().any(|condition|
                        condition.target == *target && condition.expected == Expected::LivingUnsignedAtLeast(1))) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let crate::item_contact::ContactIdentity::Entity { birth, .. } = &intent.menu.merchant.identity
                        else { return Err(ApplyFailure::InvalidAction); };
                    if !self.conditions.iter().any(|condition| condition.target == intent.menu.merchant.target()
                        && condition.expected == Expected::EntityBirthKind { birth: birth.clone(),
                            owner: intent.menu.merchant.owner, kind: intent.menu.merchant_kind }) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::CraftItems { intent } => {
                    if !intent.valid()
                        || self.kind != ActionKind::Inventory
                        || self.class != ActionClass::Replayable
                        || self.semantic_intent.as_ref()
                            != Some(&SemanticIntent::RecipeInventory(intent.clone()))
                        || intent.targets().iter().any(|target| {
                            !counterfactual_conditions.contains(target)
                                || !counterfactual_effects.contains(target)
                        })
                        || !matches!(self.id.actor, ActorId::Player(gid) if self.player_leases.iter()
                            .any(|lease| lease.gid == gid && lease.incarnation == self.id.incarnation))
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::CloseInventory { intent } => {
                    let embedded_death = self.death_inventory_intent().is_some_and(|death| death.close.as_ref() == Some(intent));
                    if !intent.valid(ActionRef {
                        tick: self.tick,
                        id: self.id,
                    }) || !(self.kind == ActionKind::Inventory || embedded_death
                        && matches!(self.kind, ActionKind::Item | ActionKind::PlayerAdministration))
                        || self.class != ActionClass::Replayable
                        || !embedded_death && self.effects
                            != vec![Effect::CloseInventory {
                                intent: intent.clone(),
                            }]
                        || !embedded_death && self.semantic_intent.as_ref()
                            != Some(&SemanticIntent::InventoryClose(intent.clone()))
                        || !self.player_leases.contains(&intent.lease)
                        || intent.targets().iter().any(|target| {
                            !self
                                .conditions
                                .iter()
                                .any(|condition| condition.target == *target)
                                || !counterfactual_conditions.contains(target)
                                || !counterfactual_effects.contains(target)
                        })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::CrossbowItems { intent } => {
                    let own_source = match intent.weapon.target {
                        Target::Player {
                            lease: Some(lease), ..
                        } => self.id.actor == ActorId::Player(lease.gid),
                        Target::Chunk {
                            part: ChunkPart::Entity(entity),
                            ..
                        } => self.id.actor == ActorId::Entity(entity),
                        _ => false,
                    };
                    if !own_source
                        || !intent.valid()
                        || !matches!(&self.semantic_intent,
                        Some(SemanticIntent::ItemUse(use_intent)) if use_intent.source == intent.weapon
                            && intent.ammunition.as_ref().is_none_or(|ammo| use_intent.launch.as_ref().is_some_and(|launch| launch.ammunition.contains(ammo))))
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::CollectProjectile { intent } => {
                    let crate::item_contact::ContactIdentity::Player(lease) = &intent.contact.picker.identity else { return Err(ApplyFailure::InvalidAction); };
                    let crate::item_contact::ContactIdentity::Entity { birth, .. } = &intent.contact.item.identity else { return Err(ApplyFailure::InvalidAction); };
                    if !intent.valid() || self.kind != ActionKind::Item || self.class != ActionClass::Replayable
                        || self.chunk_transition || self.id.actor != ActorId::Player(lease.gid) || self.id.incarnation != lease.incarnation
                        || self.tick != intent.contact.tick || self.actor_chunk != Some(intent.contact.picker.chunk)
                        || !self.player_leases.contains(lease)
                        || self.semantic_intent != Some(SemanticIntent::CollectProjectile(intent.clone()))
                        || self.effects != vec![Effect::CollectProjectile { intent: intent.clone() }]
                        || intent.targets().iter().any(|target| !counterfactual_conditions.contains(target))
                        || intent.effect_targets().iter().any(|target| !counterfactual_effects.contains(target))
                        || !self.conditions.iter().any(|condition| condition.target == intent.contact.picker.target()
                            && condition.expected == Expected::SpatialEpochEquals(intent.contact.picker.epoch))
                        || !self.conditions.iter().any(|condition| condition.target == intent.contact.item.target()
                            && condition.expected == Expected::SpatialEpochEquals(intent.contact.item.epoch))
                        || !self.conditions.iter().any(|condition| condition.target == intent.contact.item.target()
                            && condition.expected == (Expected::EntityBirthOwned { birth: birth.clone(), owner: intent.contact.item.owner })) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::ProjectileShooterTeleport { intent } => {
                    if self.kind != ActionKind::Item || self.class != ActionClass::Replayable || self.chunk_transition
                        || !matches!(&self.semantic_intent, Some(SemanticIntent::ProjectileTick(projectile)) if projectile.permits_teleport(intent))
                        || !self.conditions.iter().any(|condition| condition.target == intent.shooter.target()
                            && condition.expected == Expected::SpatialEpochEquals(intent.shooter.epoch)) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::TransformItems { intent } => {
                    if matches!(intent.operation, crate::canonical_inventory::ItemTransformOperation::MendingRepair { .. })
                        && !matches!(&self.semantic_intent, Some(SemanticIntent::CollectExperience(_))) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let actor_owns_source = match intent.source.target {
                        Target::Player {
                            lease: Some(lease), ..
                        } => self.id.actor == ActorId::Player(lease.gid),
                        Target::Chunk {
                            part: ChunkPart::Entity(entity),
                            ..
                        } => self.id.actor == ActorId::Entity(entity),
                        _ => false,
                    };
                    let combat_victim_armor = matches!(
                        &intent.operation,
                        crate::canonical_inventory::ItemTransformOperation::ArmorDamage { .. } | crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. }
                    ) && matches!(&self.semantic_intent, Some(SemanticIntent::Combat(combat)) if combat.permits_victim_armor_transform(intent))
                        || matches!(&self.semantic_intent, Some(SemanticIntent::ProjectileTick(projectile)) if projectile.permits_transform(intent))
                        || crate::object_tick::terminal_witness(self).is_some_and(|witness| witness.combat.permits_victim_armor_transform(intent));
                    if !intent.valid() || !(actor_owns_source || combat_victim_armor) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if !self.conditions.iter().any(|condition| condition.target == intent.source.target
                        && match &condition.expected {
                            Expected::InventoryItem { item, nbt, minimum } => *item == intent.item
                                && *nbt == intent.components && *minimum >= match intent.operation {
                                    crate::canonical_inventory::ItemTransformOperation::Consume { count } => count,
                                    crate::canonical_inventory::ItemTransformOperation::Damage { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::AttackDamage
                                    | crate::canonical_inventory::ItemTransformOperation::MineDamage { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ThrowTrident
                                    | crate::canonical_inventory::ItemTransformOperation::MendingRepair { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ExchangeBucket { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ArmorDamage { .. } => 1,
                                },
                            Expected::Exact(Cell { value: Value::Inventory(item), .. }) => item.item == intent.item
                                && item.nbt == intent.components && item.count >= match intent.operation {
                                    crate::canonical_inventory::ItemTransformOperation::Consume { count } => count,
                                    crate::canonical_inventory::ItemTransformOperation::Damage { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::AttackDamage
                                    | crate::canonical_inventory::ItemTransformOperation::MineDamage { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ThrowTrident
                                    | crate::canonical_inventory::ItemTransformOperation::MendingRepair { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ExchangeBucket { .. }
                                    | crate::canonical_inventory::ItemTransformOperation::ArmorDamage { .. } => 1,
                                },
                            _ => false,
                        }) {
                        return Err(ApplyFailure::Condition(intent.source.target));
                    }
                }
                Effect::MoveItems {
                    source,
                    destination,
                    count,
                    max_stack,
                } if !matches!(
                    source,
                    Target::Chunk {
                        part: ChunkPart::Entity(_),
                        inventory: None,
                        ..
                    } | Target::Chunk {
                        inventory: Some(_),
                        ..
                    } | Target::Player {
                        inventory: Some(_),
                        ..
                    }
                ) || !matches!(
                    destination,
                    Target::Chunk {
                        inventory: Some(_),
                        ..
                    } | Target::Player {
                        inventory: Some(_),
                        ..
                    }
                ) || *count == 0
                    || [source, destination].into_iter().any(|target| {
                        matches!(
                            target,
                            Target::Player {
                                inventory: Some(InventoryId::Player {
                                    inv: crate::inventory::INV_SELECTED,
                                    ..
                                }),
                                ..
                            }
                        )
                    })
                    || *max_stack == 0
                    || *max_stack > 99 =>
                {
                    return Err(ApplyFailure::Inventory(*source));
                }
                Effect::MergeDroppedItems {
                    source,
                    destination,
                    max_stack,
                    count,
                } => {
                    if source == destination
                        || *max_stack == 0
                        || *max_stack > 99
                        || *count == 0
                        || *count > *max_stack
                        || !matches!(self.class, ActionClass::Undoable | ActionClass::Replayable)
                        || self.kind != ActionKind::Item
                        || self.chunk_transition
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    for target in [source, destination] {
                        let Target::Chunk {
                            key,
                            part: ChunkPart::Entity(id),
                            inventory: None,
                        } = *target
                        else {
                            return Err(ApplyFailure::Type(*target));
                        };
                        if !self.conditions.iter().any(|condition| condition.target == *target
                            && (matches!(&condition.expected, Expected::Exact(Cell { value: Value::Entity(body), .. })
                                if body.belongs_to(id, key) && matches!(&body.payload, crate::entity_body::EntityPayload::Item(_)))
                                || matches!(&condition.expected, Expected::DroppedItemStack { birth, .. } if birth.source.server == id.origin))) {
                            return Err(ApplyFailure::Condition(*target));
                        }
                    }
                }
                Effect::AdvanceExperienceLifetime {
                    target,
                    owner,
                    birth,
                } => {
                    let Target::Chunk {
                        part: ChunkPart::Entity(id),
                        inventory: None,
                        ..
                    } = *target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.id.actor != ActorId::Entity(id)
                        || self.id.incarnation != u64::from(id.generation)
                        || owner.generation == 0
                        || birth.uuid.is_nil()
                        || birth.source.server != id.origin
                        || self.kind != ActionKind::Item
                        || self.class != ActionClass::Infallible
                        || self.chunk_transition
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::ChangeDroppedItem {
                    target,
                    issuer,
                    body_owner,
                    birth,
                    change,
                } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Entity(id),
                        inventory: None,
                    } = *target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.kind != ActionKind::Item
                        || self.chunk_transition
                        || issuer.generation == 0
                        || body_owner.generation == 0
                        || birth.uuid.is_nil()
                        || birth.source.server != id.origin
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if *change != crate::entity_body::DroppedItemChange::AdvanceLifetime {
                        let witnessed = self.conditions.iter().any(|condition| {
                            if condition.target != *target {
                                return false;
                            }
                            if let Expected::DroppedItem {
                                birth: expected_birth,
                                condition,
                            } = &condition.expected
                            {
                                return expected_birth == birth
                                    && match change {
                                        crate::entity_body::DroppedItemChange::Damage {
                                            ..
                                        } => {
                                            *condition
                                                == crate::entity_body::DroppedItemCondition::Alive
                                        }
                                        crate::entity_body::DroppedItemChange::Expire => {
                                            *condition
                                                == crate::entity_body::DroppedItemCondition::Expired
                                        }
                                        crate::entity_body::DroppedItemChange::AdvanceLifetime => {
                                            false
                                        }
                                    };
                            }
                            let Expected::Exact(Cell {
                                value: Value::Entity(body),
                                ..
                            }) = &condition.expected
                            else {
                                return false;
                            };
                            let crate::entity_body::EntityPayload::Item(item) = &body.payload
                            else {
                                return false;
                            };
                            body.belongs_to(id, key)
                                && &body.birth == birth
                                && (*change != crate::entity_body::DroppedItemChange::Expire
                                    || (!item.never_despawn && item.age >= 6000))
                        });
                        if !witnessed {
                            return Err(ApplyFailure::Condition(*target));
                        }
                    }
                    match change {
                        crate::entity_body::DroppedItemChange::AdvanceLifetime => {
                            if self.class != ActionClass::Infallible
                                || self.effects.len() != 1
                                || !((self.id.actor == ActorId::Entity(id)
                                    && self.id.incarnation == u64::from(id.generation))
                                    || (self.id.actor == ActorId::Peer(issuer.server)
                                        && self.id.incarnation == issuer.generation))
                                || self.actor_chunk != Some(key)
                            {
                                return Err(ApplyFailure::InvalidAction);
                            }
                        }
                        crate::entity_body::DroppedItemChange::Damage { amount_milli, .. } => {
                            if *amount_milli == 0 || self.class != ActionClass::Undoable {
                                return Err(ApplyFailure::InvalidAction);
                            }
                        }
                        crate::entity_body::DroppedItemChange::Expire => {
                            if self.class != ActionClass::Undoable
                                || self.id.actor != ActorId::Entity(id)
                                || self.id.incarnation != u64::from(id.generation)
                                || self.actor_chunk != Some(key)
                            {
                                return Err(ApplyFailure::InvalidAction);
                            }
                        }
                    }
                }
                Effect::UpdateMobContinuation {
                    target,
                    authority,
                    brain,
                } => {
                    let Target::Chunk {
                        key,
                        part:
                            ChunkPart::LivingProperty {
                                actor: ActorId::Entity(id),
                                property: crate::living_property::LivingProperty::MobContinuation,
                            },
                        inventory: None,
                    } = *target
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if self.class != ActionClass::Infallible
                        || self.kind != ActionKind::Actor
                        || self.chunk_transition
                        || self.effects.len() != 1
                        || self.id.actor != ActorId::Entity(id)
                        || self.id.incarnation != u64::from(id.generation)
                        || self.actor_chunk != Some(key)
                        || authority.issuer != authority.body_owner
                        || authority.issuer.generation == 0
                        || authority.birth.source.server != id.origin
                        || authority.birth.source.generation == 0
                        || authority.birth.uuid.is_nil()
                        || brain.processed_tick != Some(self.tick)
                        || !brain.valid()
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::UpdateVehicleProperty {
                    target,
                    authority,
                    value,
                } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::VehicleProperty { actor, property },
                        inventory: None,
                    } = *target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.class != ActionClass::Infallible
                        || self.kind != ActionKind::Actor
                        || self.chunk_transition
                        || self.effects.len() > crate::vehicle_property::VehicleProperty::ALL.len()
                        || !self.conditions.is_empty()
                        || self.effects.iter().any(|effect| match effect {
                            Effect::UpdateVehicleProperty {
                                target: other,
                                authority: other_authority,
                                ..
                            } => {
                                other_authority != authority
                                    || crate::vehicle_property::body_target(*other).is_none_or(
                                        |(body, _)| {
                                            body != Target::Chunk {
                                                key,
                                                part: ChunkPart::Entity(actor),
                                                inventory: None,
                                            }
                                        },
                                    )
                            }
                            _ => true,
                        })
                        || self
                            .effects
                            .iter()
                            .filter(|effect| {
                                matches!(effect,
                            Effect::UpdateVehicleProperty { target: other, .. } if other == target)
                            })
                            .count()
                            != 1
                        || self.id.actor != ActorId::Entity(actor)
                        || self.id.incarnation != u64::from(actor.generation)
                        || self.actor_chunk != Some(key)
                        || authority.issuer != authority.body_owner
                        || authority.issuer.generation == 0
                        || authority.birth.source.server != actor.origin
                        || authority.birth.source.generation == 0
                        || authority.birth.uuid.is_nil()
                        || !property.accepts(value)
                        || matches!(value, crate::vehicle_property::VehiclePropertyValue::Tick(tick) if *tick != Some(self.tick))
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::MoveEntityBody {
                    source,
                    destination,
                    spatial,
                    authority,
                } => {
                    let (
                        Target::Chunk {
                            key: old,
                            part: ChunkPart::Entity(id),
                            inventory: None,
                        },
                        Target::Chunk {
                            key: next,
                            part: ChunkPart::Entity(next_id),
                            inventory: None,
                        },
                    ) = (*source, *destination)
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if self.class != ActionClass::Infallible
                        || !matches!(
                            self.kind,
                            ActionKind::Actor | ActionKind::PlayerAdministration
                        )
                        || self.effects.len() != 1
                        || self.chunk_transition != (old != next)
                        || (self.kind == ActionKind::Actor && (self.id.family != 8 || old == next))
                        || (self.kind == ActionKind::PlayerAdministration
                            && (self.id.family != 7
                                || authority.as_ref().is_none_or(|authority| {
                                    authority.issuer != authority.body_owner
                                        || authority.issuer.generation == 0
                                        || authority.birth.source.server != id.origin
                                })))
                        || id != next_id
                        || self.id.actor != ActorId::Entity(id)
                        || self.id.incarnation != u64::from(id.generation)
                        || self.actor_chunk != Some(old)
                        || !spatial.belongs_to(next)
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::MovePlayerAvatar {
                    source,
                    destination,
                    avatar,
                    velocity_micro: _,
                } => {
                    let (
                        Target::Chunk {
                            key: old_chunk,
                            part: ChunkPart::Player(old_actor),
                            inventory: None,
                        },
                        Target::Chunk {
                            key: new_chunk,
                            part: ChunkPart::Player(new_actor),
                            inventory: None,
                        },
                    ) = (source, destination)
                    else {
                        return Err(ApplyFailure::Type(*source));
                    };
                    let position = avatar.position.map(f64::from_bits);
                    if self.class != ActionClass::Infallible
                        || !matches!(
                            self.kind,
                            ActionKind::PlayerAdministration | ActionKind::Actor
                        )
                        || (self.kind == ActionKind::Actor
                            && (self.id.family != 8 || old_chunk == new_chunk))
                        || self.effects.len() != 1
                        || old_actor != new_actor
                        || self.id.actor != ActorId::Player(*old_actor)
                        || self.actor_chunk != Some(*old_chunk)
                        || self.chunk_transition != (old_chunk != new_chunk)
                        || !avatar.is_valid()
                        || avatar.world != new_chunk.world
                        || (position[0].floor() as i32).div_euclid(16) != new_chunk.x()
                        || (position[2].floor() as i32).div_euclid(16) != new_chunk.z()
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::SetPlayerCamera { target, camera } => {
                    let Target::Chunk {
                        part: ChunkPart::Player(gid),
                        inventory: None,
                        ..
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.id.actor != ActorId::Player(*gid) || self.chunk_transition {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if let Some(camera) = camera {
                        let authority = match &camera.identity {
                            crate::item_contact::ContactIdentity::Player(lease) => self.player_leases.contains(lease),
                            crate::item_contact::ContactIdentity::Entity { birth, .. } => self.conditions.iter().any(|condition| {
                                condition.target == camera.target() && matches!(&condition.expected,
                                    Expected::EntityBirthOwned { birth: observed, owner } if observed == birth && *owner == camera.owner)
                            }),
                        };
                        if !camera.valid()
                            || !authority
                            || !self.conditions.iter().any(|condition| {
                                condition.target == camera.target()
                                    && condition.expected
                                        == Expected::SpatialEpochEquals(camera.epoch)
                            })
                        {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                }
                Effect::SetPlayerSleeping { target, sleeping } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Player(gid),
                        inventory: None,
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.id.actor != ActorId::Player(*gid) || self.chunk_transition {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let prior =
                        self.conditions
                            .iter()
                            .find_map(|condition| match &condition.expected {
                                Expected::Exact(Cell {
                                    value: Value::Living(living),
                                    ..
                                }) if condition.target == *target => living
                                    .avatar
                                    .as_ref()
                                    .and_then(|avatar| avatar.presentation.sleeping),
                                _ => None,
                            });
                    for bed in sleeping.iter().chain(prior.iter()).map(|state| state.bed) {
                        let bed_target = Target::Chunk {
                            key: ChunkKey::new(
                                key.world,
                                bed.x.div_euclid(16),
                                bed.z.div_euclid(16),
                            ),
                            part: ChunkPart::BlockEntity(bed),
                            inventory: None,
                        };
                        if !self
                            .conditions
                            .iter()
                            .any(|condition| condition.target == bed_target)
                            || !self
                                .effects
                                .iter()
                                .any(|effect| effect.targets().any(|target| target == bed_target))
                        {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                }
                Effect::SetPlayerRespawn { target, respawn } => {
                    let Target::Chunk {
                        part: ChunkPart::Player(gid),
                        inventory: None,
                        ..
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.id.actor != ActorId::Player(*gid)
                        || self.chunk_transition
                        || respawn.is_some_and(|state| !f32::from_bits(state.yaw).is_finite())
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::PlayerAvatarLifecycle { target, intent } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Player(gid),
                        inventory: None,
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if *gid != intent.lease().gid
                        || !intent.valid(self.id, self.tick, *key)
                        || self.class != ActionClass::Infallible
                        || self.chunk_transition
                        || self.effects.len() != 1
                        || self.player_leases.as_slice() != [intent.lease()]
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    match intent {
                        crate::avatar_lifecycle::AvatarLifecycleIntent::Admission { .. }
                            if !self.conditions.is_empty() =>
                        {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        _ => {}
                    }
                }
                Effect::RespawnPlayerAvatar {
                    source,
                    destination,
                    avatar,
                    max_health_attribute: _,
                } => {
                    let (
                        Target::Chunk {
                            key: old,
                            part: ChunkPart::Player(actor),
                            inventory: None,
                        },
                        Target::Chunk {
                            key: next,
                            part: ChunkPart::Player(next_actor),
                            inventory: None,
                        },
                    ) = (source, destination)
                    else {
                        return Err(ApplyFailure::Type(*source));
                    };
                    let position = avatar.position.map(f64::from_bits);
                    let dead = self.conditions.iter().any(|condition| {
                        matches!((&condition.target, &condition.expected),
                            (Target::Chunk { key, part: ChunkPart::LivingProperty { actor: guarded_actor, property: crate::living_property::LivingProperty::Health }, inventory: None },
                             Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(0)), .. }))
                            if key == old && *guarded_actor == self.id.actor)
                    });
                    let death_cause = self.conditions.iter().any(|condition| {
                        matches!((&condition.target, &condition.expected),
                            (Target::Chunk { key, part: ChunkPart::LivingProperty { actor: guarded_actor, property: crate::living_property::LivingProperty::DeathCause }, inventory: None },
                             Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::ActionReference(Some(_))), .. }))
                            if key == old && *guarded_actor == self.id.actor)
                    });
                    let semantic_respawn = matches!(&self.semantic_intent, Some(SemanticIntent::Respawn(intent)) if intent.guards_valid(self));
                    let fixed_ready = self.conditions.iter().any(|guard| matches!((&guard.target, &guard.expected),
                        (Target::Chunk { key, part: ChunkPart::LivingProperty { actor: guarded_actor, property: crate::living_property::LivingProperty::DeathInventoryProcessed }, inventory: None },
                         Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Boolean(true)), .. }))
                        if key == old && *guarded_actor == self.id.actor))
                        && self.conditions.iter().any(|guard| matches!((&guard.target, &guard.expected),
                            (Target::Chunk { key, part: ChunkPart::LivingProperty { actor: guarded_actor, property: crate::living_property::LivingProperty::MenuSession }, inventory: None },
                             Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::MenuSession(menu)), .. }))
                            if key == old && *guarded_actor == self.id.actor && menu.session == 0 && menu.valid()))
                        && self.conditions.iter().any(|guard| matches!((&guard.target, &guard.expected),
                            (Target::Chunk { key, part: ChunkPart::ActorRelation { actor: guarded_actor, relation: crate::actor_relation::ActorRelation::Mount }, inventory: None },
                             Expected::Exact(Cell { value: Value::ActorRelation(None), .. }))
                            if key == old && *guarded_actor == self.id.actor))
                        && self.conditions.iter().any(|guard| matches!((&guard.target, &guard.expected),
                            (Target::Chunk { key, part: ChunkPart::LivingProperty { actor: guarded_actor, property: crate::living_property::LivingProperty::DeathCause }, inventory: None },
                             Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::ActionReference(Some(cause))), .. }))
                            if key == old && *guarded_actor == self.id.actor && self.dependencies.contains(cause)));

                    if !(self.class == ActionClass::Undoable || self.class == ActionClass::Replayable && semantic_respawn)
                        || !dead
                        || !death_cause
                        || !self.conditions.iter().any(|condition| {
                            condition.target == *source
                                && matches!(condition.expected, Expected::SpatialEpochEquals(_))
                        })
                        || self.kind != ActionKind::PlayerAdministration
                        || (!semantic_respawn && (self.effects.len() != 1 || !fixed_ready))
                        || self.id.actor != ActorId::Player(*actor)
                        || actor != next_actor
                        || self.actor_chunk != Some(*old)
                        || self.chunk_transition != (old != next)
                        || !avatar.is_valid()
                        || avatar.world != next.world
                        || (position[0].floor() as i32).div_euclid(16) != next.x()
                        || (position[2].floor() as i32).div_euclid(16) != next.z()
                        || avatar.spatial_epoch
                            != SpatialEpoch::Action(ActionRef {
                                tick: self.tick,
                                id: self.id,
                            })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::ResetPlayerLiving { target, template } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Player(actor),
                        inventory: None,
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if self.class != ActionClass::Infallible
                        || self.kind != ActionKind::PlayerAdministration
                        || self.effects.len() != 1
                        || self.id.actor != ActorId::Player(*actor)
                        || self.actor_chunk != Some(*key)
                        || self.chunk_transition
                        || !template.belongs_to(self.id.actor)
                        || template.player_administrative_state().is_none()
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::Add { target, .. }
                    if !matches!(
                        target,
                        Target::Player {
                            inventory: None,
                            ..
                        }
                    ) && !matches!(target, Target::Chunk { part: ChunkPart::LivingProperty {
                        actor: ActorId::Player(gid), property: crate::living_property::LivingProperty::Statistic { category: 0..=8, key: _ }
                    }, inventory: None, .. } if ActorId::Player(*gid) == self.id.actor) =>
                {
                    return Err(ApplyFailure::Type(*target));
                }
                Effect::ScheduleTick { target, tick } => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Whole,
                        inventory: None,
                    } = target
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if !matches!(tick.origin, ScheduledTickOrigin::Action { cause, producer }
                        if cause == ActionRef { tick: self.tick, id: self.id }
                            && producer.generation != 0)
                        || tick.executor.generation == 0
                        || self.chunk_transition
                        || tick.position.x.div_euclid(16) != key.x()
                        || tick.position.z.div_euclid(16) != key.z()
                        || !(-3..=3).contains(&tick.priority)
                        || tick.due.distance_since(self.tick) == 0
                        || tick.due.distance_since(self.tick) > 256
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::CompleteScheduledTick { target, fact } => {
                    if !matches!(target, Target::Chunk { key, part: ChunkPart::Whole, inventory: None }
                        if fact.position.x.div_euclid(16) == key.x() && fact.position.z.div_euclid(16) == key.z())
                        || fact.due.is_after(self.tick)
                        || self.chunk_transition
                        || !self.conditions.iter().any(|condition| {
                            condition.target == *target
                                && condition.expected
                                    == Expected::ScheduledTickFact {
                                        fact: *fact,
                                        present: true,
                                    }
                        })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::DrainScheduledTicks { target, through } => {
                    if !matches!(
                        target,
                        Target::Chunk {
                            part: ChunkPart::Whole,
                            inventory: None,
                            ..
                        }
                    ) || self.class != ActionClass::Infallible
                        || self.kind != ActionKind::ScheduledTick
                        || self.effects.len() != 1
                        || !self.conditions.is_empty()
                        || self.chunk_transition
                        || *through != self.tick
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                Effect::Delete { target }
                    if matches!(
                        target,
                        Target::Chunk {
                            part: ChunkPart::Block(_),
                            inventory: None,
                            ..
                        } | Target::Chunk {
                            inventory: Some(_),
                            ..
                        } | Target::Player {
                            inventory: Some(_),
                            ..
                        }
                    ) =>
                {
                    return Err(ApplyFailure::Type(*target));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub fn apply_action<S: ActionState>(
    owners: &mut S,
    action: &Action,
    view: TruthView,
) -> Result<AppliedAction, ApplyFailure> {
    let applied = prepare_action(owners, action, view)?;
    owners.commit(
        ActionContext {
            id: applied.id,
            tick: applied.tick,
            actor_chunk: applied.actor_chunk,
        },
        view,
        &applied.writes,
    )?;
    Ok(applied)
}

pub fn prepare_action<S: ActionState>(
    owners: &S,
    action: &Action,
    view: TruthView,
) -> Result<AppliedAction, ApplyFailure> {
    action.validate()?;
    if action.conditions.iter().any(|condition| matches!(condition.expected, Expected::IssuerEntityAdmission(_))) {
        crate::issuer_entity_admission::witness(action).ok_or(ApplyFailure::InvalidAction)?.validate_context(owners, action)?;
    }
    if let Some(witness) = crate::entity_placement::witness(action) { witness.validate_context(owners, action)?; }
    if let Some(witness) = crate::object_tick::terminal_witness(action) {
        witness.validate_context(owners, action)?;
    }
    if action
        .player_leases
        .iter()
        .any(|lease| !owners.validate_live_player_lease(*lease))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    for target in action
        .conditions
        .iter()
        .map(|condition| condition.target)
        .chain(action.effects.iter().flat_map(Effect::targets))
    {
        if !owners.available(view, target) {
            return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
        }
    }
    for condition in &action.conditions {
        if !condition
            .expected
            .matches(owners.read(view, condition.target).as_ref())
        {
            return Err(ApplyFailure::Condition(condition.target));
        }
    }
    if let Some(SemanticIntent::CreativeInventory(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if intent.effects(owners, &values)? != action.effects {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::InventoryGestures(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if intent
            .derive_effects_with_context(&values, owners)
            .map_err(|_| ApplyFailure::InvalidAction)?
            != action.effects
        {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::MergeDroppedItems { contact, max_stack }) = &action.semantic_intent
    {
        let values = [contact.source.target(), contact.destination.target()]
            .into_iter()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if crate::item_lifecycle::derive_merge(contact, *max_stack, &values)? != action.effects {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(
        SemanticIntent::Pickup { inventory, .. } | SemanticIntent::InventoryMove(inventory),
    ) = &action.semantic_intent
    {
        let values = inventory
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if inventory
            .derive_effects_with_context(&values, owners)
            .map_err(|_| ApplyFailure::InvalidAction)?
            != action.effects
        {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::World(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive_effects_with(&values, owners)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::MergeExperience(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .into_iter()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(&values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::ExperienceDamage(intent)) = &action.semantic_intent {
        let values = owners
            .read(view, intent.target)
            .map(|cell| [(intent.target, cell)].into_iter().collect())
            .ok_or(ApplyFailure::Condition(intent.target))?;
        if action.effects != intent.derive(&values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::CollectExperience(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(owners, action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::DeathInventory(intent)) = &action.semantic_intent {
        let values = intent.targets().into_iter().filter_map(|target| owners.read(view, target).map(|cell| (target, cell))).collect();
        if action.effects != intent.derive(owners, action.id, action.tick, &values)? { return Err(ApplyFailure::InvalidAction); }
    }
    if let Some(SemanticIntent::ProjectileTick(intent)) = &action.semantic_intent {
        let values = intent.targets().into_iter().filter_map(|target| owners.read(view, target).map(|cell| (target, cell))).collect();
        if action.effects != intent.derive(owners, action.id, action.tick, &values)? { return Err(ApplyFailure::InvalidAction); }
    }
    if let Some(SemanticIntent::DropItem(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(owners, action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::VehicleTick(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .into_iter()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive_with(owners, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::ItemUse(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(owners, action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::ObjectTick(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .into_iter()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::Object(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::Respawn(intent)) = &action.semantic_intent {
        let values = intent.targets()?.into_iter().filter_map(|target| owners.read(view, target).map(|cell| (target, cell))).collect();
        if action.effects != intent.derive(owners, action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::MenuSessionChange(intent)) = &action.semantic_intent {
        let values = intent.targets().into_iter().filter_map(|target|
            owners.read(view, target).map(|cell| (target, cell))).collect();
        if action.effects != intent.derive(&values)? { return Err(ApplyFailure::InvalidAction); }
    }
    if let Some(SemanticIntent::Wake(intent)) = &action.semantic_intent {
        intent.validate_beds(owners)?;
        if let Some(morning) = intent.morning { owners.validate_morning(intent.chunk.world, morning, ActionRef { id: action.id, tick: action.tick })?; }
        if action.effects != intent.effects(action.id, action.tick)? { return Err(ApplyFailure::InvalidAction); }
    }
    if let Some(SemanticIntent::ActorTags(intent)) = &action.semantic_intent {
        let values = intent.targets().into_iter().filter_map(|target| owners.read(view, target).map(|cell| (target, cell))).collect();
        if action.effects != intent.derive(action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::MobBreeding(intent)) = &action.semantic_intent {
        let values = intent
            .resolution_targets()
            .into_iter()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive(owners, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if let Some(SemanticIntent::Combat(intent)) = &action.semantic_intent {
        let values = intent
            .targets()
            .filter_map(|target| owners.read(view, target).map(|cell| (target, cell)))
            .collect();
        if action.effects != intent.derive_with(owners, action.id, action.tick, &values)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let mut staged = BTreeMap::<Target, Option<Cell>>::new();
    for effect in &action.effects {
        for target in effect.targets() {
            if matches!(target, Target::Chunk { part: ChunkPart::ActorOccupancy, inventory: None, .. }) { continue; }
            staged
                .entry(target)
                .or_insert_with(|| owners.read(view, target));
        }
    }
    let before = staged.clone();
    let body_kinds = before
        .values()
        .filter_map(|cell| match cell {
            Some(Cell {
                value: Value::Entity(body),
                ..
            }) => Some((body.id, body.as_ref())),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut sanctioned_inventory_delta = if let Some(intent) = crate::vehicle_terminal::witness(action) {
        intent.validate_context(owners, action.tick)?
    } else {
        BTreeMap::<(u16, Vec<u8>), i64>::new()
    };
    for effect in &action.effects {
        match effect {
            effect @ Effect::EntityAdmission { .. } => prepare_entity_admission_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::CreativeItems { .. } => prepare_creative_items_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            Effect::MenuVirtualInventoryAdmission { admission } => {
                for (target, cell) in admission.entries() {
                    if staged.get(&target).is_some_and(Option::is_some) { return Err(ApplyFailure::Condition(target)); }
                    staged.insert(target, Some(Cell { revision: 0, value: Value::Inventory(cell) }));
                }
            }
            effect @ Effect::TickBlockInventory { .. } => prepare_tick_block_inventory_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::TransformMenuInventory { .. } => prepare_transform_menu_inventory_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::TradeItems { .. } => prepare_trade_items_effect(owners, action, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::CraftItems { .. } => prepare_craft_items_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::CloseInventory { .. } => prepare_close_inventory_effect(owners, action, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::CrossbowItems { .. } => prepare_crossbow_items_effect(owners, action, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::CollectProjectile { .. } => prepare_collect_projectile_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::ProjectileShooterTeleport { .. } => prepare_projectile_shooter_teleport_effect(action, effect, &mut staged)?,
            effect @ Effect::TransformItems { .. } => prepare_transform_items_effect(owners, action, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::Put { .. } => prepare_put_effect(owners, action, effect, &mut staged, &body_kinds)?,
            effect @ Effect::PlayerAvatarLifecycle { .. } => prepare_player_avatar_lifecycle_effect(owners, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            effect @ Effect::RetireEntity { .. } => prepare_retire_entity_effect(owners, action, effect, &mut staged, &mut sanctioned_inventory_delta)?,
            Effect::Delete { target } => {
                staged.insert(*target, None);
            }
            effect @ Effect::MovePlayerAvatar { .. } => prepare_move_player_avatar_effect(effect, &mut staged)?,
            Effect::SetPlayerCamera { target, .. }
            | Effect::SetPlayerSleeping { target, .. }
            | Effect::SetPlayerRespawn { target, .. } => {
                let Some(Some(Cell {
                    value: Value::Living(current),
                    ..
                })) = staged.get(target)
                else {
                    return Err(ApplyFailure::Type(*target));
                };
                let mut next = current.clone();
                let avatar = next.avatar.as_mut().ok_or(ApplyFailure::Type(*target))?;
                match effect {
                    Effect::SetPlayerCamera { camera, .. } => {
                        avatar.presentation.camera_target = camera.clone()
                    }
                    Effect::SetPlayerSleeping { sleeping, .. } => {
                        avatar.presentation.sleeping = *sleeping
                    }
                    Effect::SetPlayerRespawn { respawn, .. } => {
                        avatar.presentation.respawn = *respawn
                    }
                    _ => unreachable!(),
                }
                let revision = next_revision(*target, staged.get(target).and_then(Option::as_ref))?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::Living((next).into()),
                    }),
                );
            }
            effect @ Effect::RespawnPlayerAvatar { .. } => prepare_respawn_player_avatar_effect(owners, effect, &mut staged)?,
            effect @ Effect::ResetPlayerLiving { .. } => prepare_reset_player_living_effect(action, effect, &mut staged)?,
            Effect::Add { target, amount } => {
                let Some(Some(current)) = staged.get(target) else {
                    return Err(ApplyFailure::Type(*target));
                };
                let value = match &current.value {
                    Value::Scalar(value) => Value::Scalar(
                        value
                            .checked_add(*amount)
                            .ok_or(ApplyFailure::Overflow(*target))?,
                    ),
                    Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::Unsigned(value),
                    ) if *amount >= 0
                        && matches!(
                            target,
                            Target::Chunk {
                                part: ChunkPart::LivingProperty {
                                    property: crate::living_property::LivingProperty::Statistic { .. },
                                    ..
                                },
                                ..
                            }
                        ) =>
                    {
                        Value::LivingProperty(
                            crate::living_property::LivingPropertyValue::Unsigned(
                                i64::from(*value)
                                    .saturating_add(*amount)
                                    .min(i64::from(i32::MAX))
                                    as u32,
                            ),
                        )
                    }
                    _ => return Err(ApplyFailure::Type(*target)),
                };
                let revision = next_revision(*target, Some(current))?;
                staged.insert(*target, Some(Cell { revision, value }));
            }
            Effect::ScheduleTick { target, tick } => {
                let current = staged.get(target).and_then(Option::as_ref);
                let mut queue = match current.map(|cell| &cell.value) {
                    Some(Value::ScheduledQueue(queue)) => queue.clone(),
                    None => Vec::new(),
                    _ => return Err(ApplyFailure::Type(*target)),
                };
                if queue.iter().any(|entry| {
                    entry.kind == tick.kind
                        && entry.id == tick.id
                        && entry.position == tick.position
                }) {
                    continue;
                }
                queue.push(*tick);
                queue.sort_by_key(|entry| {
                    (
                        entry.due,
                        entry.priority,
                        entry.source_order,
                        entry.kind,
                        entry.position,
                        entry.id,
                        entry.origin,
                    )
                });
                let revision = next_revision(*target, current)?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::ScheduledQueue(queue),
                    }),
                );
            }
            Effect::CompleteScheduledTick { target, fact } => {
                let current = staged.get(target).and_then(Option::as_ref);
                let Some(Cell {
                    value: Value::ScheduledQueue(queue),
                    ..
                }) = current
                else {
                    return Err(ApplyFailure::Type(*target));
                };
                if !queue.contains(fact) {
                    return Err(ApplyFailure::Condition(*target));
                }
                let remaining = queue
                    .iter()
                    .copied()
                    .filter(|entry| entry != fact)
                    .collect();
                let revision = next_revision(*target, current)?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::ScheduledQueue(remaining),
                    }),
                );
            }
            Effect::DrainScheduledTicks { target, through } => {
                let current = staged.get(target).and_then(Option::as_ref);
                let Some(Cell {
                    value: Value::ScheduledQueue(queue),
                    ..
                }) = current
                else {
                    return Err(ApplyFailure::Type(*target));
                };
                let remaining = queue
                    .iter()
                    .copied()
                    .filter(|tick| tick.due.is_after(*through))
                    .collect::<Vec<_>>();
                if remaining.len() == queue.len() {
                    continue;
                }
                let revision = next_revision(*target, current)?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::ScheduledQueue(remaining),
                    }),
                );
            }
            Effect::MergeDroppedItems {
                source,
                destination,
                max_stack,
                count,
            } => {
                let Some(source_cell) = staged.get(source).and_then(Option::as_ref) else {
                    return Err(ApplyFailure::Type(*source));
                };
                let Some(destination_cell) = staged.get(destination).and_then(Option::as_ref)
                else {
                    return Err(ApplyFailure::Type(*destination));
                };
                let Value::Entity(source_body) = &source_cell.value else {
                    return Err(ApplyFailure::Type(*source));
                };
                let Value::Entity(destination_body) = &destination_cell.value else {
                    return Err(ApplyFailure::Type(*destination));
                };
                let crate::entity_body::EntityPayload::Item(source_item) = &source_body.payload
                else {
                    return Err(ApplyFailure::Type(*source));
                };
                if source_body.id == destination_body.id
                    || source_body.birth.uuid == destination_body.birth.uuid
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let mut next = destination_body.clone();
                let crate::entity_body::EntityPayload::Item(destination_item) = &mut next.payload
                else {
                    return Err(ApplyFailure::Type(*destination));
                };
                let mut consumed = source_item.clone();
                consumed.count = *count;
                if *count > source_item.count || !destination_item.merge_from(&consumed, *max_stack)
                {
                    return Err(ApplyFailure::InventoryConservation);
                }
                let revision = next_revision(*destination, Some(destination_cell))?;
                let remainder = if *count == source_item.count {
                    None
                } else {
                    let mut remaining = source_body.clone();
                    let crate::entity_body::EntityPayload::Item(item) = &mut remaining.payload
                    else {
                        unreachable!();
                    };
                    item.count -= *count;
                    Some(Cell {
                        revision: next_revision(*source, Some(source_cell))?,
                        value: Value::Entity((remaining).into()),
                    })
                };
                staged.insert(*source, remainder);
                staged.insert(
                    *destination,
                    Some(Cell {
                        revision,
                        value: Value::Entity((next).into()),
                    }),
                );
            }
            Effect::AdvanceExperienceLifetime {
                target,
                owner,
                birth,
            } => {
                let Some(current) = staged.get(target).and_then(Option::as_ref) else {
                    continue;
                };
                let Value::Entity(body) = &current.value else {
                    return Err(ApplyFailure::Type(*target));
                };
                if &body.birth != birth || body.owner != *owner {
                    continue;
                }
                let crate::entity_body::EntityPayload::Experience(orb) = &body.payload else {
                    return Err(ApplyFailure::Type(*target));
                };
                let previous = orb.last_lifetime_tick.unwrap_or(birth.action.tick);
                if !action.tick.is_after(previous) {
                    continue;
                }
                let mut next = body.clone();
                let crate::entity_body::EntityPayload::Experience(after) = &mut next.payload else {
                    unreachable!();
                };
                after.age = after
                    .age
                    .saturating_add(u32::from(action.tick.distance_since(previous)));
                after.last_lifetime_tick = Some(action.tick);
                let cell = if after.age >= 6000 {
                    None
                } else {
                    Some(Cell {
                        revision: next_revision(*target, Some(current))?,
                        value: Value::Entity((next).into()),
                    })
                };
                staged.insert(*target, cell);
            }
            Effect::ChangeDroppedItem {
                target,
                issuer,
                body_owner,
                birth,
                change,
            } => {
                let Some(current) = staged.get(target).and_then(Option::as_ref) else {
                    if *change == crate::entity_body::DroppedItemChange::AdvanceLifetime {
                        continue;
                    }
                    return Err(ApplyFailure::Type(*target));
                };
                let Value::Entity(body) = &current.value else {
                    return Err(ApplyFailure::Type(*target));
                };
                let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(id),
                    inventory: None,
                } = *target
                else {
                    return Err(ApplyFailure::Type(*target));
                };
                if !body.belongs_to(id, key) || &body.birth != birth {
                    return Err(ApplyFailure::Type(*target));
                }
                if body.owner != *body_owner {
                    if *change == crate::entity_body::DroppedItemChange::AdvanceLifetime {
                        continue;
                    }
                    return Err(ApplyFailure::Type(*target));
                }
                if matches!(
                    change,
                    crate::entity_body::DroppedItemChange::AdvanceLifetime
                        | crate::entity_body::DroppedItemChange::Expire
                ) && body.owner != *issuer
                    && !(action.id.actor == ActorId::Entity(id)
                        && action.id.incarnation == u64::from(id.generation))
                {
                    continue;
                }
                let mut next = body.clone();
                let crate::entity_body::EntityPayload::Item(item) = &mut next.payload else {
                    return Err(ApplyFailure::Type(*target));
                };
                if *change == crate::entity_body::DroppedItemChange::AdvanceLifetime {
                    if item
                        .last_lifetime_tick
                        .is_some_and(|tick| tick == action.tick || tick.is_after(action.tick))
                    {
                        continue;
                    }
                    let previous = item.last_lifetime_tick.unwrap_or(body.birth.action.tick);
                    if !action.tick.is_after(previous) {
                        continue;
                    }
                    let extra = action.tick.distance_since(previous).saturating_sub(1);
                    if !item.never_pickup {
                        item.pickup_delay = item.pickup_delay.saturating_sub(extra);
                    }
                    if !item.never_despawn {
                        item.age = item.age.saturating_add(u32::from(extra));
                    }
                    item.last_lifetime_tick = Some(action.tick);
                }
                let alive = item.apply_change(*change);
                if alive && next == *body {
                    continue;
                }
                let replacement = if alive {
                    Some(Cell {
                        revision: next_revision(*target, Some(current))?,
                        value: Value::Entity((next).into()),
                    })
                } else {
                    None
                };
                staged.insert(*target, replacement);
            }
            Effect::UpdateMobContinuation {
                target,
                authority,
                brain,
            } => {
                let Some((body_target, _)) = crate::living_property::body_target(*target) else {
                    return Err(ApplyFailure::Type(*target));
                };
                let Some(Cell {
                    value: Value::Entity(body),
                    ..
                }) = owners.read(view, body_target)
                else {
                    continue;
                };
                if body.birth != authority.birth || body.owner != authority.body_owner {
                    continue;
                }
                let crate::entity_body::EntityPayload::Living(living) = &body.payload else {
                    continue;
                };
                let Some(mob) = &living.mob else {
                    continue;
                };
                if mob
                    .brain
                    .processed_tick
                    .is_some_and(|previous| !action.tick.is_after(previous))
                {
                    continue;
                }
                let Some(current) = staged.get(target).and_then(Option::as_ref) else {
                    continue;
                };
                if !matches!(
                    &current.value,
                    Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::MobContinuation(Some(_))
                    )
                ) {
                    continue;
                }
                let revision = next_revision(*target, Some(current))?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::LivingProperty(
                            crate::living_property::LivingPropertyValue::MobContinuation(Some(
                                brain.clone(),
                            )),
                        ),
                    }),
                );
            }
            Effect::UpdateVehicleProperty {
                target,
                authority,
                value,
            } => {
                let Some((body_target, property)) = crate::vehicle_property::body_target(*target)
                else {
                    return Err(ApplyFailure::Type(*target));
                };
                let Some(Cell {
                    value: Value::Entity(body),
                    ..
                }) = owners.read(view, body_target)
                else {
                    continue;
                };
                if body.birth != authority.birth || body.owner != authority.body_owner {
                    continue;
                }
                let crate::entity_body::EntityPayload::Vehicle(_) = &body.payload else {
                    continue;
                };
                let Some(current) = staged.get(target).and_then(Option::as_ref) else {
                    continue;
                };
                if current.value == Value::VehicleProperty(value.clone()) {
                    continue;
                }
                let mut proof = Cell {
                    revision: 0,
                    value: Value::Entity((body.clone()).into()),
                };
                if !crate::vehicle_property::patch_cell(
                    &mut proof,
                    property,
                    &Cell {
                        revision: 1,
                        value: Value::VehicleProperty(value.clone()),
                    },
                ) {
                    return Err(ApplyFailure::Type(*target));
                }
                let revision = next_revision(*target, Some(current))?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::VehicleProperty(value.clone()),
                    }),
                );
            }
            effect @ Effect::MoveEntityBody { .. } => prepare_move_entity_body_effect(action, effect, &mut staged)?,
            effect @ Effect::MoveItems { .. } => prepare_move_items_effect(owners, action, effect, &mut staged)?,
        }
    }
    let mut balance = BTreeMap::<(u16, Vec<u8>), i64>::new();
    let includes_inventory = staged.keys().any(|target| {
        matches!(
            target,
            Target::Chunk {
                inventory: Some(_),
                ..
            } | Target::Player {
                inventory: Some(_),
                ..
            }
        )
    });
    if includes_inventory {
        for (target, after) in &staged {
            if matches!(
                target,
                Target::Player {
                    inventory: Some(InventoryId::Player {
                        inv: crate::inventory::INV_SELECTED,
                        slot: 0
                    }),
                    ..
                }
            ) {
                if !after.as_ref().is_some_and(|cell| matches!(&cell.value, Value::Inventory(value)
                    if value.loc == crate::inventory::InvLoc::new(crate::inventory::INV_SELECTED, 0)
                        && (1..=9).contains(&value.item) && value.count == 1 && value.nbt.is_empty())) {
                    return Err(ApplyFailure::Inventory(*target));
                }
                continue;
            }
            for (cell, sign) in [
                (before.get(target).and_then(Option::as_ref), -1_i64),
                (after.as_ref(), 1_i64),
            ] {
                let Some(cell) = cell else {
                    continue;
                };
                let (item, components, count) = match &cell.value {
                    Value::Inventory(item) if !item.is_empty() => {
                        (item.item, &item.nbt, item.count)
                    }
                    Value::Entity(body) => match &body.payload {
                        crate::entity_body::EntityPayload::Item(item) => (item.item, &item.components, item.count),
                        _ => continue,
                    },
                    _ => continue,
                };
                *balance.entry((item, components.clone())).or_default() += sign * i64::from(count);
            }
        }
    }
    for (identity, sanctioned) in sanctioned_inventory_delta {
        *balance.entry(identity).or_default() -= sanctioned;
    }
    if balance.values().any(|amount| *amount != 0) {
        return Err(ApplyFailure::InventoryConservation);
    }
    let mut writes = Vec::with_capacity(staged.len());
    for (target, mut after) in staged {
        let previous = before.get(&target).cloned().flatten();
        if let Some(cell) = &mut after {
            let relocation_source = action.effects.iter().find_map(|effect| match effect {
                Effect::ProjectileShooterTeleport { intent } if intent.destination_target() == target
                    && intent.shooter.target() != target => Some(intent.shooter.target()),
                Effect::MovePlayerAvatar {
                    source,
                    destination,
                    ..
                }
                | Effect::RespawnPlayerAvatar {
                    source,
                    destination,
                    ..
                }
                | Effect::MoveEntityBody {
                    source,
                    destination,
                    ..
                } if *destination == target && *source != target => Some(*source),
                _ => None,
            });
            let relocation_before = relocation_source
                .and_then(|source| before.get(&source))
                .and_then(Option::as_ref);
            crate::derived_living::normalize_body_versions(
                target,
                previous.as_ref().or(relocation_before),
                cell,
            )?;
        }
        if previous == after {
            continue;
        }
        writes.push(AppliedWrite {
            target,
            before: previous,
            after,
        });
    }
    let species_bodies = writes
        .iter()
        .filter_map(|write| match write.target {
            Target::Chunk {
                part:
                    ChunkPart::LivingProperty {
                        property:
                            crate::living_property::LivingProperty::MobSpecies
                            | crate::living_property::LivingProperty::MobSpeciesProperty(_),
                        ..
                    },
                ..
            } => crate::derived_living::actor_body_target(write.target),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for body in species_bodies {
        let original = owners
            .read(view, body)
            .ok_or(ApplyFailure::Condition(body))?;
        crate::derived_living::normalize_species_writes(body, &original, &mut writes)?;
    }
    let mut occupancy_cuts = BTreeMap::new();
    for effect in &action.effects {
        let Effect::RespawnPlayerAvatar { source, destination, avatar, .. } = effect else { continue; };
        let Target::Chunk { part: ChunkPart::Player(gid), .. } = source else {
            return Err(ApplyFailure::Type(*source));
        };
        if !matches!(destination, Target::Chunk { part: ChunkPart::Player(actual), .. } if actual == gid) {
            return Err(ApplyFailure::Type(*destination));
        }
        let original = owners.read(view, *source).ok_or(ApplyFailure::Condition(*source))?;
        let (owner, _) = owners.actor_occupancy_cut(*source, &original.value)?;
        let cut = crate::actor_occupancy::ActorOccupancyCut { position: avatar.position,
            sample_tick: avatar.movement_tick, simulation_sequence: avatar.movement_sequence };
        if !cut.valid() || occupancy_cuts.insert(ActorId::Player(*gid), (owner, cut)).is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    crate::derived_actor_occupancy::normalize_with_cuts(owners, view, &mut writes, &occupancy_cuts)?;

    Ok(AppliedAction {
        id: action.id,
        tick: action.tick,
        actor_chunk: action.actor_chunk,
        view,
        writes,
    })
}

#[inline(never)]
fn prepare_creative_items_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::CreativeItems { intent } => {
                let values = staged
                    .iter()
                    .filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell)))
                    .collect();
                for (target, after) in intent.derive(owners, &values)? {
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if let Some(Cell {
                        value: Value::Inventory(item),
                        ..
                    }) = previous
                    {
                        if !item.is_empty() {
                            *sanctioned_inventory_delta
                                .entry((item.item, item.nbt.clone()))
                                .or_default() -= i64::from(item.count);
                        }
                    }
                    if !after.is_empty() {
                        *sanctioned_inventory_delta
                            .entry((after.item, after.nbt.clone()))
                            .or_default() += i64::from(after.count);
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(
                        target,
                        Some(Cell {
                            revision,
                            value: Value::Inventory(after),
                        }),
                    );
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_tick_block_inventory_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::TickBlockInventory { intent } => {
                let values = staged.iter().filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell))).collect();
                for (target, after) in intent.derive_writes(owners, &values)?.writes {
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if intent.kind != crate::canonical_block_inventory_tick::BlockInventoryTickKind::Hopper {
                        for (value, sign) in [(previous.map(|cell| &cell.value), -1_i64), (after.as_ref(), 1_i64)] {
                            if let Some(value) = value && let Some(item) = transferable_item(value, true) && !item.is_empty() {
                                *sanctioned_inventory_delta.entry((item.item, item.nbt.clone())).or_default() += sign * i64::from(item.count);
                            }
                        }
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(target, after.map(|value| Cell { revision, value }));
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_transform_menu_inventory_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::TransformMenuInventory { intent } => {
                let values = staged.iter().filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell))).collect();
                for (target, after) in intent.derive(owners, &values)? {
                    if !after.is_empty() && after.count > owners.inventory_limit(&after, target)? { return Err(ApplyFailure::Inventory(target)); }
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if let Some(Cell { value: Value::Inventory(before), .. }) = previous && !before.is_empty() {
                        *sanctioned_inventory_delta.entry((before.item, before.nbt.clone())).or_default() -= i64::from(before.count);
                    }
                    if !after.is_empty() { *sanctioned_inventory_delta.entry((after.item, after.nbt.clone())).or_default() += i64::from(after.count); }
                    let revision = next_revision(target, previous)?;
                    staged.insert(target, Some(Cell { revision, value: Value::Inventory(after) }));
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_trade_items_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::TradeItems { intent } => {
                let values = staged.iter().filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell))).collect();
                let derived = intent.derive(owners, ActionRef { tick: action.tick, id: action.id }, &values)?;
                for (target, after) in derived.inventories {
                    if !after.is_empty() && after.count > owners.inventory_limit(&after, target)? {
                        return Err(ApplyFailure::Inventory(target));
                    }
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if let Some(Cell { value: Value::Inventory(before), .. }) = previous && !before.is_empty() {
                        *sanctioned_inventory_delta.entry((before.item, before.nbt.clone())).or_default() -= i64::from(before.count);
                    }
                    if !after.is_empty() {
                        *sanctioned_inventory_delta.entry((after.item, after.nbt.clone())).or_default() += i64::from(after.count);
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(target, Some(Cell { revision, value: Value::Inventory(after) }));
                }
                if let Some(offers) = derived.offers {
                    let target = intent.menu.offers_target();
                    let revision = next_revision(target, staged.get(&target).and_then(Option::as_ref))?;
                    staged.insert(target, Some(Cell { revision, value: Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::MerchantOffers(Some(offers))) }));
                }
                if let Some(menu) = derived.menu {
                    let target = intent.menu_target;
                    let revision = next_revision(target, staged.get(&target).and_then(Option::as_ref))?;
                    staged.insert(target, Some(Cell { revision, value: Value::LivingProperty(
                        crate::living_property::LivingPropertyValue::MenuSession(menu)) }));
                }
                for (target, body) in derived.experience_rewards {
                    let Target::Chunk { key, .. } = target else { return Err(ApplyFailure::InvalidAction); };
                    if owners.scheduled_executor(key)? != body.owner { return Err(ApplyFailure::InvalidAction); }
                    let revision = next_revision(target, staged.get(&target).and_then(Option::as_ref))?;
                    staged.insert(target, Some(Cell { revision, value: Value::Entity((body).into()) }));
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_craft_items_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::CraftItems { intent } => {
                let values = staged
                    .iter()
                    .filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell)))
                    .collect();
                for (target, after) in intent.derive(&values)? {
                    if !after.is_empty() && after.count > owners.inventory_limit(&after, target)? {
                        return Err(ApplyFailure::Inventory(target));
                    }
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if let Some(Cell {
                        value: Value::Inventory(item),
                        ..
                    }) = previous
                        && !item.is_empty()
                    {
                        *sanctioned_inventory_delta
                            .entry((item.item, item.nbt.clone()))
                            .or_default() -= i64::from(item.count);
                    }
                    if !after.is_empty() {
                        *sanctioned_inventory_delta
                            .entry((after.item, after.nbt.clone()))
                            .or_default() += i64::from(after.count);
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(
                        target,
                        Some(Cell {
                            revision,
                            value: Value::Inventory(after),
                        }),
                    );
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_close_inventory_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::CloseInventory { intent } => {
                let values = staged
                    .iter()
                    .filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell)))
                    .collect();
                let derived = intent.derive(
                    owners,
                    ActionRef {
                        tick: action.tick,
                        id: action.id,
                    },
                    &values,
                )?;
                if let Some(menu) = derived.menu {
                    let previous_menu = staged.get(&intent.menu).and_then(Option::as_ref);
                    let menu_revision = next_revision(intent.menu, previous_menu)?;
                    staged.insert(intent.menu, Some(Cell { revision: menu_revision,
                        value: Value::LivingProperty(crate::living_property::LivingPropertyValue::MenuSession(menu)) }));
                }
                for (target, after) in derived.cells {
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if intent.results.iter().any(|result| result.target == target)
                        && let Some(Cell {
                            value: Value::Inventory(item),
                            ..
                        }) = previous
                        && !item.is_empty()
                    {
                        *sanctioned_inventory_delta
                            .entry((item.item, item.nbt.clone()))
                            .or_default() -= i64::from(item.count);
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(
                        target,
                        Some(Cell {
                            revision,
                            value: Value::Inventory(after),
                        }),
                    );
                }
                for (target, body) in derived.drops {
                    let revision =
                        next_revision(target, staged.get(&target).and_then(Option::as_ref))?;
                    staged.insert(
                        target,
                        Some(Cell {
                            revision,
                            value: Value::Entity((body).into()),
                        }),
                    );
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_crossbow_items_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::CrossbowItems { intent } => {
                let values = staged
                    .iter()
                    .filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell)))
                    .collect();
                for (target, after) in intent.derive(owners, action.id, action.tick, &values)? {
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if let Some(Cell {
                        value: Value::Inventory(before),
                        ..
                    }) = previous
                        && !before.is_empty()
                    {
                        *sanctioned_inventory_delta
                            .entry((before.item, before.nbt.clone()))
                            .or_default() -= i64::from(before.count);
                    }
                    if !after.is_empty() {
                        *sanctioned_inventory_delta
                            .entry((after.item, after.nbt.clone()))
                            .or_default() += i64::from(after.count);
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(
                        target,
                        Some(Cell {
                            revision,
                            value: Value::Inventory(after),
                        }),
                    );
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_collect_projectile_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::CollectProjectile { intent } => {
                let values = staged.iter().filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell))).collect();
                for effect in intent.derive(owners, &values)? {
                    match effect {
                        Effect::Delete { target } if target == intent.contact.item.target() => { staged.insert(target, None); },
                        Effect::Put { target, value } => {
                            let previous = staged.get(&target).and_then(Option::as_ref);
                            if let Value::Inventory(after) = &value {
                                if let Some(Cell { value: Value::Inventory(before), .. }) = previous && !before.is_empty() {
                                    *sanctioned_inventory_delta.entry((before.item, before.nbt.clone())).or_default() -= i64::from(before.count);
                                }
                                if !after.is_empty() { *sanctioned_inventory_delta.entry((after.item, after.nbt.clone())).or_default() += i64::from(after.count); }
                            }
                            let revision = next_revision(target, previous)?;
                            staged.insert(target, Some(Cell { revision, value }));
                        }
                        _ => return Err(ApplyFailure::InvalidAction),
                    }
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_transform_items_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::TransformItems { intent } => {
                let values = staged
                    .iter()
                    .filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell)))
                    .collect();
                let transformed = intent.derive(owners, action.id, action.tick, &values)?;
                for (target, after) in transformed {
                    let previous = staged.get(&target).and_then(Option::as_ref);
                    if let Some(Cell {
                        value: Value::Inventory(item),
                        ..
                    }) = previous
                    {
                        if !item.is_empty() {
                            *sanctioned_inventory_delta
                                .entry((item.item, item.nbt.clone()))
                                .or_default() -= i64::from(item.count);
                        }
                    }
                    if !after.is_empty() {
                        *sanctioned_inventory_delta
                            .entry((after.item, after.nbt.clone()))
                            .or_default() += i64::from(after.count);
                    }
                    let revision = next_revision(target, previous)?;
                    staged.insert(
                        target,
                        Some(Cell {
                            revision,
                            value: Value::Inventory(after),
                        }),
                    );
                }
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}

#[inline(never)]
fn prepare_move_items_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    match effect {
            Effect::MoveItems {
                source,
                destination,
                count,
                max_stack,
            } => {
                if source == destination || *count == 0 || *max_stack == 0 {
                    return Err(ApplyFailure::Inventory(*source));
                }
                let Some(Some(source_cell)) = staged.get(source) else {
                    return Err(ApplyFailure::Inventory(*source));
                };
                let source_item = transferable_item(&source_cell.value, matches!(&action.semantic_intent, Some(SemanticIntent::Pickup { contact, inventory }) if crate::item_lifecycle::hopper_pickup_guard(action, contact, inventory)))
                    .ok_or(ApplyFailure::Inventory(*source))?;
                if *max_stack > owners.inventory_limit(&source_item, *destination)? {
                    return Err(ApplyFailure::Inventory(*destination));
                }
                if source_item.is_empty() || source_item.count < *count {
                    return Err(ApplyFailure::Inventory(*source));
                }
                let Some(Some(destination_cell)) = staged.get(destination) else {
                    return Err(ApplyFailure::Inventory(*destination));
                };
                let Value::Inventory(destination_item) = &destination_cell.value else {
                    return Err(ApplyFailure::Type(*destination));
                };
                if !destination_item.is_empty()
                    && (source_item.item != destination_item.item
                        || source_item.nbt != destination_item.nbt)
                {
                    return Err(ApplyFailure::Inventory(*destination));
                }
                let destination_count = destination_item
                    .count
                    .checked_add(*count)
                    .ok_or(ApplyFailure::Overflow(*destination))?;
                if destination_count > *max_stack {
                    return Err(ApplyFailure::Inventory(*destination));
                }
                let mut source_next = source_item.clone();
                source_next.count -= count;
                if source_next.count == 0 {
                    source_next.item = 0;
                    source_next.nbt.clear();
                }
                let mut destination_next = source_item.clone();
                destination_next.loc = destination_item.loc;
                destination_next.count = destination_count;
                let source_revision = next_revision(*source, Some(source_cell))?;
                let destination_revision = next_revision(*destination, Some(destination_cell))?;
                let source_after = match &source_cell.value {
                    Value::Entity(_) if source_next.count == 0 => None,
                    Value::Entity(body) => {
                        let mut body = body.clone();
                        let crate::entity_body::EntityPayload::Item(item) = &mut body.payload
                        else {
                            return Err(ApplyFailure::Type(*source));
                        };
                        item.count = source_next.count;
                        Some(Cell {
                            revision: source_revision,
                            value: Value::Entity((body).into()),
                        })
                    }
                    _ => Some(Cell {
                        revision: source_revision,
                        value: Value::Inventory(source_next),
                    }),
                };
                staged.insert(*source, source_after);
                staged.insert(
                    *destination,
                    Some(Cell {
                        revision: destination_revision,
                        value: Value::Inventory(destination_next),
                    }),
                );
            }
        _ => return Err(ApplyFailure::InvalidAction),
    }
    Ok(())
}


#[inline(never)]
fn prepare_entity_admission_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::EntityAdmission {
                target,
                body,
                inventory,
            } => {
                let Target::Chunk { key, .. } = target else {
                    return Err(ApplyFailure::Type(*target));
                };
                if staged.get(target).and_then(Option::as_ref).is_some() {
                    return Err(ApplyFailure::Condition(*target));
                }
                let required: BTreeSet<_> =
                    owners.entity_inventory_layout(body)?.into_iter().collect();
                let actual: BTreeSet<_> = inventory
                    .cells
                    .iter()
                    .filter_map(|cell| match &cell.value {
                        Value::Inventory(item) => Some(item.loc),
                        _ => None,
                    })
                    .collect();
                if actual != required || actual.len() != inventory.cells.len() {
                    return Err(ApplyFailure::InvalidAction);
                }
                if let crate::entity_body::EntityPayload::Living(living) = &body.payload {
                    use crate::mob_continuation::{HorseSpeciesKind, MobSpecies};
                    use crate::mob_species_property::MobSpeciesProperty as P;
                    let properties = match living.mob.as_ref().map(|mob| &mob.species) {
                        Some(MobSpecies::Horse { kind: HorseSpeciesKind::Donkey | HorseSpeciesKind::Mule, .. }) => vec![P::HorseChested],
                        Some(MobSpecies::Llama { .. }) => vec![P::LlamaChested, P::LlamaStrength],
                        _ => Vec::new(),
                    };
                    let facets = properties.into_iter().map(|property| {
                        living.mob.as_ref().and_then(|mob| property.read(&mob.species))
                            .map(|value| (property, value)).ok_or(ApplyFailure::InvalidAction)
                    }).collect::<Result<Vec<_>, _>>()?;
                    if inventory.cells.iter().any(|cell| matches!(&cell.value,
                        Value::Inventory(item) if item.loc.inv == crate::inventory::INV_MAIN)) {
                        let capacity = owners.entity_inventory_storage_capacity(body.kind, &facets, actual.iter().filter(|loc| loc.inv == crate::inventory::INV_MAIN).count() as u16)?;
                        if inventory.cells.iter().any(|cell| matches!(&cell.value,
                            Value::Inventory(item) if item.loc.inv == crate::inventory::INV_MAIN
                                && item.loc.slot >= capacity && !item.is_empty())) {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                }
                for (slot, cell) in inventory.entries(*key, body.id) {
                    if staged.get(&slot).and_then(Option::as_ref).is_some() {
                        return Err(ApplyFailure::Condition(slot));
                    }
                    let Value::Inventory(item) = &cell.value else {
                        return Err(ApplyFailure::Type(slot));
                    };
                    if !item.is_empty() {
                        if item.count > owners.entity_inventory_limit(body, item, slot)? {
                            return Err(ApplyFailure::Inventory(slot));
                        }
                        *sanctioned_inventory_delta
                            .entry((item.item, item.nbt.clone()))
                            .or_default() += i64::from(item.count);
                    }
                    staged.insert(slot, Some(cell.clone()));
                }
                staged.insert(
                    *target,
                    Some(Cell {
                        revision: 0,
                        value: Value::Entity((body.clone()).into()),
                    }),
                );

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_projectile_shooter_teleport_effect(
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::ProjectileShooterTeleport { intent } => {
                let values = staged.iter().filter_map(|(target, cell)| cell.clone().map(|cell| (*target, cell))).collect();
                let relocated = intent.project(ActionRef { tick: action.tick, id: action.id }, &values)?;
                staged.retain(|target, _| crate::living_property::body_target(*target)
                    .is_none_or(|(body, _)| body != intent.shooter.target()));
                for (target, value) in relocated {
                    let cell = if let Some(value) = value {
                        let revision = next_revision(target, staged.get(&target).and_then(Option::as_ref))?;
                        Some(Cell { revision, value })
                    } else { None };
                    staged.insert(target, cell);
                }

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_put_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    body_kinds: &BTreeMap<GlobalEntityId, &crate::entity_body::EntityBody>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::Put { target, value } => {
                if let Value::Inventory(item) = value
                    && !item.is_empty()
                    && !matches!(
                        target,
                        Target::Player {
                            inventory: Some(InventoryId::Player {
                                inv: crate::inventory::INV_SELECTED,
                                slot: 0
                            }),
                            ..
                        }
                    )
                    && item.count > owners.inventory_limit(item, *target)?
                {
                    return Err(ApplyFailure::Inventory(*target));
                }
                if let Value::Entity(body) = value
                    && let Some(previous) = body_kinds.get(&body.id)
                    && (body.birth != previous.birth
                        || body.spatial_epoch != previous.spatial_epoch
                            && !(action.kind == ActionKind::Actor
                                && action.chunk_transition
                                && body.spatial_epoch
                                    == SpatialEpoch::Action(ActionRef {
                                        tick: action.tick,
                                        id: action.id,
                                    }))
                        || body.kind != previous.kind
                        || std::mem::discriminant(&body.payload)
                            != std::mem::discriminant(&previous.payload)
                        || body.owner != previous.owner
                            && !(action.kind == ActionKind::Actor && action.chunk_transition))
                {
                    return Err(ApplyFailure::Type(*target));
                }
                if let Value::Entity(body) = value
                    && !body_kinds.contains_key(&body.id)
                    && (body.birth.action
                        != (ActionRef {
                            tick: action.tick,
                            id: action.id,
                        })
                        || body.spatial_epoch
                            != SpatialEpoch::Action(ActionRef {
                                tick: action.tick,
                                id: action.id,
                            }))
                {
                    return Err(ApplyFailure::Type(*target));
                }
                let revision = next_revision(*target, staged.get(target).and_then(Option::as_ref))?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: value.clone(),
                    }),
                );

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_retire_entity_effect<S: ActionState>(
    owners: &S,
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::RetireEntity { intent } => {
                let Some(Some(Cell { value: Value::Entity(body), .. })) = staged.get(&intent.target) else {
                    return Err(ApplyFailure::Condition(intent.target));
                };
                intent.validate_body(owners, body)?;
                if (!body.passengers.is_empty() || body.mount.is_some()) && !crate::actor_relation::owner_detach_all(action) {
                    return Err(ApplyFailure::InvalidAction);
                }
                for binding in &intent.inventory {
                    let Some(Some(Cell { value: Value::Inventory(item), .. })) = staged.get(&binding.target) else {
                        return Err(ApplyFailure::Inventory(binding.target));
                    };
                    if item.loc != binding.slot.loc { return Err(ApplyFailure::Inventory(binding.target)); }
                    if !item.is_empty() {
                        *sanctioned_inventory_delta.entry((item.item, item.nbt.clone())).or_default() -= i64::from(item.count);
                    }
                    staged.insert(binding.target, None);
                }
                for target in intent.relation_targets() { staged.insert(target, None); }
                staged.insert(intent.target, None);

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_move_player_avatar_effect(
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::MovePlayerAvatar {
                source,
                destination,
                avatar,
                velocity_micro,
            } => {
                let Some(Some(Cell {
                    value: Value::Living(current),
                    ..
                })) = staged.get(source)
                else {
                    return Err(ApplyFailure::Type(*source));
                };
                if source != destination && staged.get(destination).is_some_and(Option::is_some) {
                    return Err(ApplyFailure::InvalidAction);
                }
                let mut next = current.clone();
                next.avatar = Some(avatar.clone());
                next.combat.velocity_micro = *velocity_micro;
                let revision = next_revision(
                    *destination,
                    staged.get(destination).and_then(Option::as_ref),
                )?;
                if source != destination {
                    staged.insert(*source, None);
                }
                staged.insert(
                    *destination,
                    Some(Cell {
                        revision,
                        value: Value::Living((next).into()),
                    }),
                );

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_respawn_player_avatar_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::RespawnPlayerAvatar {
                source,
                destination,
                avatar,
                max_health_attribute,
            } => {
                let Some(Some(Cell {
                    value: Value::Living(current),
                    ..
                })) = staged.get(source)
                else {
                    return Err(ApplyFailure::Type(*source));
                };
                if source != destination && staged.get(destination).is_some_and(Option::is_some) {
                    return Err(ApplyFailure::InvalidAction);
                }
                let mut next = current.clone();
                let source_facets = staged.iter().filter_map(|(target, cell)| {
                    if crate::living_property::body_target(*target).is_none() && crate::actor_relation::body_target(*target).is_none() { return None; }
                    let body = crate::derived_living::actor_body_target(*target)?;
                    (body == *source).then(|| (*target, cell.clone()))
                }).collect::<Vec<_>>();
                for (target, cell) in &source_facets {
                    let Some(cell) = cell else { return Err(ApplyFailure::Type(*target)); };
                    if let Some((_, property)) = crate::living_property::body_target(*target) {
                        let Value::LivingProperty(value) = &cell.value else { return Err(ApplyFailure::Type(*target)); };
                        if !property.write(&mut next, value) { return Err(ApplyFailure::Type(*target)); }
                    } else if let Some((_, crate::actor_relation::ActorRelation::Mount)) = crate::actor_relation::body_target(*target) {
                        let Value::ActorRelation(value) = &cell.value else { return Err(ApplyFailure::Type(*target)); };
                        next.avatar.as_mut().ok_or(ApplyFailure::Type(*source))?.mount = value.clone();
                    }
                }
                for (target, _) in source_facets { staged.remove(&target); }

                let old_avatar = next.avatar.as_ref().ok_or(ApplyFailure::Type(*source))?;
                let mut moved = old_avatar.clone();
                moved.world = avatar.world;
                moved.position = avatar.position;
                moved.yaw = avatar.yaw;
                moved.head_yaw = avatar.head_yaw;
                moved.pitch = avatar.pitch;
                moved.spatial_epoch = avatar.spatial_epoch;
                moved.movement_tick = avatar.movement_tick;
                moved.movement_sequence = avatar.movement_sequence;
                moved.on_ground = false;
                moved.fall_distance = 0;
                moved.on_fire = false;
                moved.stance = Default::default();
                moved.mount = None;
                moved.presentation.camera_target = None;
                moved.presentation.sleeping = None;
                next.avatar = Some(moved);
                next.combat.absorption_milli = 0;
                next.combat.velocity_micro = [0; 3];
                next.combat.timing = Default::default();
                next.combat.revision = next
                    .combat
                    .revision
                    .checked_add(1)
                    .ok_or(ApplyFailure::Overflow(*source))?;
                let old_effects = crate::active_effects::ActiveEffectSet {
                    visible: next.effects.clone(),
                    hidden: next.hidden_effects.clone(),
                };
                let old_modifiers = owners.status_effect_modifiers(&old_effects)?;
                next.combat.equipment.modifiers.retain(|modifier| {
                    !old_modifiers
                        .iter()
                        .any(|old| old.attribute == modifier.attribute && old.id == modifier.id)
                });
                let base = next.combat.effective_attributes.get(max_health_attribute)
                    .copied().map(f64::from_bits).ok_or(ApplyFailure::InvalidAction)?;
                let health = next.attribute(*max_health_attribute, base);
                if !health.is_finite() || !(0.001..=1_000_000.0).contains(&health) {
                    return Err(ApplyFailure::InvalidAction);
                }
                next.combat.health_milli = (health * 1000.0).round() as u32;
                next.environment = Default::default();
                next.combat.status = crate::combat_action::CombatStatusFacts {
                    invulnerable: next.combat.status.invulnerable,
                    ..Default::default()
                };
                next.food.food = 20;
                next.food.saturation_milli = 5_000;
                next.food.exhaustion_milli = 0;
                next.food.tick_timer = 0;
                next.food.revision = next
                    .food
                    .revision
                    .checked_add(1)
                    .ok_or(ApplyFailure::Overflow(*source))?;
                next.use_state = None;
                next.effects.clear();
                next.hidden_effects.clear();
                next.cooldowns.clear();
                let revision = next_revision(
                    *destination,
                    staged.get(destination).and_then(Option::as_ref),
                )?;
                if source != destination {
                    staged.insert(*source, None);
                }
                staged.insert(
                    *destination,
                    Some(Cell {
                        revision,
                        value: Value::Living((next).into()),
                    }),
                );

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_reset_player_living_effect(
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::ResetPlayerLiving { target, template } => {
                let Some(Some(Cell {
                    value: Value::Living(current),
                    ..
                })) = staged.get(target)
                else {
                    return Err(ApplyFailure::Type(*target));
                };
                let current_admin = current
                    .player_administrative_state()
                    .ok_or(ApplyFailure::Type(*target))?;
                let mut reset_admin = template
                    .player_administrative_state()
                    .ok_or(ApplyFailure::Type(*target))?;
                reset_admin.world = current_admin.world;
                reset_admin.position = current_admin.position;
                reset_admin.yaw = current_admin.yaw;
                reset_admin.pitch = current_admin.pitch;
                let mut next = current
                    .with_player_administrative_state(&reset_admin)
                    .ok_or(ApplyFailure::Type(*target))?;
                if current.combat.health_milli > 0 && next.combat.health_milli == 0 {
                    next.combat.timing.death_cause = Some(ActionRef {
                        id: action.id,
                        tick: action.tick,
                    });
                }
                let revision = next_revision(*target, staged.get(target).and_then(Option::as_ref))?;
                staged.insert(
                    *target,
                    Some(Cell {
                        revision,
                        value: Value::Living((next).into()),
                    }),
                );

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_move_entity_body_effect(
    action: &Action,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::MoveEntityBody {
                source,
                destination,
                spatial,
                authority,
            } => {
                let Some(source_cell) = staged.get(source).and_then(Option::as_ref) else {
                    return Ok(());
                };
                let Value::Entity(body) = &source_cell.value else {
                    return Err(ApplyFailure::Type(*source));
                };
                let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(id),
                    inventory: None,
                } = *source
                else {
                    return Err(ApplyFailure::Type(*source));
                };
                if !body.belongs_to(id, key)
                    || authority.as_ref().is_some_and(|authority| {
                        authority.birth != body.birth
                            || authority.body_owner != body.owner
                            || authority.issuer.generation == 0
                    })
                {
                    return Err(ApplyFailure::Type(*source));
                }
                if source != destination
                    && staged.get(destination).and_then(Option::as_ref).is_some()
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let mut moved = body.clone();
                moved.spatial = spatial.clone();
                moved.spatial_epoch = SpatialEpoch::Action(ActionRef {
                    tick: action.tick,
                    id: action.id,
                });
                let revision =
                    next_revision(*destination, (source == destination).then_some(source_cell))?;
                staged.insert(*source, None);
                staged.insert(
                    *destination,
                    Some(Cell {
                        revision,
                        value: Value::Entity((moved).into()),
                    }),
                );

                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

#[inline(never)]
fn prepare_player_avatar_lifecycle_effect<S: ActionState>(
    owners: &S,
    effect: &Effect,
    staged: &mut BTreeMap<Target, Option<Cell>>,
    sanctioned_inventory_delta: &mut BTreeMap<(u16, Vec<u8>), i64>,
) -> Result<(), ApplyFailure> {
    match effect {
        Effect::PlayerAvatarLifecycle { target, intent } => {
                    match intent {
                crate::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                    initial,
                    lease,
                    inventory,
                    ..
                } => {
                    if staged.get(target).and_then(Option::as_ref).is_some() {
                        return Err(ApplyFailure::Condition(*target));
                    }
                    for (slot, cell) in inventory.entries(*lease) {
                        if staged.get(&slot).and_then(Option::as_ref).is_some() {
                            return Err(ApplyFailure::Condition(slot));
                        }
                        let Value::Inventory(item) = &cell.value else {
                            return Err(ApplyFailure::Type(slot));
                        };
                        if item.loc.inv != crate::inventory::INV_SELECTED && !item.is_empty() {
                            if item.count > owners.inventory_limit(item, slot)? {
                                return Err(ApplyFailure::Condition(slot));
                            }
                            *sanctioned_inventory_delta
                                .entry((item.item, item.nbt.clone()))
                                .or_default() += i64::from(item.count);
                        }
                        staged.insert(slot, Some(cell.clone()));
                    }
                    staged.insert(
                        *target,
                        Some(Cell {
                            revision: 0,
                            value: Value::Living((initial.clone()).into()),
                        }),
                    );
                }
                crate::avatar_lifecycle::AvatarLifecycleIntent::Retirement { lease, .. } => {
                    let Some(Cell {
                        value: Value::Living(living),
                        ..
                    }) = staged.get(target).and_then(Option::as_ref)
                    else {
                        return Err(ApplyFailure::Condition(*target));
                    };
                    let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(*target))?;
                    let admitted_identity_matches = match avatar.spatial_epoch {
                        SpatialEpoch::Admission { lease: admitted, .. } => admitted == *lease,
                        SpatialEpoch::Action(reference) => reference.id.incarnation != 0,
                    };
                    if !living.belongs_to(ActorId::Player(lease.gid))
                        || !admitted_identity_matches
                    {
                        return Err(ApplyFailure::Condition(*target));
                    }
                    staged.insert(*target, None);
                }
            };
                    Ok(())

        }
        _ => Err(ApplyFailure::InvalidAction),
    }
}

pub fn undo_action<S: ActionState>(
    owners: &mut S,
    applied: &AppliedAction,
) -> Result<(), InvariantFailure> {
    let inverse = prepare_undo(owners, applied)?;
    owners
        .commit(
            ActionContext {
                id: applied.id,
                tick: applied.tick,
                actor_chunk: applied.actor_chunk,
            },
            applied.view,
            &inverse,
        )
        .map_err(|_| InvariantFailure::Commit)?;
    Ok(())
}

pub fn prepare_undo<S: ActionState>(
    owners: &S,
    applied: &AppliedAction,
) -> Result<Vec<AppliedWrite>, InvariantFailure> {
    for write in &applied.writes {
        if owners.read(applied.view, write.target) != write.after {
            return Err(InvariantFailure::ChangedSinceApply(write.target));
        }
    }
    let inverse = applied
        .writes
        .iter()
        .rev()
        .map(|write| AppliedWrite {
            target: write.target,
            before: write.after.clone(),
            after: write.before.clone(),
        })
        .collect::<Vec<_>>();
    Ok(inverse)
}

pub fn prepare_semantic_undo<S: ActionState>(
    owners: &S,
    action: &Action,
    applied: &AppliedAction,
) -> Result<Vec<AppliedWrite>, InvariantFailure> {
    if action.id != applied.id || action.tick != applied.tick {
        return Err(InvariantFailure::Commit);
    }
    let procedures = action
        .undo
        .iter()
        .skip(1)
        .filter_map(|procedure| match procedure {
            UndoProcedure::AdjustInventoryCount { target }
            | UndoProcedure::AdjustScalar { target } => Some((*target, *procedure)),
            UndoProcedure::RestoreWrittenTargets => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut inverse = Vec::with_capacity(applied.writes.len());
    for write in applied.writes.iter().rev() {
        let current = owners.read(applied.view, write.target);
        let after = match procedures.get(&write.target) {
            Some(UndoProcedure::AdjustInventoryCount { .. }) => {
                let (Some(before), Some(original_after), Some(current)) =
                    (&write.before, &write.after, current.as_ref())
                else {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                };
                let (
                    Value::Inventory(before_item),
                    Value::Inventory(after_item),
                    Value::Inventory(current_item),
                ) = (&before.value, &original_after.value, &current.value)
                else {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                };
                if before_item.loc != after_item.loc || after_item.loc != current_item.loc {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                }
                if !before_item.is_empty()
                    && !after_item.is_empty()
                    && (before_item.item != after_item.item || before_item.nbt != after_item.nbt)
                {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                }
                let delta = i16::from(after_item.count) - i16::from(before_item.count);
                let count = i16::from(current_item.count) - delta;
                let Ok(count) = u8::try_from(count) else {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                };
                if count > 99 {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                }
                let identity = [before_item, after_item, current_item]
                    .into_iter()
                    .find(|item| !item.is_empty())
                    .ok_or(InvariantFailure::ChangedSinceApply(write.target))?;
                if !current_item.is_empty()
                    && (current_item.item != identity.item || current_item.nbt != identity.nbt)
                {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                }
                let mut restored = if count == 0 {
                    InventoryCell::empty(current_item.loc)
                } else {
                    identity.clone()
                };
                restored.count = count;
                Some(Cell {
                    revision: current
                        .revision
                        .checked_add(1)
                        .ok_or(InvariantFailure::ChangedSinceApply(write.target))?,
                    value: Value::Inventory(restored),
                })
            }
            Some(UndoProcedure::AdjustScalar { .. }) => {
                let (Some(before), Some(original_after), Some(current)) =
                    (&write.before, &write.after, current.as_ref())
                else {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                };
                let (Value::Scalar(before), Value::Scalar(after), Value::Scalar(value)) =
                    (&before.value, &original_after.value, &current.value)
                else {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                };
                let delta = after
                    .checked_sub(*before)
                    .ok_or(InvariantFailure::ChangedSinceApply(write.target))?;
                Some(Cell {
                    revision: current
                        .revision
                        .checked_add(1)
                        .ok_or(InvariantFailure::ChangedSinceApply(write.target))?,
                    value: Value::Scalar(
                        value
                            .checked_sub(delta)
                            .ok_or(InvariantFailure::ChangedSinceApply(write.target))?,
                    ),
                })
            }
            _ => {
                if current != write.after {
                    return Err(InvariantFailure::ChangedSinceApply(write.target));
                }
                write.before.clone()
            }
        };
        inverse.push(AppliedWrite {
            target: write.target,
            before: current,
            after,
        });
    }
    Ok(inverse)
}

fn next_revision(target: Target, before: Option<&Cell>) -> Result<u64, ApplyFailure> {
    before.map_or(Ok(0), |cell| {
        cell.revision
            .checked_add(1)
            .ok_or(ApplyFailure::Overflow(target))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::inventory::InvLoc;
    use crate::protocol::WorldId;

    #[test]
    fn player_administration_relocates_one_avatar_across_two_chunk_stakeholders() {
        use crate::item_action::{ActorCombatState, ActorFoodState, PlayerAvatarState};

        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(3));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(3)),
            gid: actor,
            incarnation: 9,
        };
        let world = WorldId::new(0, 0);
        let old_chunk = ChunkKey::new(world, 0, 0);
        let new_chunk = ChunkKey::new(world, 2, 0);
        let old = ActorLivingState {
            permanent_attribute_modifiers: Default::default(),
            menu: Default::default(),
            environment: Default::default(),
            tags: Default::default(),
            tags_revision: 0,
            advancements: Default::default(),
            stand: None,
            mob: None,
            merchant: None,
            simulation_tick: None,
            effects: Vec::new(),
            hidden_effects: BTreeMap::new(),
            property_versions: std::collections::BTreeMap::new(),
            food: ActorFoodState {
                exhaustion_milli: 0,
                tick_timer: 0,
                actor: ActorId::Player(actor),
                food: 20,
                saturation_milli: 5_000,
                revision: 0,
            },
            combat: ActorCombatState {
                actor: ActorId::Player(actor),
                revision: 0,
                health_milli: 20_000,
                absorption_milli: 0,
                velocity_micro: [0; 3],
                timing: Default::default(),
                effective_attributes: Default::default(),
                equipment: Default::default(),
                status: Default::default(),
            },
            use_state: None,
            cooldowns: BTreeMap::new(),
            avatar: Some(PlayerAvatarState {
                stance: Default::default(),
                mount: None,
                relation_versions: Default::default(),
                experience: Default::default(),
                statistics: Default::default(),
                presentation: Default::default(),
                spatial_epoch: SpatialEpoch::Admission {
                    lease: LivePlayerLease {
                        key: lease.key,
                        gid: actor,
                        incarnation: lease.incarnation,
                    },
                    admitted_at: TickStamp(0),
                },
                world,
                position: [1.0_f64.to_bits(), 80.0_f64.to_bits(), 1.0_f64.to_bits()],
                yaw: 0.0_f32.to_bits(),
                head_yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                on_ground: true,
                movement_tick: TickStamp(0),
                movement_sequence: PlayerSeq(0),
                game_mode: 0,
                fall_distance: 0.0_f32.to_bits(),
                on_fire: false,
            }),
        };
        let mut new = old.clone();
        new.avatar.as_mut().unwrap().position[0] = 33.0_f64.to_bits();
        let old_target = Target::Chunk {
            key: old_chunk,
            part: ChunkPart::Player(actor),
            inventory: None,
        };
        let new_target = Target::Chunk {
            key: new_chunk,
            part: ChunkPart::Player(actor),
            inventory: None,
        };
        let before = Cell {
            revision: 4,
            value: Value::Living((old).into()),
        };
        let action = Action::from_player_administration(
            lease,
            PlayerSeq(7),
            TickStamp(11),
            old_chunk,
            new_chunk,
            before.clone(),
            new,
        )
        .unwrap();
        assert_eq!(action.class, ActionClass::Infallible);
        assert!(action.chunk_transition);
        let boundary = crate::protocol::PlayerChunkTransition {
            gid: actor,
            incarnation: lease.incarnation,
            seq: PlayerSeq(7),
            tick: TickStamp(11),
            from: old_chunk,
            to: new_chunk,
            snapshot: crate::protocol::PlayerReplicaSnapshot {
                movement: crate::protocol::PosUpdate {
                    spatial_epoch: crate::canonical_action::SpatialEpoch::Admission {
                        lease: crate::canonical_action::LivePlayerLease {
                            key: lease.key,
                            gid: actor,
                            incarnation: lease.incarnation,
                        },
                        admitted_at: TickStamp(11),
                    },
                    gid: actor,
                    incarnation: lease.incarnation,
                    seq: PlayerSeq(7),
                    tick: TickStamp(11),
                    world,
                    pos: [33.0, 80.0, 1.0],
                    vel: [0.25, 0.0, 0.0],
                    yaw: 0.0,
                    head_yaw: 0.5,
                    pitch: 0.0,
                    on_ground: false,
                },
                visual: crate::protocol::PlayerVisualSnapshot::default(),
            },
        };
        let boundary_action =
            Action::from_player_chunk_transition(&boundary, lease, before.clone()).unwrap();
        assert_eq!(boundary_action.kind, ActionKind::Actor);
        let mut boundary_truth = OwnedStakeholderSet::default();
        boundary_truth.ground.insert(old_target, before.clone());
        apply_action(&mut boundary_truth, &boundary_action, TruthView::Ground).unwrap();
        assert!(matches!(
            boundary_truth.ground.get(&new_target),
            Some(Cell { value: Value::Living(living), .. })
                if living.combat.velocity_micro == [250_000, 0, 0]
        ));
        let Value::Living(boundary_living) = &boundary_truth.ground.get(&new_target).unwrap().value
        else {
            unreachable!();
        };
        let accepted_avatar = boundary_living.avatar.as_ref().unwrap();
        assert_eq!(accepted_avatar.head_yaw, 0.5_f32.to_bits());
        assert!(!accepted_avatar.on_ground);
        assert_eq!(accepted_avatar.movement_tick, TickStamp(11));
        assert_eq!(accepted_avatar.movement_sequence, PlayerSeq(7));
        let mut damaged = match &before.value {
            Value::Living(state) => state.clone(),
            _ => unreachable!(),
        };
        damaged.combat.health_milli = 7_000;
        damaged.combat.revision = 1;
        let current = Cell {
            revision: 5,
            value: Value::Living((damaged).into()),
        };
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(old_target, current.clone());
        state.ground.insert(old_target, current);
        let applied = apply_action(&mut state, &action, TruthView::Local).unwrap();
        assert!(state.local.get(&old_target).is_none());
        assert!(matches!(
            state.local.get(&new_target),
            Some(Cell {
                value: Value::Living(_),
                ..
            })
        ));
        assert_eq!(applied.writes.len(), 2);
        assert!(matches!(
            state.local.get(&new_target),
            Some(Cell {
                value: Value::Living(living),
                ..
            }) if living.combat.health_milli == 7_000
        ));
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        assert_eq!(state.local, state.ground);
        let before = state.local.get(&new_target).unwrap().clone();
        let mut next = match &before.value {
            Value::Living(state) => state.clone(),
            _ => unreachable!(),
        };
        next.avatar.as_mut().unwrap().position[0] = 34.0_f64.to_bits();
        let same_chunk = Action::from_player_administration(
            lease,
            PlayerSeq(8),
            TickStamp(12),
            new_chunk,
            new_chunk,
            before,
            *next,
        )
        .unwrap();
        assert_eq!(same_chunk.class, ActionClass::Infallible);
        assert!(!same_chunk.chunk_transition);
        let current = state.local.get_mut(&new_target).unwrap();
        let Value::Living(living) = &mut current.value else {
            unreachable!();
        };
        living.combat.health_milli = 3_000;
        living.combat.revision = living.combat.revision.wrapping_add(1);
        apply_action(&mut state, &same_chunk, TruthView::Local).unwrap();
        assert!(matches!(
            state.local.get(&new_target),
            Some(Cell {
                value: Value::Living(living),
                ..
            }) if living.combat.health_milli == 3_000 && living.avatar.as_ref().is_some_and(|avatar| avatar.position[0] == 34.0_f64.to_bits())
        ));

        let before_reset = state.local.get(&new_target).unwrap().clone();
        let mut reset = match &before_reset.value {
            Value::Living(living) => living.clone(),
            _ => unreachable!(),
        };
        reset.combat.health_milli = 20_000;
        reset.avatar.as_mut().unwrap().game_mode = 1;
        let reset_action = Action::from_player_administration(
            lease,
            PlayerSeq(9),
            TickStamp(13),
            new_chunk,
            new_chunk,
            before_reset.clone(),
            *reset.clone(),
        )
        .unwrap();
        assert_eq!(reset_action.class, ActionClass::Infallible);
        let current = state.local.get_mut(&new_target).unwrap();
        let Value::Living(living) = &mut current.value else {
            unreachable!();
        };
        living.avatar.as_mut().unwrap().position[0] = 35.0_f64.to_bits();
        current.revision += 1;
        apply_action(&mut state, &reset_action, TruthView::Local).unwrap();
        assert!(matches!(
            state.local.get(&new_target),
            Some(Cell {
                value: Value::Living(living),
                ..
            }) if living.combat.health_milli == 20_000
                && living.avatar.as_ref().is_some_and(|avatar| avatar.game_mode == 1
                    && avatar.position[0] == 35.0_f64.to_bits())
        ));

        reset.combat.health_milli = 10_000;
        reset.avatar.as_mut().unwrap().game_mode = 2;
        let second_reset = Action::from_player_administration(
            lease,
            PlayerSeq(10),
            TickStamp(13),
            new_chunk,
            new_chunk,
            before_reset.clone(),
            *reset,
        )
        .unwrap();
        let destination_chunk = ChunkKey::new(world, 3, 0);
        let mut moved = match &before_reset.value {
            Value::Living(living) => living.clone(),
            _ => unreachable!(),
        };
        moved.avatar.as_mut().unwrap().position[0] = 49.0_f64.to_bits();
        let move_after_resets = Action::from_player_administration(
            lease,
            PlayerSeq(11),
            TickStamp(13),
            new_chunk,
            destination_chunk,
            before_reset.clone(),
            *moved,
        )
        .unwrap();
        let destination_target = Target::Chunk {
            key: destination_chunk,
            part: ChunkPart::Player(actor),
            inventory: None,
        };
        for sequences in [[9, 10, 11], [u16::MAX, 0, 1]] {
            let mut first = reset_action.clone();
            let mut second = second_reset.clone();
            let mut transition = move_after_resets.clone();
            first.id.sequence = sequences[0];
            second.id.sequence = sequences[1];
            transition.id.sequence = sequences[2];
            for mut actions in [
                [first.clone(), second.clone(), transition.clone()],
                [transition.clone(), second.clone(), first.clone()],
            ] {
                let mut graphs = crate::action_graph::ActionGraphs::default();
                graphs
                    .install_chunk_frontier(
                        new_chunk,
                        crate::action_graph::ChunkGroundFrontier {
                            epoch: 1,
                            settled_through: TickStamp(12),
                        },
                    )
                    .unwrap();
                let lease = LivePlayerLease {
                    key: PlayerKey(uuid::Uuid::from_u128(93)),
                    gid: actor,
                    incarnation: 9,
                };
                for (target, value) in [
                    (new_target, Some(before_reset.clone())),
                    (destination_target, None),
                ] {
                    graphs
                        .install_player_avatar_admission(
                            target,
                            lease,
                            crate::action_graph::GroundAnchor {
                                settled_through: TickStamp(12),
                                value,
                            },
                        )
                        .unwrap();
                }
                for action in &mut actions {
                    action.player_leases = vec![lease];
                }
                let mut conflicting_transitions = graphs.clone();
                let transition_action = actions
                    .iter()
                    .find(|action| action.chunk_transition)
                    .unwrap()
                    .clone();
                let mut duplicate_transition = transition_action.clone();
                duplicate_transition.id.sequence = transition_action.id.sequence.wrapping_add(1);
                assert!(
                    conflicting_transitions
                        .insert_collected(TickStamp(13), [transition_action, duplicate_transition],)
                        .is_err()
                );
                assert!(conflicting_transitions.ticks.is_empty());
                let mut before_admission = actions[0].clone();
                before_admission.tick = TickStamp(12);
                assert!(
                    graphs
                        .insert_collected(TickStamp(12), [before_admission])
                        .is_err()
                );
                graphs
                    .insert_collected(TickStamp(13), actions.clone())
                    .unwrap();
                assert!(!graphs.advance_empty_chunk_frontier(TickStamp(13), new_chunk, true));
                let mut graph = crate::action_graph::TickActionGraph::default();
                for action in actions {
                    graph.insert(action).unwrap();
                }
                let components =
                    graph.affected_components(&graph.actions.keys().copied().collect());
                assert_eq!(components.len(), 1);
                let anchors = BTreeMap::from([
                    (new_target, Some(before_reset.clone())),
                    (destination_target, None),
                ]);
                let solved = crate::action_solver::solve_component(
                    &graph,
                    &components[0],
                    &anchors,
                    7,
                    |_| Some(true),
                )
                .unwrap();
                assert_eq!(
                    solved.causal_order,
                    vec![first.id, second.id, transition.id]
                );
                assert!(matches!(
                    &solved.variants[&second.id],
                    crate::action_solver::SolvedVariant::Applied { result, .. }
                        if matches!(
                            result.writes[0].after.as_ref(),
                            Some(Cell { value: Value::Living(living), .. })
                                if living.combat.health_milli == 10_000
                        )
                ));
                assert!(matches!(
                    &solved.variants[&transition.id],
                    crate::action_solver::SolvedVariant::Applied { result, .. }
                        if result.writes.iter().any(|write| {
                            write.target == destination_target
                                && matches!(
                                    write.after.as_ref(),
                                    Some(Cell { value: Value::Living(living), .. })
                                        if living.combat.health_milli == 10_000
                                            && living.avatar.as_ref().is_some_and(|avatar| avatar.position[0] == 49.0_f64.to_bits())
                                )
                        })
                ));
            }
        }
    }

    fn identity() -> ActionId {
        ActionId {
            actor: ActorId::Player(GlobalPlayerId {
                server: ServerId(1),
                player: PlayerSlot(3),
            }),
            incarnation: 17,
            sequence: 42,
            family: 1,
        }
    }

    #[test]
    fn infallible_scheduled_tick_deduplicates_at_the_whole_chunk_target() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Whole,
            inventory: None,
        };
        let tick = ScheduledTickFact {
            executor: crate::realm_completion::PeerGeneration {
                server: crate::identity::ServerId(0),
                generation: 1,
            },
            origin: ScheduledTickOrigin::Action {
                cause: ActionRef {
                    tick: TickStamp(8),
                    id: identity(),
                },
                producer: crate::realm_completion::PeerGeneration {
                    server: identity().actor.origin(),
                    generation: 1,
                },
            },
            kind: ScheduledTickKind::Block,
            id: 1,
            position: BlockPos { x: 1, y: 64, z: 1 },
            due: TickStamp(12),
            priority: 0,
            source_order: 9,
        };
        let mut action = Action {
            edit_authority: None,
            id: identity(),
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
            tick: TickStamp(8),
            class: ActionClass::Infallible,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            dependencies: Vec::new(),
            conditions: Vec::new(),
            effects: vec![Effect::ScheduleTick { target, tick }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::ScheduledTick,
        };
        let mut state = OwnedStakeholderSet::default();
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        action.id.sequence += 1;
        action.effects = vec![Effect::ScheduleTick {
            target,
            tick: ScheduledTickFact {
                executor: crate::realm_completion::PeerGeneration {
                    server: crate::identity::ServerId(0),
                    generation: 1,
                },
                origin: ScheduledTickOrigin::Action {
                    cause: ActionRef {
                        tick: action.tick,
                        id: action.id,
                    },
                    producer: crate::realm_completion::PeerGeneration {
                        server: action.id.actor.origin(),
                        generation: 1,
                    },
                },
                due: TickStamp(10),
                priority: -1,
                ..tick
            },
        }];
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        let Some(Cell {
            value: Value::ScheduledQueue(queue),
            ..
        }) = state.ground.get(&target)
        else {
            panic!("scheduled queue missing");
        };
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].due, TickStamp(12));
        assert_eq!(queue[0].priority, 0);
        action.id.sequence += 1;
        action.tick = TickStamp(12);
        action.effects = vec![Effect::DrainScheduledTicks {
            target,
            through: TickStamp(12),
        }];
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        assert!(
            matches!(&state.ground[&target].value, Value::ScheduledQueue(queue) if queue.is_empty())
        );
    }

    #[test]
    fn scheduled_tick_due_wraps_from_last_stamp_to_zero() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Whole,
            inventory: None,
        };
        let mut action = Action {
            edit_authority: None,
            id: identity(),
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
            tick: TickStamp(u16::MAX),
            class: ActionClass::Infallible,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            dependencies: Vec::new(),
            conditions: Vec::new(),
            effects: vec![Effect::ScheduleTick {
                target,
                tick: ScheduledTickFact {
                    executor: crate::realm_completion::PeerGeneration {
                        server: crate::identity::ServerId(0),
                        generation: 1,
                    },
                    origin: ScheduledTickOrigin::Action {
                        cause: ActionRef {
                            tick: TickStamp(u16::MAX),
                            id: identity(),
                        },
                        producer: crate::realm_completion::PeerGeneration {
                            server: identity().actor.origin(),
                            generation: 1,
                        },
                    },
                    kind: ScheduledTickKind::Fluid,
                    id: 1,
                    position: BlockPos { x: 1, y: 64, z: 1 },
                    due: TickStamp(0),
                    priority: 0,
                    source_order: 1,
                },
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::ScheduledTick,
        };
        let mut state = OwnedStakeholderSet::default();
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        action.tick = TickStamp(0);
        action.id.sequence += 1;
        action.effects = vec![Effect::DrainScheduledTicks {
            target,
            through: TickStamp(0),
        }];
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        assert!(
            matches!(&state.ground[&target].value, Value::ScheduledQueue(queue) if queue.is_empty())
        );
    }

    #[test]
    fn due_scheduled_ticks_orders_wrap_and_refuses_truncation() {
        let make = |due, priority, source_order| ScheduledTickFact {
            executor: crate::realm_completion::PeerGeneration {
                server: crate::identity::ServerId(0),
                generation: 1,
            },
            origin: crate::canonical_action::ScheduledTickOrigin::GenesisPrimary,
            kind: ScheduledTickKind::Block,
            id: 1,
            position: BlockPos { x: 1, y: 64, z: 1 },
            due: TickStamp(due),
            priority,
            source_order,
        };
        let queue = [
            make(1, 0, 4),
            make(u16::MAX, 0, 3),
            make(0, 1, 2),
            make(0, -1, 1),
            make(2, 0, 5),
        ];
        assert_eq!(due_scheduled_ticks(&queue, TickStamp(1), 3), Err(4));
        let due = due_scheduled_ticks(&queue, TickStamp(1), 4).unwrap();
        assert_eq!(
            due.iter().map(|tick| tick.due.0).collect::<Vec<_>>(),
            [u16::MAX, 0, 0, 1]
        );
        assert_eq!(due[1].priority, -1);
        assert_eq!(due[2].priority, 1);
    }

    fn targets() -> (Target, Target) {
        let player = Target::Player {
            key: PlayerKey(Uuid::from_u128(1)),
            lease: None,
            inventory: None,
        };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let block = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 64, z: 1 }),
            inventory: None,
        };
        (player, block)
    }

    #[test]
    fn item_pickup_conserves_components_and_undo_restores_body_and_slot() {
        use crate::entity_body::{
            DroppedItem, EntityBirth, EntityBody, EntityPayload, EntitySpatial,
        };
        use crate::inventory::InvLoc;
        #[derive(Clone, Default)]
        struct ItemState(OwnedStakeholderSet);
        impl std::ops::Deref for ItemState {
            type Target = OwnedStakeholderSet;
            fn deref(&self) -> &Self::Target { &self.0 }
        }
        impl std::ops::DerefMut for ItemState {
            fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
        }
        fn shape() -> crate::actor_occupancy::CollisionShape {
            crate::actor_occupancy::CollisionShape { offsets: [
                [-0.125_f64, 0.0, -0.125].map(f64::to_bits),
                [0.125_f64, 0.25, 0.125].map(f64::to_bits)], blocks_building: false, hanging: false }
        }
        impl ActionState for ItemState {
            fn read(&self, view: TruthView, target: Target) -> Option<Cell> { self.0.read(view, target) }
            fn borrow(&self, view: TruthView, target: Target) -> Option<std::borrow::Cow<'_, Cell>> { self.0.borrow(view, target) }
            fn commit(&mut self, context: ActionContext, view: TruthView, writes: &[AppliedWrite]) -> Result<(), ApplyFailure> {
                self.0.commit(context, view, writes)
            }
            fn actor_occupancy_cut(&self, target: Target, value: &Value) -> Result<(crate::realm_completion::PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
                let Value::Entity(body) = value else { return Err(ApplyFailure::Type(target)); };
                Ok((body.owner, crate::actor_occupancy::ActorOccupancyCut {
                    position: body.spatial.position,
                    sample_tick: body.birth.action.tick,
                    simulation_sequence: PlayerSeq(0),
                }))
            }
            fn actor_collision_shape(&self, target: Target, value: &Value) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
                if !matches!(value, Value::Entity(body) if matches!(body.payload, EntityPayload::Item(_))) { return Err(ApplyFailure::Type(target)); }
                Ok(shape())
            }
        }
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let id = GlobalEntityId {
            origin: ServerId(1),
            local_id: 42,
            generation: 2,
        };
        let body_target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(id),
            inventory: None,
        };
        let slot = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 80, z: 1 }),
            inventory: Some(InventoryId::Block { inv: 0, slot: 0 }),
        };
        let before = Cell {
            revision: 7,
            value: Value::Entity((EntityBody { mount: None,
                tags: Default::default(),
                tags_revision: 0,
                relation_versions: Default::default(),
                passengers: Default::default(),
                id,
                spatial_epoch: SpatialEpoch::Action(ActionRef {
                    tick: TickStamp(1),
                    id: identity(),
                }),
                birth: EntityBirth {
                    action: ActionRef {
                        tick: TickStamp(1),
                        id: identity(),
                    },
                    source: crate::realm_completion::PeerGeneration {
                        server: id.origin,
                        generation: 3,
                    },
                    uuid: Uuid::from_u128(42),
                },
                owner: crate::realm_completion::PeerGeneration {
                    server: ServerId(1),
                    generation: 3,
                },
                kind: 1,
                spatial: EntitySpatial {
                    world: chunk.world,
                    position: [0.5_f64.to_bits(), 80.0_f64.to_bits(), 0.5_f64.to_bits()],
                    velocity_micro: [0; 3],
                    yaw: 0,
                    pitch: 0,
                    on_ground: true,
                },
                payload: EntityPayload::Item(DroppedItem {
                    item: 5,
                    count: 2,
                    components: vec![9],
                    age: 0,
                    last_lifetime_tick: None,
                    pickup_delay: 0,
                    health_milli: 5_000,
                    never_despawn: false,
                    never_pickup: false,
                    fire_immune: false,
                }),
            }).into()),
        };
        let empty = Cell {
            revision: 3,
            value: Value::Inventory(InventoryCell::empty(InvLoc { inv: 0, slot: 0 })),
        };
        let mut state = ItemState::default();
        state.local.insert(body_target, before.clone());
        state.local.insert(slot, empty.clone());
        let Value::Entity(controlled_body) = &before.value else {
            unreachable!();
        };
        let occupancy = Target::Chunk { key: chunk, part: ChunkPart::ActorOccupancy, inventory: None };
        state.local.insert(occupancy, Cell { revision: 0, value: Value::ActorOccupancy(
            crate::actor_occupancy::ActorOccupancyRoot::from_entries(vec![crate::actor_occupancy::ActorOccupancyEntry {
                actor: ActorId::Entity(id), identity: crate::actor_occupancy::OccupancyIdentity::Entity(controlled_body.birth.clone()),
                spatial_epoch: controlled_body.spatial_epoch, shape: shape(),
                association: chunk,
                owner: controlled_body.owner,
                cut: crate::actor_occupancy::ActorOccupancyCut {
                    position: controlled_body.spatial.position,
                    sample_tick: controlled_body.birth.action.tick,
                    simulation_sequence: PlayerSeq(0),
                },
            }]).unwrap()) });
        let control = Expected::ActorControlled {
            birth: controlled_body.birth.clone(),
            issuer: controlled_body.owner,
        };
        let referenced = Expected::EntityBirthOwned {
            birth: controlled_body.birth.clone(),
            owner: controlled_body.owner,
        };
        let typed = Expected::EntityBirthKind {
            birth: controlled_body.birth.clone(),
            owner: controlled_body.owner,
            kind: controlled_body.kind,
        };
        assert!(typed.matches(Some(&before)));
        assert!(!typed.matches(None));
        let mut wrong_kind = before.clone();
        let Value::Entity(body) = &mut wrong_kind.value else { unreachable!(); };
        body.kind = body.kind.wrapping_add(1);
        assert!(!typed.matches(Some(&wrong_kind)));
        assert!(referenced.matches(Some(&before)));
        assert!(!referenced.matches(None));
        assert!(control.matches(Some(&before)));
        assert!(!control.matches(None));
        let mut transferred = before.clone();
        let Value::Entity(body) = &mut transferred.value else {
            unreachable!();
        };
        body.owner.generation += 1;
        assert!(!control.matches(Some(&transferred)));
        assert!(!referenced.matches(Some(&transferred)));
        assert!(!typed.matches(Some(&transferred)));
        let Value::Entity(body) = &mut transferred.value else {
            unreachable!();
        };
        body.owner = controlled_body.owner;
        body.birth.uuid = Uuid::from_u128(43);
        assert!(!control.matches(Some(&transferred)));
        assert!(!referenced.matches(Some(&transferred)));
        assert!(!typed.matches(Some(&transferred)));
        let mut pickup = action(
            vec![
                Condition {
                    target: body_target,
                    expected: Expected::Exact(before.clone()),
                },
                Condition {
                    target: slot,
                    expected: Expected::Exact(empty.clone()),
                },
            ],
            vec![
                Effect::Delete {
                    target: body_target,
                },
                Effect::Put {
                    target: slot,
                    value: Value::Inventory(InventoryCell {
                        loc: InvLoc { inv: 0, slot: 0 },
                        item: 5,
                        count: 2,
                        nbt: vec![9],
                    }),
                },
            ],
        );
        pickup.kind = ActionKind::Item;
        let mut entity_pickup = pickup.clone();
        entity_pickup.id.actor = crate::actor::ActorId::Entity(id);
        entity_pickup.id.incarnation = u64::from(id.generation);
        entity_pickup.actor_chunk = Some(chunk);
        let mut scope = crate::actor_action_scope::ActorActionScope::new_entity(
            &state,
            crate::actor_action_scope::FrozenEntityIssueAuthority {
                target: body_target,
                birth: controlled_body.birth.clone(),
                producer: controlled_body.owner,
                controlled_owner: controlled_body.owner,
                effective_tick: controlled_body.birth.action.tick,
                next_sequence: entity_pickup.id.sequence,
            },
            entity_pickup.tick,
            1,
            3,
        )
        .unwrap();
        scope.issue(entity_pickup).unwrap();
        let guarded = scope.finish().pop().unwrap();
        let mut guarded_completion = crate::realm_completion::RealmCompletionLedger::default();
        let successor = crate::realm_completion::PeerGeneration {
            server: ServerId(2),
            generation: 4,
        };
        let participants = BTreeSet::from([controlled_body.owner, successor]);
        let guarded_requirement = guarded_completion
            .capture_chunk(guarded.tick, chunk, participants.clone(), participants)
            .unwrap();
        guarded_completion
            .capture_actor_owner(guarded.tick, guarded.id.actor, successor)
            .unwrap();
        guarded_completion
            .capture_action_issuer(&guarded, controlled_body.owner)
            .unwrap();
        guarded_completion
            .note_action(
                controlled_body.owner,
                &guarded,
                BTreeSet::from([guarded_requirement]),
            )
            .unwrap();
        let guarded_reference = ActionRef {
            tick: guarded.tick,
            id: guarded.id,
        };
        let snapshot = guarded_completion
            .snapshot_for_actions(guarded.tick, &BTreeSet::from([guarded_reference]))
            .unwrap();
        let mut imported_completion = crate::realm_completion::RealmCompletionLedger::default();
        imported_completion
            .import_snapshot(
                &snapshot,
                &BTreeMap::from([(guarded_reference, guarded.clone())]),
            )
            .unwrap();
        assert!(guarded.conditions.contains(&Condition {
            target: body_target,
            expected: control.clone()
        }));
        let mut guarded_graph = crate::action_graph::TickActionGraph::default();
        guarded_graph.insert(guarded.clone()).unwrap();
        let mut replacement = guarded_graph.actions[&guarded.id].factual.clone();
        replacement
            .conditions
            .retain(|condition| !matches!(condition.expected, Expected::ActorControlled { .. }));
        assert_eq!(
            guarded_graph.validate_factual_replacement(
                guarded.id,
                crate::action_graph::SemanticWrapper::Applied,
                &replacement
            ),
            Err(ApplyFailure::InvalidAction)
        );
        let initial_occupancy = state.local[&occupancy].clone();
        let mut dead_state = state.clone();
        dead_state.local.remove(&body_target);
        dead_state.local.insert(occupancy, Cell { revision: 1, value: Value::ActorOccupancy(crate::actor_occupancy::ActorOccupancyRoot::empty()) });
        assert!(prepare_action(&dead_state, &guarded, TruthView::Local).is_err());
        assert_eq!(dead_state.local[&slot], empty);
        let solved = crate::action_solver::solve_component(
            &guarded_graph,
            &BTreeSet::from([guarded.id]),
            &BTreeMap::from([(body_target, None), (slot, Some(empty.clone())), (occupancy, Some(dead_state.local[&occupancy].clone()))]),
            29,
            |_| None,
        )
        .unwrap();
        assert_eq!(
            solved.variants[&guarded.id],
            crate::action_solver::SolvedVariant::Undone
        );
        let applied = apply_action(&mut state, &pickup, TruthView::Local).unwrap();
        assert!(!state.local.contains_key(&body_target));
        let Value::Entity(initial_body) = &before.value else {
            unreachable!();
        };
        let source = initial_body.owner;
        let peers = BTreeSet::from([source]);
        let reference = ActionRef {
            tick: pickup.tick,
            id: pickup.id,
        };
        let mut completion = crate::realm_completion::RealmCompletionLedger::default();
        let requirement = completion
            .capture_chunk(pickup.tick, chunk, peers.clone(), peers)
            .unwrap();
        completion
            .capture_actor_owner(pickup.tick, pickup.id.actor, source)
            .unwrap();
        completion
            .note_action(source, &pickup, BTreeSet::from([requirement]))
            .unwrap();
        completion
            .note_finish(
                source,
                crate::realm_completion::HolisticFinish {
                    full_source_slot: u64::from((pickup.tick).0),
                    acquisition_prefix: None,
                    tick: pickup.tick,
                    issued: BTreeMap::from([(requirement, BTreeSet::from([pickup.id]))]),
                },
            )
            .unwrap();
        let mut graph = crate::action_graph::TickActionGraph::default();
        let mut resolved_pickup = pickup.clone();
        resolved_pickup.factual_result = Some(applied.clone());
        graph.insert(resolved_pickup.clone()).unwrap();
        let node = &graph.actions[&pickup.id];
        let mut evidence = crate::entity_history::EntityHistoryEvidence {
            snapshot_cut: pickup.tick,
            graphs: vec![crate::action_graph::TickGraphSnapshot {
                tick: pickup.tick,
                nodes: vec![crate::action_graph::GraphNodeSnapshot {
                    action: resolved_pickup,
                    actor_binding: None,
                    factual: crate::action_graph::FactualVariant {
                        conditions: pickup.conditions.clone(),
                        effects: pickup.effects.clone(),
                        result: Some(applied.clone()),
                    },
                    wrapper: crate::action_graph::SemanticWrapper::Applied,
                    semantic_order: Some(0),
                    required_realms: node.realms.clone(),
                    applied_realms: node.realms.clone(),
                }],
            }],
            anchors: vec![
                (occupancy, crate::action_graph::GroundAnchor { settled_through: TickStamp(pickup.tick.0.wrapping_sub(1)), value: Some(initial_occupancy) }),
                (
                    body_target,
                    crate::action_graph::GroundAnchor {
                        settled_through: TickStamp(pickup.tick.0.wrapping_sub(1)),
                        value: Some(before.clone()),
                    },
                ),
                (
                    slot,
                    crate::action_graph::GroundAnchor {
                        settled_through: TickStamp(pickup.tick.0.wrapping_sub(1)),
                        value: Some(empty.clone()),
                    },
                ),
            ],
            completion: completion
                .snapshot_for_actions(pickup.tick, &BTreeSet::from([reference]))
                .unwrap(),
        };
        let observed = applied
            .writes
            .iter()
            .map(|write| (write.target, state.local.get(&write.target).cloned()))
            .collect();
        evidence.births().unwrap();
        graph.validate_factual_replacement(pickup.id, crate::action_graph::SemanticWrapper::Applied, &evidence.graphs[0].nodes[0].factual).unwrap();
        assert_eq!(crate::action_graph::project_chunk_graph_snapshot_ground(chunk, evidence.snapshot_cut, &evidence.graphs, &evidence.anchors).unwrap(), observed);
        let mut history_completion = crate::realm_completion::RealmCompletionLedger::default();
        history_completion.import_snapshot(&evidence.completion, &BTreeMap::from([(reference, evidence.graphs[0].nodes[0].action.clone())])).unwrap();
        assert!(history_completion.action_complete(reference));
        for realm in &evidence.graphs[0].nodes[0].required_realms {
            assert!(history_completion.realm_complete(reference.tick, *realm), "incomplete history realm {realm:?}");
        }
        assert_eq!(
            evidence.validate_source(chunk, id, &initial_body.birth, reference, &observed),
            Ok(crate::entity_history::EntityDisposition::Removed)
        );
        let mut partial = graph.clone();
        partial.actions.get_mut(&pickup.id).unwrap().factual =
            evidence.graphs[0].nodes[0].factual.clone();
        let mut historical = crate::action_graph::ActionGraphs::default();
        historical.ticks.insert(pickup.tick, partial);
        historical
            .install_dependency_history(std::sync::Arc::new(evidence.clone()), None)
            .unwrap();
        assert!(
            historical.ticks[&pickup.tick].actions[&pickup.id]
                .applied_realms
                .is_empty()
        );
        let mut conflicting = evidence.clone();
        conflicting.graphs[0].nodes[0].factual.effects.clear();
        assert!(
            historical
                .install_dependency_history(std::sync::Arc::new(conflicting), None)
                .is_err()
        );
        let finish = evidence.completion.finishes.pop().unwrap();
        assert!(
            evidence
                .validate_source(chunk, id, &initial_body.birth, reference, &observed)
                .is_err()
        );
        evidence.completion.finishes.push(finish);
        evidence.graphs[0].nodes[0].applied_realms.clear();
        assert!(
            evidence
                .validate_source(chunk, id, &initial_body.birth, reference, &observed)
                .is_err()
        );
        assert!(
            matches!(&state.local[&slot].value, Value::Inventory(item) if item.count == 2 && item.nbt == [9])
        );
        let inverse = prepare_undo(&state, &applied).unwrap();
        assert_eq!(
            prepare_semantic_undo(&state, &pickup, &applied).unwrap(),
            inverse
        );
        state
            .commit(
                ActionContext {
                    id: applied.id,
                    tick: applied.tick,
                    actor_chunk: None,
                },
                TruthView::Local,
                &inverse,
            )
            .unwrap();
        assert_eq!(state.local[&body_target], before);
        assert_eq!(state.local[&slot], empty);
        if let Effect::Put {
            value: Value::Inventory(item),
            ..
        } = &mut pickup.effects[1]
        {
            item.count = 3;
        }
        assert_eq!(
            prepare_action(&state, &pickup, TruthView::Local),
            Err(ApplyFailure::InventoryConservation)
        );
        if let Effect::Put {
            value: Value::Inventory(item),
            ..
        } = &mut pickup.effects[1]
        {
            item.count = 2;
            item.nbt = vec![8];
        }
        assert_eq!(
            prepare_action(&state, &pickup, TruthView::Local),
            Err(ApplyFailure::InventoryConservation)
        );
        assert_eq!(state.local[&body_target], before);

        pickup.effects = vec![Effect::MoveItems {
            source: body_target,
            destination: slot,
            count: 1,
            max_stack: 64,
        }];
        let intent = InventoryReplayIntent {
            source: body_target,
            item: 5,
            components: vec![9],
            count: InventoryCountRule::All,
            ordered_destinations: vec![InventoryReplayCandidate {
                target: slot,
                max_stack: 64,
            }],
            selector_revision: 0,
            destination_policy: InventoryDestinationPolicy::Ordered,
            equipment: Vec::new(),
        };
        let mut replay_pickup = pickup.clone();
        replay_pickup.class = ActionClass::Replayable;
        replay_pickup.effects = intent.derive_effects(&state.local).unwrap();
        replay_pickup.semantic_intent = Some(SemanticIntent::InventoryMove(intent.clone()));
        replay_pickup.counterfactual_conditions = vec![body_target, slot];
        replay_pickup.counterfactual_effects = vec![body_target, slot];
        replay_pickup.conditions.push(Condition {
            target: body_target,
            expected: Expected::DroppedItem {
                birth: controlled_body.birth.clone(),
                condition: crate::entity_body::DroppedItemCondition::PickupReady,
            },
        });
        let mut replay_graph = crate::action_graph::TickActionGraph::default();
        replay_graph.insert(replay_pickup.clone()).unwrap();
        let mut changed = before.clone();
        changed.revision += 1;
        let Value::Entity(body) = &mut changed.value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.age += 1;
        let mut anchors =
            BTreeMap::from([(body_target, Some(changed)), (slot, Some(empty.clone()))]);
        let solved = crate::action_solver::solve_component(
            &replay_graph,
            &BTreeSet::from([replay_pickup.id]),
            &anchors,
            29,
            |_| None,
        )
        .unwrap();
        assert!(
            matches!(&solved.variants[&replay_pickup.id], crate::action_solver::SolvedVariant::Applied { replayed: true, conditions, .. } if conditions.iter().any(|condition| matches!(&condition.expected, Expected::DroppedItem { birth, .. } if *birth == controlled_body.birth)))
        );
        let Value::Entity(body) = &mut anchors
            .get_mut(&body_target)
            .unwrap()
            .as_mut()
            .unwrap()
            .value
        else {
            unreachable!();
        };
        body.birth.uuid = Uuid::from_u128(43);
        let solved = crate::action_solver::solve_component(
            &replay_graph,
            &BTreeSet::from([replay_pickup.id]),
            &anchors,
            29,
            |_| None,
        )
        .unwrap();
        assert_eq!(
            solved.variants[&replay_pickup.id],
            crate::action_solver::SolvedVariant::Undone
        );
        let mut blocked = state.local.clone();
        let Value::Entity(body) = &mut blocked.get_mut(&body_target).unwrap().value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.pickup_delay = 1;
        assert_eq!(
            intent.derive_effects(&blocked),
            Err(InventoryReplayFailure::MissingCell(body_target))
        );
        let Value::Entity(body) = &mut blocked.get_mut(&body_target).unwrap().value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.pickup_delay = 0;
        item.never_pickup = false;
        item.health_milli = 0;
        assert_eq!(
            intent.derive_effects(&blocked),
            Err(InventoryReplayFailure::MissingCell(body_target))
        );
        let Value::Entity(body) = &mut blocked.get_mut(&body_target).unwrap().value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.health_milli = 5000;
        let Value::Entity(body) = &mut blocked.get_mut(&body_target).unwrap().value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.pickup_delay = 0;
        item.never_pickup = true;
        assert_eq!(
            intent.derive_effects(&blocked),
            Err(InventoryReplayFailure::MissingCell(body_target))
        );
        let partial = apply_action(&mut state, &pickup, TruthView::Local).unwrap();
        let Value::Entity(remaining) = &state.local[&body_target].value else {
            panic!("lost body");
        };
        let Value::Entity(original) = &before.value else {
            unreachable!();
        };
        assert_eq!(remaining.birth, original.birth);
        assert_eq!(remaining.spatial, original.spatial);
        assert!(matches!(&remaining.payload, EntityPayload::Item(item) if item.count == 1));
        let effects = intent.derive_effects(&state.local).unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::MoveItems { count: 1, .. }]
        ));
        pickup.conditions = vec![
            Condition {
                target: body_target,
                expected: Expected::Exact(state.local[&body_target].clone()),
            },
            Condition {
                target: slot,
                expected: Expected::Exact(state.local[&slot].clone()),
            },
        ];
        pickup.effects = effects;
        apply_action(&mut state, &pickup, TruthView::Local).unwrap();
        assert!(!state.local.contains_key(&body_target));
        assert!(matches!(&state.local[&slot].value, Value::Inventory(item) if item.count == 2));
        assert_eq!(partial.writes.len(), 2);
        let destination_chunk = ChunkKey::new(chunk.world, 1, 0);
        let destination_body = Target::Chunk {
            key: destination_chunk,
            part: ChunkPart::Entity(id),
            inventory: None,
        };
        let Value::Entity(original) = &before.value else {
            unreachable!();
        };
        let mut spatial = original.spatial.clone();
        spatial.position[0] = 17.0_f64.to_bits();
        let mut transition = action(
            Vec::new(),
            vec![Effect::MoveEntityBody {
                source: body_target,
                destination: destination_body,
                spatial: spatial.clone(),
                authority: None,
            }],
        );
        transition.class = ActionClass::Infallible;
        transition.kind = ActionKind::Actor;
        transition.chunk_transition = true;
        transition.actor_chunk = Some(chunk);
        transition.id.actor = ActorId::Entity(id);
        transition.id.incarnation = u64::from(id.generation);
        transition.id.family = 8;
        transition.stakeholders = vec![
            Stakeholder::Chunk(chunk),
            Stakeholder::Chunk(destination_chunk),
        ];
        let absent = state.local.clone();
        let no_resurrection = apply_action(&mut state, &transition, TruthView::Local).unwrap();
        assert!(no_resurrection.writes.is_empty());
        assert_eq!(state.local, absent);
        state.local.insert(body_target, before.clone());
        let moved = apply_action(&mut state, &transition, TruthView::Local).unwrap();
        assert!(!state.local.contains_key(&body_target));
        let Value::Entity(body) = &state.local[&destination_body].value else {
            panic!("missing moved body");
        };
        assert_eq!(body.birth, original.birth);
        assert_eq!(body.payload, original.payload);
        assert_eq!(body.owner, original.owner);
        assert_eq!(body.spatial, spatial);
        let reference = ActionRef {
            tick: transition.tick,
            id: transition.id,
        };
        let source = original.owner;
        let mut completion = crate::realm_completion::RealmCompletionLedger::default();
        completion
            .capture_actor_owner(transition.tick, transition.id.actor, source)
            .unwrap();
        let requirements = [chunk, destination_chunk]
            .map(|key| {
                crate::realm_completion::RequirementKey::chunk(key, &BTreeSet::from([source]))
            })
            .into_iter()
            .collect::<BTreeSet<_>>();
        for requirement in &requirements {
            completion
                .capture(transition.tick, *requirement, BTreeSet::from([source]))
                .unwrap();
        }
        completion
            .note_action(source, &transition, requirements.clone())
            .unwrap();
        completion
            .note_finish(
                source,
                crate::realm_completion::HolisticFinish {
                    full_source_slot: u64::from((transition.tick).0),
                    acquisition_prefix: None,
                    tick: transition.tick,
                    issued: requirements
                        .iter()
                        .map(|key| (*key, BTreeSet::from([transition.id])))
                        .collect(),
                },
            )
            .unwrap();
        let mut graph = crate::action_graph::TickActionGraph::default();
        graph.insert(transition.clone()).unwrap();
        let source_realm = body_target.action_realm();
        let evidence = crate::entity_history::EntityHistoryEvidence {
            snapshot_cut: transition.tick,
            graphs: vec![crate::action_graph::TickGraphSnapshot {
                tick: transition.tick,
                nodes: vec![crate::action_graph::GraphNodeSnapshot {
                    action: transition.clone(),
                    actor_binding: None,
                    factual: crate::action_graph::FactualVariant {
                        conditions: Vec::new(),
                        effects: transition.effects.clone(),
                        result: Some(moved.clone()),
                    },
                    wrapper: crate::action_graph::SemanticWrapper::Applied,
                    semantic_order: Some(0),
                    required_realms: graph.actions[&transition.id].realms.clone(),
                    applied_realms: BTreeSet::from([source_realm]),
                }],
            }],
            anchors: vec![
                (
                    body_target,
                    crate::action_graph::GroundAnchor {
                        settled_through: TickStamp(transition.tick.0.wrapping_sub(1)),
                        value: Some(before.clone()),
                    },
                ),
                (
                    destination_body,
                    crate::action_graph::GroundAnchor {
                        settled_through: TickStamp(transition.tick.0.wrapping_sub(1)),
                        value: None,
                    },
                ),
            ],
            completion: completion
                .snapshot_for_actions(transition.tick, &BTreeSet::from([reference]))
                .unwrap(),
        };
        assert_eq!(
            evidence.validate_source(
                chunk,
                id,
                &original.birth,
                reference,
                &BTreeMap::from([(body_target, None)])
            ),
            Ok(crate::entity_history::EntityDisposition::Located(
                destination_chunk,
                source
            ))
        );
        let departures = evidence.fixed_departures().unwrap();
        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].0, id);
        assert_eq!(departures[0].1.before, Some((chunk, source)));
        assert_eq!(departures[0].1.after, Some((destination_chunk, source)));
        let mut inconsistent = evidence.clone();
        let source_write = inconsistent.graphs[0].nodes[0]
            .factual
            .result
            .as_mut()
            .unwrap()
            .writes
            .iter_mut()
            .find(|write| write.target == body_target)
            .unwrap();
        source_write.before.as_mut().unwrap().revision += 1;
        assert!(inconsistent.fixed_departures().is_err());
        let payload = crate::xfer::ChunkPayload::new(
            chunk,
            source.server.0,
            transition.tick,
            Vec::new(),
            Vec::new(),
            vec![source.server.0],
        )
        .with_entity_history(vec![evidence.clone()]);
        let decoded =
            crate::xfer::decode_payload(&crate::xfer::encode_payload(&payload).unwrap()).unwrap();
        assert_eq!(decoded.entity_history, payload.entity_history);
        assert_eq!(
            decoded.entity_history[0].fixed_departures().unwrap(),
            departures
        );
        assert!(
            evidence
                .validate_source(
                    chunk,
                    id,
                    &original.birth,
                    reference,
                    &BTreeMap::from([(body_target, Some(before.clone()))])
                )
                .is_err()
        );
        let undo = prepare_undo(&state, &moved).unwrap();
        state
            .commit(
                ActionContext {
                    id: moved.id,
                    tick: moved.tick,
                    actor_chunk: Some(chunk),
                },
                TruthView::Local,
                &undo,
            )
            .unwrap();
        assert_eq!(state.local[&body_target], before);
        assert!(!state.local.contains_key(&destination_body));
        let mut lifetime = action(
            Vec::new(),
            vec![Effect::ChangeDroppedItem {
                target: body_target,
                issuer: source,
                body_owner: original.owner,
                birth: original.birth.clone(),
                change: crate::entity_body::DroppedItemChange::AdvanceLifetime,
            }],
        );
        lifetime.class = ActionClass::Infallible;
        lifetime.kind = ActionKind::Item;
        lifetime.id.actor = ActorId::Entity(id);
        lifetime.id.incarnation = u64::from(id.generation);
        lifetime.actor_chunk = Some(chunk);
        lifetime.stakeholders = vec![Stakeholder::Chunk(chunk)];
        let mut superseded = lifetime.clone();
        let Effect::ChangeDroppedItem {
            body_owner: issuer, ..
        } = &mut superseded.effects[0]
        else {
            unreachable!();
        };
        issuer.generation += 1;
        assert!(
            apply_action(&mut state, &superseded, TruthView::Local)
                .unwrap()
                .writes
                .is_empty()
        );
        assert_eq!(state.local[&body_target], before);
        assert_eq!(
            completion.note_action(source, &superseded, BTreeSet::new()),
            Err(crate::realm_completion::CompletionFailure::InvalidActionRequirements)
        );
        let mut wrong_birth = lifetime.clone();
        let Effect::ChangeDroppedItem { birth, .. } = &mut wrong_birth.effects[0] else {
            unreachable!();
        };
        birth.uuid = uuid::Uuid::from_u128(original.birth.uuid.as_u128() ^ 1);
        assert_eq!(
            prepare_action(&state, &wrong_birth, TruthView::Local),
            Err(ApplyFailure::Type(body_target))
        );
        assert_eq!(state.local[&body_target], before);
        let advanced = apply_action(&mut state, &lifetime, TruthView::Local).unwrap();
        let mut primary_state = OwnedStakeholderSet::default();
        primary_state.local.insert(body_target, before.clone());
        let mut primary_lifetime = lifetime.clone();
        let primary = crate::realm_completion::PeerGeneration {
            server: crate::identity::ServerId(source.server.0.wrapping_add(1)),
            generation: source.generation + 1,
        };
        primary_lifetime.id.actor = ActorId::Entity(id);
        primary_lifetime.id.incarnation = u64::from(id.generation);
        let Effect::ChangeDroppedItem { issuer, .. } = &mut primary_lifetime.effects[0] else {
            unreachable!();
        };
        *issuer = primary;
        let primary_result =
            apply_action(&mut primary_state, &primary_lifetime, TruthView::Local).unwrap();
        assert_eq!(primary_result.writes, advanced.writes);
        let spatial_guard = Expected::SpatialEpochEquals(original.spatial_epoch);
        assert!(spatial_guard.matches(primary_state.local.get(&body_target)));
        let mut moved = primary_state.local[&body_target].clone();
        let Value::Entity(body) = &mut moved.value else {
            unreachable!()
        };
        body.spatial_epoch = SpatialEpoch::Action(ActionRef {
            tick: primary_lifetime.tick,
            id: primary_lifetime.id,
        });
        assert!(!spatial_guard.matches(Some(&moved)));
        let mut repeated_lifetime = lifetime.clone();
        repeated_lifetime.id.sequence = repeated_lifetime.id.sequence.wrapping_add(1);
        assert!(
            apply_action(&mut state, &repeated_lifetime, TruthView::Local)
                .unwrap()
                .writes
                .is_empty()
        );
        assert_eq!(
            state.local[&body_target],
            advanced.writes[0].after.clone().unwrap()
        );
        let Value::Entity(aged) = &state.local[&body_target].value else {
            unreachable!();
        };
        assert_eq!(aged.birth, original.birth);
        assert_eq!(aged.spatial, original.spatial);
        let EntityPayload::Item(aged_item) = &aged.payload else {
            unreachable!();
        };
        let EntityPayload::Item(original_item) = &original.payload else {
            unreachable!();
        };
        assert_eq!(
            aged_item.age,
            original_item.age
                + u32::from(
                    lifetime.tick.distance_since(
                        original_item
                            .last_lifetime_tick
                            .unwrap_or(original.birth.action.tick)
                    )
                )
        );
        assert_eq!(aged_item.last_lifetime_tick, Some(lifetime.tick));
        let mut damage = action(
            vec![Condition {
                target: body_target,
                expected: Expected::DroppedItem {
                    birth: original.birth.clone(),
                    condition: crate::entity_body::DroppedItemCondition::Alive,
                },
            }],
            vec![Effect::ChangeDroppedItem {
                target: body_target,
                issuer: source,
                body_owner: original.owner,
                birth: original.birth.clone(),
                change: crate::entity_body::DroppedItemChange::Damage {
                    amount_milli: u32::MAX,
                    fire: false,
                },
            }],
        );
        damage.kind = ActionKind::Item;
        damage.stakeholders = vec![Stakeholder::Chunk(chunk)];
        assert!(damage.conditions[0].expected.matches(Some(&before)));
        let mut wrong_birth = before.clone();
        let Value::Entity(wrong_body) = &mut wrong_birth.value else {
            unreachable!();
        };
        wrong_body.birth.uuid = Uuid::from_u128(998);
        assert!(!damage.conditions[0].expected.matches(Some(&wrong_birth)));
        let destroyed = apply_action(&mut state, &damage, TruthView::Local).unwrap();
        assert!(!state.local.contains_key(&body_target));
        let inverse = prepare_semantic_undo(&state, &damage, &destroyed).unwrap();
        state
            .commit(
                ActionContext {
                    id: damage.id,
                    tick: damage.tick,
                    actor_chunk: damage.actor_chunk,
                },
                TruthView::Local,
                &inverse,
            )
            .unwrap();
        assert_eq!(
            state.local[&body_target],
            advanced.writes[0].after.clone().unwrap()
        );
        damage.effects = vec![Effect::ChangeDroppedItem {
            target: body_target,
            issuer: source,
            body_owner: original.owner,
            birth: original.birth.clone(),
            change: crate::entity_body::DroppedItemChange::Expire,
        }];
        assert_eq!(damage.validate(), Err(ApplyFailure::Condition(body_target)));
        let other_id = crate::protocol::GlobalEntityId {
            local_id: id.local_id + 1,
            ..id
        };
        let other_target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(other_id),
            inventory: None,
        };
        let mut other_body = original.clone();
        other_body.id = other_id;
        other_body.birth.uuid = Uuid::from_u128(999);
        let other_cell = Cell {
            revision: 17,
            value: Value::Entity((other_body).into()),
        };
        state.local.insert(other_target, other_cell.clone());
        let before_merge = state.local.clone();
        let mut merge = action(
            vec![
                Condition {
                    target: body_target,
                    expected: Expected::Exact(state.local[&body_target].clone()),
                },
                Condition {
                    target: other_target,
                    expected: Expected::Exact(other_cell),
                },
            ],
            vec![Effect::MergeDroppedItems {
                source: other_target,
                destination: body_target,
                max_stack: 64,
                count: original_item.count,
            }],
        );
        merge.kind = ActionKind::Item;
        let merged = apply_action(&mut state, &merge, TruthView::Local).unwrap();
        assert!(!state.local.contains_key(&other_target));
        let Value::Entity(merged_body) = &state.local[&body_target].value else {
            unreachable!();
        };
        assert_eq!(merged_body.birth, original.birth);
        assert!(matches!(&merged_body.payload, EntityPayload::Item(item) if item.count == 4));
        let inverse = prepare_semantic_undo(&state, &merge, &merged).unwrap();
        state
            .commit(
                ActionContext {
                    id: merge.id,
                    tick: merge.tick,
                    actor_chunk: merge.actor_chunk,
                },
                TruthView::Local,
                &inverse,
            )
            .unwrap();
        assert_eq!(state.local, before_merge);
        damage.effects = vec![Effect::ChangeDroppedItem {
            target: body_target,
            issuer: source,
            body_owner: original.owner,
            birth: original.birth.clone(),
            change: crate::entity_body::DroppedItemChange::Damage {
                amount_milli: 1000,
                fire: false,
            },
        }];
        let anchors = BTreeMap::from([(body_target, Some(before.clone()))]);
        let mut expected_solution = None;
        for arrivals in [
            [lifetime.clone(), damage.clone()],
            [damage.clone(), lifetime.clone()],
        ] {
            let mut graph = crate::action_graph::TickActionGraph::default();
            for incoming in arrivals {
                graph.insert(incoming).unwrap();
            }
            let solved = crate::action_solver::solve_component(
                &graph,
                &BTreeSet::from([lifetime.id, damage.id]),
                &anchors,
                29,
                |_| Some(true),
            )
            .unwrap();
            assert!(solved.variants.values().all(|variant| matches!(
                variant,
                crate::action_solver::SolvedVariant::Applied { .. }
            )));
            if let Some(expected) = &expected_solution {
                assert_eq!(&solved, expected);
            } else {
                expected_solution = Some(solved);
            }
        }
        lifetime.id.sequence = u16::MAX;
        transition.id.sequence = 0;
        for arrivals in [
            [lifetime.clone(), transition.clone()],
            [transition.clone(), lifetime.clone()],
        ] {
            let mut graph = crate::action_graph::TickActionGraph::default();
            for incoming in arrivals {
                graph.insert(incoming).unwrap();
            }
            let solved = crate::action_solver::solve_component(
                &graph,
                &BTreeSet::from([lifetime.id, transition.id]),
                &BTreeMap::from([
                    (body_target, Some(before.clone())),
                    (destination_body, None),
                ]),
                29,
                |_| Some(true),
            )
            .unwrap();
            assert_eq!(solved.causal_order, [lifetime.id, transition.id]);
            let crate::action_solver::SolvedVariant::Applied { result, .. } =
                &solved.variants[&transition.id]
            else {
                unreachable!();
            };
            let moved = result
                .writes
                .iter()
                .find(|write| write.target == destination_body)
                .unwrap()
                .after
                .as_ref()
                .unwrap();
            let Value::Entity(body) = &moved.value else {
                unreachable!();
            };
            assert_eq!(body.birth, original.birth);
            assert_eq!(body.spatial, spatial);
            assert!(
                matches!(&body.payload, EntityPayload::Item(item) if item.age == original_item.age + u32::from(
                    lifetime.tick.distance_since(original_item.last_lifetime_tick.unwrap_or(original.birth.action.tick))))
            );
        }
        let source_view = OwnedStakeholderSet {
            local: BTreeMap::from([(body_target, before.clone())]),
            ground: BTreeMap::new(),
        };
        let authority = crate::actor_action_scope::FrozenEntityIssueAuthority {
            target: body_target,
            birth: original.birth.clone(),
            producer: original.owner,
            controlled_owner: original.owner,
            effective_tick: original.birth.action.tick,
            next_sequence: lifetime.id.sequence,
        };
        let mut superseded = authority.clone();
        superseded.controlled_owner.generation += 1;
        assert!(matches!(
            crate::actor_action_scope::ActorActionScope::new_entity(
                &source_view,
                superseded,
                lifetime.tick,
                2,
                2,
            ),
            Err(crate::actor_action_scope::ScopeFailure::Identity)
        ));
        let mut future = authority.clone();
        future.effective_tick = TickStamp(lifetime.tick.0.wrapping_add(1));
        assert!(matches!(
            crate::actor_action_scope::ActorActionScope::new_entity(
                &source_view,
                future,
                lifetime.tick,
                2,
                2,
            ),
            Err(crate::actor_action_scope::ScopeFailure::Identity)
        ));
        let mut scope = crate::actor_action_scope::ActorActionScope::new_entity(
            &source_view,
            authority.clone(),
            lifetime.tick,
            2,
            2,
        )
        .unwrap();
        assert!(matches!(
            scope.read(body_target),
            Some(std::borrow::Cow::Borrowed(_))
        ));
        scope.issue(lifetime.clone()).unwrap();
        assert_eq!(scope.next_sequence(), Some(0));
        let mut duplicate_lifetime = lifetime.clone();
        duplicate_lifetime.id.sequence = 0;
        assert_eq!(
            scope.issue(duplicate_lifetime).unwrap_err().0,
            crate::actor_action_scope::ScopeFailure::DuplicateLifetime
        );
        assert_eq!(scope.next_sequence(), Some(0));
        let mut aliased = transition.clone();
        aliased.id.sequence = lifetime.id.sequence;
        assert_eq!(
            scope.issue(aliased).unwrap_err().0,
            crate::actor_action_scope::ScopeFailure::IssueOrder
        );
        let mut reversed = transition.clone();
        reversed.id.sequence = lifetime.id.sequence.wrapping_sub(1);
        assert_eq!(
            scope.issue(reversed).unwrap_err().0,
            crate::actor_action_scope::ScopeFailure::IssueOrder
        );
        scope.issue(transition.clone()).unwrap();
        assert_eq!(scope.next_sequence(), Some(1));
        assert!(scope.read(body_target).is_none());
        assert!(scope.read(destination_body).is_some());
        let mut later = lifetime.clone();
        later.id.sequence = 1;
        assert_eq!(
            scope.issue(later).unwrap_err().0,
            crate::actor_action_scope::ScopeFailure::TransitionLast
        );
        assert_eq!(source_view.local[&body_target], before);
        assert!(!source_view.local.contains_key(&destination_body));
        let emitted = scope.finish();
        assert_eq!(emitted.len(), 2);
        assert!(emitted[1].dependencies.contains(&ActionRef {
            tick: lifetime.tick,
            id: lifetime.id
        }));
        assert!(emitted.iter().all(|action| action.factual_result.is_some()));
        let mut bounded_scope = crate::actor_action_scope::ActorActionScope::new_entity(
            &source_view,
            authority,
            lifetime.tick,
            2,
            1,
        )
        .unwrap();
        bounded_scope.issue(lifetime.clone()).unwrap();
        let (failure, retained) = bounded_scope.issue(transition.clone()).unwrap_err();
        assert_eq!(failure, crate::actor_action_scope::ScopeFailure::Capacity);
        assert_eq!(retained.id, transition.id);
        assert!(bounded_scope.read(body_target).is_some());
        assert!(bounded_scope.read(destination_body).is_none());
        assert_eq!(bounded_scope.finish().len(), 1);
        assert_eq!(source_view.local[&body_target], before);
    }

    fn action(conditions: Vec<Condition>, effects: Vec<Effect>) -> Action {
        let player_leases: Vec<LivePlayerLease> = conditions
            .iter()
            .map(|condition| condition.target)
            .chain(effects.iter().flat_map(Effect::targets))
            .filter_map(|target| match target {
                Target::Player {
                    lease: Some(lease),
                    inventory: Some(_),
                    ..
                } => Some(lease),
                _ => None,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let player_chunks = player_leases
            .iter()
            .map(|lease: &LivePlayerLease| (lease.key, ChunkKey::new(WorldId::new(0, 0), 0, 0)))
            .collect::<Vec<_>>();
        let stakeholders = conditions
            .iter()
            .map(|condition| condition.target.stakeholder())
            .chain(
                player_chunks
                    .iter()
                    .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Action {
            edit_authority: None,
            id: identity(),
            tick: TickStamp(8),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases,
            player_chunks,
            stakeholders,
            dependencies: Vec::new(),
            conditions,
            effects,
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Block,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        }
    }

    #[test]
    fn actor_private_block_updates_project_section_without_publishing() {
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let block = Target::Chunk {
            key,
            part: ChunkPart::Block(crate::protocol::BlockPos { x: 0, y: 0, z: 0 }),
            inventory: None,
        };
        let section = Target::Chunk {
            key,
            part: ChunkPart::BlockSection(0),
            inventory: None,
        };
        let before = Cell {
            revision: 0,
            value: Value::Block(0),
        };
        let owners = OwnedStakeholderSet {
            local: BTreeMap::from([
                (block, before.clone()),
                (
                    section,
                    Cell {
                        revision: 9,
                        value: Value::BlockSection(vec![0; 4096]),
                    },
                ),
            ]),
            ground: BTreeMap::new(),
        };
        let mut first = action(
            vec![Condition {
                target: block,
                expected: Expected::Exact(before.clone()),
            }],
            vec![Effect::Put {
                target: block,
                value: Value::Block(1),
            }],
        );
        first.actor_chunk = Some(key);
        let mut draft = crate::actor_action_scope::ActorActionDraft::new(
            first.id.actor,
            first.id.incarnation,
            first.tick,
            2,
            2,
        );
        draft.issue(&owners, first.clone()).unwrap();
        let section_after = draft.view(&owners).read(TruthView::Local, section).unwrap();
        assert_eq!(section_after.revision, 10);
        let Value::BlockSection(palette) = &section_after.value else {
            unreachable!()
        };
        assert_eq!(palette[0], 1);
        assert_eq!(owners.local[&block], before);
        assert_eq!(owners.local[&section].revision, 9);
        let mut second = action(
            vec![Condition {
                target: section,
                expected: Expected::RevisionEquals(10),
            }],
            vec![Effect::Put {
                target: block,
                value: Value::Block(2),
            }],
        );
        second.id.sequence = first.id.sequence.wrapping_add(1);
        second.actor_chunk = Some(key);
        second.conditions.push(Condition {
            target: block,
            expected: Expected::Exact(Cell {
                revision: 1,
                value: Value::Block(1),
            }),
        });
        draft.issue(&owners, second).unwrap();
        assert_eq!(
            draft
                .view(&owners)
                .read(TruthView::Local, section)
                .unwrap()
                .revision,
            11
        );
        assert!(draft.finish()[1].dependencies.contains(&ActionRef {
            tick: first.tick,
            id: first.id
        }));
    }

    #[test]
    fn bounded_joint_footprint_accepts_dense_effects_and_rejects_overflow() {
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let targets = (0..4096)
            .map(|index| Target::Chunk {
                key,
                part: ChunkPart::Block(crate::protocol::BlockPos {
                    x: index & 15,
                    y: index >> 8,
                    z: (index >> 4) & 15,
                }),
                inventory: None,
            })
            .collect::<Vec<_>>();
        let conditions = targets
            .iter()
            .map(|target| Condition {
                target: *target,
                expected: Expected::Exact(Cell {
                    revision: 0,
                    value: Value::Block(0),
                }),
            })
            .collect();
        let effects = targets
            .iter()
            .map(|target| Effect::Put {
                target: *target,
                value: Value::Block(1),
            })
            .collect();
        let mut dense = action(conditions, effects);
        dense.validate().unwrap();
        dense.effects.push(dense.effects[0].clone());
        assert_eq!(dense.validate(), Err(ApplyFailure::InvalidAction));
        dense.effects.pop();
        dense.conditions.push(dense.conditions[0].clone());
        assert_eq!(dense.validate(), Err(ApplyFailure::InvalidAction));
    }

    #[test]
    fn infallible_action_applies_against_a_changed_local_cell() {
        let (_, block) = targets();
        let mut update = action(
            Vec::new(),
            vec![Effect::Put {
                target: block,
                value: Value::Block(9),
            }],
        );
        update.class = ActionClass::Infallible;
        update.stakeholders = vec![block.stakeholder()];
        let mut state = OwnedStakeholderSet {
            local: BTreeMap::from([(
                block,
                Cell {
                    revision: 4,
                    value: Value::Block(7),
                },
            )]),
            ground: BTreeMap::new(),
        };
        let applied = apply_action(&mut state, &update, TruthView::Local).unwrap();
        assert_eq!(
            applied.writes[0].before.as_ref().map(|cell| &cell.value),
            Some(&Value::Block(7))
        );
        assert_eq!(state.local[&block].value, Value::Block(9));
    }

    #[test]
    fn stale_live_inventory_lease_cannot_mutate_a_reused_player_slot() {
        struct LeasedState {
            cells: OwnedStakeholderSet,
            current: LivePlayerLease,
        }

        impl ActionState for LeasedState {
            fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
                self.cells.read(view, target)
            }

            fn validate_live_player_lease(&self, lease: LivePlayerLease) -> bool {
                lease == self.current
            }

            fn commit(
                &mut self,
                context: ActionContext,
                view: TruthView,
                writes: &[AppliedWrite],
            ) -> Result<(), ApplyFailure> {
                self.cells.commit(context, view, writes)
            }
        }

        let key = PlayerKey(Uuid::from_u128(17));
        let gid = GlobalPlayerId {
            server: ServerId(1),
            player: PlayerSlot(3),
        };
        let target = Target::Player {
            key,
            lease: Some(LivePlayerLease {
                key,
                gid,
                incarnation: 9,
            }),
            inventory: Some(InventoryId::Player { inv: 0, slot: 0 }),
        };
        let cell = Cell {
            revision: 4,
            value: Value::Inventory(InventoryCell {
                loc: InvLoc { inv: 0, slot: 0 },
                item: 1,
                count: 2,
                nbt: Vec::new(),
            }),
        };
        let stale = LivePlayerLease {
            key,
            gid,
            incarnation: 8,
        };
        let mut action = action(
            vec![Condition {
                target,
                expected: Expected::Exact(cell.clone()),
            }],
            vec![Effect::Put {
                target,
                value: cell.value.clone(),
            }],
        );
        action.player_leases = vec![stale];
        let mut state = LeasedState {
            cells: OwnedStakeholderSet {
                local: BTreeMap::from([(target, cell.clone())]),
                ground: BTreeMap::new(),
            },
            current: LivePlayerLease {
                incarnation: 9,
                ..stale
            },
        };
        assert_eq!(
            apply_action(&mut state, &action, TruthView::Local),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(state.cells.local.get(&target), Some(&cell));
        action.player_leases = vec![state.current];
        assert!(apply_action(&mut state, &action, TruthView::Local).is_ok());
    }

    #[test]
    fn joint_action_with_late_failed_condition_leaves_both_owners_unchanged() {
        let (player, block) = targets();
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(
            player,
            Cell {
                revision: 4,
                value: Value::Scalar(8),
            },
        );
        state.local.insert(
            block,
            Cell {
                revision: 2,
                value: Value::Block(1),
            },
        );
        let before = state.local.clone();
        let action = action(
            vec![
                Condition {
                    target: player,
                    expected: Expected::Exact(before.get(&player).cloned().unwrap()),
                },
                Condition {
                    target: block,
                    expected: Expected::Exact(Cell {
                        revision: 2,
                        value: Value::Block(2),
                    }),
                },
            ],
            vec![
                Effect::Add {
                    target: player,
                    amount: -1,
                },
                Effect::Put {
                    target: block,
                    value: Value::Block(3),
                },
            ],
        );
        assert_eq!(
            apply_action(&mut state, &action, TruthView::Local),
            Err(ApplyFailure::Condition(block))
        );
        assert_eq!(state.local, before);
    }

    #[test]
    fn undo_refuses_to_erase_a_later_dependent_write() {
        let (player, block) = targets();
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(
            player,
            Cell {
                revision: 0,
                value: Value::Scalar(5),
            },
        );
        let first = action(
            vec![Condition {
                target: player,
                expected: Expected::Exact(state.local.get(&player).cloned().unwrap()),
            }],
            vec![Effect::Add {
                target: player,
                amount: 2,
            }],
        );
        let applied = apply_action(&mut state, &first, TruthView::Local).unwrap();
        let next = action(
            vec![Condition {
                target: player,
                expected: Expected::Exact(state.local.get(&player).cloned().unwrap()),
            }],
            vec![Effect::Add {
                target: player,
                amount: 3,
            }],
        );
        let _ = apply_action(&mut state, &next, TruthView::Local).unwrap();
        assert_eq!(
            undo_action(&mut state, &applied),
            Err(InvariantFailure::ChangedSinceApply(player))
        );
        assert_eq!(state.local.get(&player).unwrap().value, Value::Scalar(10));
        assert!(!state.local.contains_key(&block));
    }

    #[test]
    fn rejected_optimistic_break_restores_the_accepted_ground_block() {
        let (_, block) = targets();
        let mut state = OwnedStakeholderSet::default();
        let dirt = Cell {
            revision: 1,
            value: Value::Block(2),
        };
        let wood = Cell {
            revision: 2,
            value: Value::Block(5),
        };
        state.local.insert(block, dirt.clone());
        state.ground.insert(block, dirt.clone());
        let break_action = action(
            vec![Condition {
                target: block,
                expected: Expected::BlockState(2),
            }],
            vec![Effect::Put {
                target: block,
                value: Value::Block(0),
            }],
        );
        let optimistic = apply_action(&mut state, &break_action, TruthView::Local).unwrap();
        state.ground.insert(block, wood.clone());
        assert_eq!(
            prepare_action(&state, &break_action, TruthView::Ground),
            Err(ApplyFailure::Condition(block))
        );
        let mut correction = prepare_undo(&state, &optimistic).unwrap();
        correction[0].after = state.read(TruthView::Ground, block);
        state
            .commit(
                ActionContext {
                    id: break_action.id,
                    tick: break_action.tick,
                    actor_chunk: break_action.actor_chunk,
                },
                TruthView::Local,
                &correction,
            )
            .unwrap();
        assert_eq!(state.local.get(&block), Some(&wood));
        assert_eq!(state.ground.get(&block), Some(&wood));
    }

    #[test]
    fn missing_chunk_owner_does_not_turn_into_a_terminal_condition_loss() {
        struct MissingChunk;
        impl ActionState for MissingChunk {
            fn read(&self, _: TruthView, _: Target) -> Option<Cell> {
                None
            }

            fn available(&self, _: TruthView, _: Target) -> bool {
                false
            }

            fn commit(
                &mut self,
                _: ActionContext,
                _: TruthView,
                _: &[AppliedWrite],
            ) -> Result<(), ApplyFailure> {
                unreachable!()
            }
        }

        let (_, block) = targets();
        let action = action(
            vec![Condition {
                target: block,
                expected: Expected::Missing,
            }],
            vec![Effect::Put {
                target: block,
                value: Value::Block(1),
            }],
        );
        assert_eq!(
            prepare_action(&MissingChunk, &action, TruthView::Ground),
            Err(ApplyFailure::MissingStakeholder(block.stakeholder()))
        );
    }

    #[test]
    fn accepted_semantic_action_rebases_optimistic_result_to_ground() {
        let (player, _) = targets();
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(
            player,
            Cell {
                revision: 3,
                value: Value::Scalar(5),
            },
        );
        state.ground.insert(
            player,
            Cell {
                revision: 4,
                value: Value::Scalar(8),
            },
        );
        let action = action(
            vec![Condition {
                target: player,
                expected: Expected::ScalarAtLeast(2),
            }],
            vec![Effect::Add {
                target: player,
                amount: -2,
            }],
        );
        let optimistic = apply_action(&mut state, &action, TruthView::Local).unwrap();
        let accepted = prepare_action(&state, &action, TruthView::Ground).unwrap();
        let _ = prepare_undo(&state, &optimistic).unwrap();
        let correction = vec![AppliedWrite {
            target: player,
            before: optimistic.writes[0].after.clone(),
            after: accepted.writes[0].after.clone(),
        }];
        state
            .commit(
                ActionContext {
                    id: action.id,
                    tick: action.tick,
                    actor_chunk: None,
                },
                TruthView::Ground,
                &accepted.writes,
            )
            .unwrap();
        state
            .commit(
                ActionContext {
                    id: action.id,
                    tick: action.tick,
                    actor_chunk: None,
                },
                TruthView::Local,
                &correction,
            )
            .unwrap();
        assert_eq!(state.local.get(&player), state.ground.get(&player));
        assert_eq!(state.ground[&player].value, Value::Scalar(6));
    }

    #[test]
    fn multi_stakeholder_transfer_preserves_items_and_rejects_stale_destination() {
        let (player, _) = targets();
        let Target::Player { key, .. } = player else {
            unreachable!()
        };
        let source = Target::Player {
            key,
            lease: Some(LivePlayerLease {
                key,
                gid: GlobalPlayerId {
                    server: ServerId(1),
                    player: PlayerSlot(3),
                },
                incarnation: 17,
            }),
            inventory: Some(InventoryId::Player { inv: 0, slot: 2 }),
        };
        let destination = Target::Chunk {
            key: ChunkKey::new(WorldId::new(0, 0), 0, 0),
            part: ChunkPart::Block(BlockPos { x: 2, y: 64, z: 1 }),
            inventory: Some(InventoryId::Block { inv: 0, slot: 4 }),
        };
        let source_loc = InvLoc { inv: 0, slot: 2 };
        let destination_loc = InvLoc { inv: 0, slot: 4 };
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(
            source,
            Cell {
                revision: 3,
                value: Value::Inventory(InventoryCell {
                    loc: source_loc,
                    item: 7,
                    count: 9,
                    nbt: vec![1, 2],
                }),
            },
        );
        state.local.insert(
            destination,
            Cell {
                revision: 5,
                value: Value::Inventory(InventoryCell::empty(destination_loc)),
            },
        );
        let before = state.local.clone();
        let transfer = action(
            vec![
                Condition {
                    target: source,
                    expected: Expected::Exact(before[&source].clone()),
                },
                Condition {
                    target: destination,
                    expected: Expected::Exact(before[&destination].clone()),
                },
            ],
            vec![Effect::MoveItems {
                source,
                destination,
                count: 4,
                max_stack: 64,
            }],
        );
        state.local.get_mut(&destination).unwrap().revision = 6;
        let stale = state.local.clone();
        assert_eq!(
            apply_action(&mut state, &transfer, TruthView::Local),
            Err(ApplyFailure::Condition(destination))
        );
        assert_eq!(state.local, stale);
        state.local = before;
        apply_action(&mut state, &transfer, TruthView::Local).unwrap();
        let counts = [source, destination].map(|target| match &state.local[&target].value {
            Value::Inventory(cell) => cell.count,
            _ => unreachable!(),
        });
        assert_eq!(counts, [5, 4]);
    }

    #[test]
    fn half_stack_replay_uses_changed_source_and_next_available_slot() {
        let player = PlayerKey(Uuid::from_u128(33));
        let slot = |slot| Target::Player {
            key: player,
            lease: Some(LivePlayerLease {
                key: player,
                gid: GlobalPlayerId {
                    server: ServerId(1),
                    player: PlayerSlot(3),
                },
                incarnation: 17,
            }),
            inventory: Some(InventoryId::Player { inv: 0, slot }),
        };
        let source = slot(0);
        let full = slot(1);
        let empty = slot(2);
        let item = |slot, count| Cell {
            revision: 7,
            value: Value::Inventory(InventoryCell {
                loc: InvLoc { inv: 0, slot },
                item: 1,
                count,
                nbt: vec![4],
            }),
        };
        let intent = InventoryReplayIntent {
            source,
            item: 1,
            components: vec![4],
            count: InventoryCountRule::Half,
            ordered_destinations: vec![
                InventoryReplayCandidate {
                    target: full,
                    max_stack: 64,
                },
                InventoryReplayCandidate {
                    target: empty,
                    max_stack: 64,
                },
            ],
            selector_revision: 9,
            destination_policy: InventoryDestinationPolicy::Ordered,
            equipment: Vec::new(),
        };
        let mut values = BTreeMap::from([
            (source, item(0, 3)),
            (full, item(1, 64)),
            (
                empty,
                Cell {
                    revision: 7,
                    value: Value::Inventory(InventoryCell::empty(InvLoc { inv: 0, slot: 2 })),
                },
            ),
        ]);
        assert_eq!(
            intent.derive_effects(&values),
            Ok(vec![Effect::MoveItems {
                source,
                destination: empty,
                count: 2,
                max_stack: 64,
            }])
        );
        values.insert(source, item(0, 2));
        assert_eq!(
            intent.derive_effects(&values),
            Ok(vec![Effect::MoveItems {
                source,
                destination: empty,
                count: 1,
                max_stack: 64,
            }])
        );
    }

    #[test]
    fn semantic_inventory_undo_survives_a_later_replayed_half_move() {
        let key = PlayerKey(Uuid::from_u128(72));
        let slot = |slot| Target::Player {
            key,
            lease: Some(LivePlayerLease {
                key,
                gid: GlobalPlayerId {
                    server: ServerId(1),
                    player: PlayerSlot(3),
                },
                incarnation: 17,
            }),
            inventory: Some(InventoryId::Player { inv: 0, slot }),
        };
        let pool = slot(0);
        let player = slot(1);
        let cursor = slot(2);
        let cell = |slot, count, revision| Cell {
            revision,
            value: Value::Inventory(if count == 0 {
                InventoryCell::empty(InvLoc { inv: 0, slot })
            } else {
                InventoryCell {
                    loc: InvLoc { inv: 0, slot },
                    item: 1,
                    count,
                    nbt: vec![9],
                }
            }),
        };
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(pool, cell(0, 2, 1));
        state.local.insert(player, cell(1, 2, 1));
        state.local.insert(cursor, cell(2, 0, 1));
        let mut pickup = action(
            vec![
                Condition {
                    target: pool,
                    expected: Expected::Exact(cell(0, 2, 1)),
                },
                Condition {
                    target: player,
                    expected: Expected::Exact(cell(1, 2, 1)),
                },
            ],
            vec![Effect::MoveItems {
                source: pool,
                destination: player,
                count: 2,
                max_stack: 64,
            }],
        );
        pickup.undo.extend([
            UndoProcedure::AdjustInventoryCount { target: pool },
            UndoProcedure::AdjustInventoryCount { target: player },
        ]);
        let applied = apply_action(&mut state, &pickup, TruthView::Local).unwrap();
        state.local.insert(player, cell(1, 3, 4));
        state.local.insert(cursor, cell(2, 1, 4));
        let inverse = prepare_semantic_undo(&state, &pickup, &applied).unwrap();
        state
            .commit(
                ActionContext {
                    id: pickup.id,
                    tick: pickup.tick,
                    actor_chunk: None,
                },
                TruthView::Local,
                &inverse,
            )
            .unwrap();
        let counts = [pool, player, cursor].map(|target| match &state.local[&target].value {
            Value::Inventory(item) => item.count,
            _ => unreachable!(),
        });
        assert_eq!(counts, [2, 1, 1]);
        assert_eq!(state.local[&player].revision, 5);
    }
}

impl Value {
    pub fn logical_record_count(&self) -> Result<usize, ApplyFailure> { crate::logical_records::count(self) }
}

impl ActionProposal {
    pub fn logical_record_count(&self) -> Result<usize, ApplyFailure> { crate::logical_records::count(self) }
}
