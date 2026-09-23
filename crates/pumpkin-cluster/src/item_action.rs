use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::completion::ActionKey;
use crate::identity::PlayerSeq;
use crate::inventory::{
    InventoryCell, InventoryMutation, InventoryOp, InventorySlotRef, InventoryStateSnapshot,
    InventoryTarget, SemanticInventoryOp,
};
use crate::protocol::{BlockPos, ChunkKey, WorldId};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ClaimedHit {
    pub position: [f32; 3],
    pub face: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorItemInteraction {
    Interact,
    Attack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ItemActionKind {
    Use {
        claimed_yaw: f32,
        claimed_pitch: f32,
    },
    UseOnBlock {
        world: WorldId,
        block: BlockPos,
        hit: ClaimedHit,
        expected_block_state: u16,
    },
    UseOnActor {
        target: ActorId,
        target_chunk: ChunkKey,
        hit: Option<[f32; 3]>,
        interaction: ActorItemInteraction,
    },
    Release {
        claimed_head_position: [f32; 3],
    },
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimedInventoryTarget {
    Container(InventorySlotRef),
    World,
    Creative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedInventoryChange {
    pub target: ClaimedInventoryTarget,
    pub before: InventoryCell,
    pub after: InventoryCell,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemActionClientClaim {
    pub changes: Vec<ClaimedInventoryChange>,
    pub item_in_hand: Option<InventoryCell>,
    pub hotbar_slot: Option<u8>,
    pub clicked_block_state: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemActionIntent {
    pub actor: ActorId,
    pub tick: TickStamp,
    pub source: InventorySlotRef,
    pub expected_inventory_revision: u64,
    pub expected_item: InventoryCell,
    pub actor_chunk: ChunkKey,
    pub affected: Vec<ChunkKey>,
    pub kind: ItemActionKind,
    pub client_claim: ItemActionClientClaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EffectiveItemPose {
    pub actor: ActorId,
    pub tick: TickStamp,
    pub world: WorldId,
    pub position: [f64; 3],
    pub eye_position: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
    pub reach: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ActorItemSnapshot {
    pub actor: ActorId,
    pub chunk: ChunkKey,
    pub position: [f64; 3],
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemBlockMutation {
    pub world: WorldId,
    pub block: BlockPos,
    pub before: u16,
    pub after: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemActorMutation {
    pub actor: ActorId,
    pub before: ActorItemSnapshot,
    pub after: ActorItemSnapshot,
    pub before_state: Vec<u8>,
    pub after_state: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemUseMode {
    Consume,
    Block,
    Bow,
    Crossbow,
    Trident,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemUseState {
    pub actor: ActorId,
    pub source: InventorySlotRef,
    pub item: InventoryCell,
    pub started: TickStamp,
    pub mode: ItemUseMode,
    pub duration_ticks: u32,
    pub cooldown_ticks: u32,
    pub has_remainder: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemUseTransition {
    pub before: Option<ItemUseState>,
    pub after: Option<ItemUseState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemUseVisualUpdate {
    pub actor: ActorId,
    pub chunk: ChunkKey,
    pub sequence: PlayerSeq,
    pub tick: TickStamp,
    pub state: Option<ItemUseState>,
    pub correction: bool,
}

impl ItemUseVisualUpdate {
    #[must_use]
    pub fn from_action(action: &ItemAction, accepted: bool) -> Option<Self> {
        let transition = action.use_transition.as_ref()?;
        Some(Self {
            actor: action.actor,
            chunk: action.actor_chunk,
            sequence: action.sequence,
            tick: action.tick,
            state: if accepted {
                transition.after.clone()
            } else {
                transition.before.clone()
            },
            correction: !accepted,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemCooldownState {
    pub actor: ActorId,
    pub group: String,
    pub until: TickStamp,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemCooldownTransition {
    pub before: Option<ItemCooldownState>,
    pub after: ItemCooldownState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorCombatState {
    pub actor: ActorId,
    pub revision: u64,
    pub health_milli: u32,
    pub absorption_milli: u32,
    pub velocity_micro: [i64; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemCombatEffect {
    pub attacker: ActorId,
    pub before: ActorCombatState,
    pub after: ActorCombatState,
    pub damage_milli: u32,
    pub knockback_micro: [i64; 3],
}

impl ItemCombatEffect {
    #[must_use]
    pub fn new(
        attacker: ActorId,
        before: ActorCombatState,
        damage_milli: u32,
        knockback_micro: [i64; 3],
    ) -> Option<Self> {
        if damage_milli == 0 || before.health_milli == 0 {
            return None;
        }
        let absorbed = damage_milli.min(before.absorption_milli);
        let health_damage = damage_milli - absorbed;
        let mut velocity_micro = before.velocity_micro;
        for (current, change) in velocity_micro.iter_mut().zip(knockback_micro) {
            *current = current.checked_add(change)?;
        }
        Some(Self {
            attacker,
            before,
            after: ActorCombatState {
                revision: before.revision.wrapping_add(1),
                health_milli: before.health_milli.saturating_sub(health_damage),
                absorption_milli: before.absorption_milli - absorbed,
                velocity_micro,
                ..before
            },
            damage_milli,
            knockback_micro,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorFoodState {
    pub actor: ActorId,
    pub food: u8,
    pub saturation_milli: u32,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemFoodEffect {
    pub before: ActorFoodState,
    pub after: ActorFoodState,
    pub nutrition: u8,
    pub saturation_gain_milli: u32,
}

impl ItemFoodEffect {
    #[must_use]
    pub fn new(before: ActorFoodState, nutrition: u8, saturation_gain_milli: u32) -> Self {
        let food = before.food.saturating_add(nutrition).min(20);
        let saturation_milli = before
            .saturation_milli
            .saturating_add(saturation_gain_milli)
            .min(u32::from(food) * 1_000);
        Self {
            before,
            after: ActorFoodState {
                food,
                saturation_milli,
                revision: before.revision.wrapping_add(1),
                ..before
            },
            nutrition,
            saturation_gain_milli,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemActorEffect {
    Food(ItemFoodEffect),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStatisticMutation {
    pub actor: crate::identity::GlobalPlayerId,
    pub category: i32,
    pub statistic: i32,
    pub amount: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemProjectileKind {
    Bow,
    Crossbow,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemProjectileEffect {
    pub kind: ItemProjectileKind,
    pub spawn: ItemProjectileSpawn,
    pub weapon_after: InventoryCell,
    pub ammo: InventorySlotRef,
    pub ammo_before: InventoryCell,
    pub ammo_after: InventoryCell,
    pub ammo_revision: u64,
    pub direction: [f32; 3],
    pub power_milli: u16,
    pub durability_cost: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ItemProjectileSpawn {
    pub id: crate::protocol::GlobalEntityId,
    pub world: WorldId,
    pub position: [f64; 3],
    pub velocity: [f64; 3],
}

impl ItemProjectileSpawn {
    #[must_use]
    pub fn for_bow(
        id: crate::protocol::GlobalEntityId,
        pose: EffectiveItemPose,
        direction: [f32; 3],
        power_milli: u16,
    ) -> Option<Self> {
        if power_milli == 0
            || power_milli > 1_000
            || direction.iter().any(|value| !value.is_finite())
            || direction.iter().all(|value| *value == 0.0)
            || !(0.99..=1.01).contains(&direction.iter().map(|value| value * value).sum::<f32>())
        {
            return None;
        }
        let speed = 3.0 * f64::from(power_milli) / 1_000.0;
        Some(Self {
            id,
            world: pose.world,
            position: pose.eye_position,
            velocity: direction.map(|component| f64::from(component) * speed),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRemainderEffect {
    pub source_before: InventoryCell,
    pub source_after: InventoryCell,
    pub remainder_item: InventoryCell,
    pub destinations: Vec<SemanticInventoryOp>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ItemActionPlan {
    pub source_after: Option<InventoryCell>,
    pub blocks: Vec<ItemBlockMutation>,
    pub actors: Vec<ItemActorMutation>,
    pub use_transition: Option<ItemUseTransition>,
    pub cooldown_transition: Option<ItemCooldownTransition>,
    pub combat: Option<ItemCombatEffect>,
    pub projectile: Option<ItemProjectileEffect>,
    pub remainder: Option<ItemRemainderEffect>,
    pub actor_effects: Vec<ItemActorEffect>,
    pub statistics: Vec<ItemStatisticMutation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemAction {
    pub actor: ActorId,
    pub sequence: PlayerSeq,
    pub tick: TickStamp,
    pub input_tick: TickStamp,
    pub source: InventorySlotRef,
    pub expected_inventory_revision: u64,
    pub expected_item: InventoryCell,
    pub source_after: Option<InventoryCell>,
    pub actor_chunk: ChunkKey,
    pub affected: Vec<ChunkKey>,
    pub kind: ItemActionKind,
    pub client_claim: ItemActionClientClaim,
    pub effective: EffectiveItemPose,
    pub target: Option<ActorItemSnapshot>,
    pub blocks: Vec<ItemBlockMutation>,
    pub actors: Vec<ItemActorMutation>,
    pub use_transition: Option<ItemUseTransition>,
    pub cooldown_transition: Option<ItemCooldownTransition>,
    pub combat: Option<ItemCombatEffect>,
    pub projectile: Option<ItemProjectileEffect>,
    pub remainder: Option<ItemRemainderEffect>,
    pub actor_effects: Vec<ItemActorEffect>,
    pub statistics: Vec<ItemStatisticMutation>,
    pub dependencies: Vec<ActionKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemActionRejection {
    ActorMismatch,
    TickMismatch,
    InvalidPose,
    InvalidReach,
    MissingInventory,
    StaleInventory,
    StaleItem,
    MissingBlock,
    StaleBlock,
    MissingActor,
    StaleActor,
    UnsupportedEffect,
    LostConflict,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemActionResolution {
    pub accepted: Vec<ItemAction>,
    pub rejected: Vec<(ItemAction, ItemActionRejection)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InventoryItemAction {
    Inventory(InventoryOp),
    Item(ItemAction),
}

impl InventoryItemAction {
    #[must_use]
    pub fn key(&self) -> ActionKey {
        match self {
            Self::Inventory(action) => ActionKey::inventory(action),
            Self::Item(action) => ActionKey::item_action(action),
        }
    }
}

pub fn sort_inventory_item_actions(
    cluster_seed: u64,
    tick: TickStamp,
    actions: &mut [InventoryItemAction],
) {
    actions.sort_by(|left, right| {
        let left_key = left.key();
        let right_key = right.key();
        crate::order::order_actors(cluster_seed, tick, left_key.actor, right_key.actor)
            .then_with(|| left_key.sequence.cmp(&right_key.sequence))
            .then_with(|| left_key.family.cmp(&right_key.family))
    });
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ItemActionResource {
    Inventory(InventoryTarget, u8, u16),
    Block(WorldId, BlockPos),
    Actor(ActorId),
}

impl ItemActionIntent {
    #[must_use]
    pub fn normalized(mut self) -> Self {
        self.affected.push(self.actor_chunk);
        if let ItemActionKind::UseOnBlock { world, block, .. } = &self.kind {
            self.affected.push(ChunkKey::new(
                *world,
                block.x.div_euclid(16),
                block.z.div_euclid(16),
            ));
        }
        if let ItemActionKind::UseOnActor { target_chunk, .. } = &self.kind {
            self.affected.push(*target_chunk);
        }
        self.affected.extend(self.source.target.intrinsic_chunk());
        self.affected.sort_unstable();
        self.affected.dedup();
        self
    }

    #[must_use]
    pub fn capture(
        self,
        sequence: PlayerSeq,
        effective: EffectiveItemPose,
        target: Option<ActorItemSnapshot>,
        plan: ItemActionPlan,
    ) -> ItemAction {
        let mut normalized = self.normalized();
        normalized
            .affected
            .extend(plan.blocks.iter().map(|mutation| {
                ChunkKey::new(
                    mutation.world,
                    mutation.block.x.div_euclid(16),
                    mutation.block.z.div_euclid(16),
                )
            }));
        normalized
            .affected
            .extend(plan.actors.iter().map(|mutation| mutation.before.chunk));
        normalized
            .affected
            .extend(plan.actors.iter().map(|mutation| mutation.after.chunk));
        if let Some(projectile) = &plan.projectile {
            normalized
                .affected
                .extend(projectile.ammo.target.intrinsic_chunk());
        }
        if let Some(remainder) = &plan.remainder {
            normalized.affected.extend(
                remainder
                    .destinations
                    .iter()
                    .filter_map(|operation| operation.target.intrinsic_chunk()),
            );
        }
        normalized.affected.sort_unstable();
        normalized.affected.dedup();
        ItemAction {
            actor: normalized.actor,
            sequence,
            tick: normalized.tick,
            input_tick: normalized.tick,
            source: normalized.source,
            expected_inventory_revision: normalized.expected_inventory_revision,
            expected_item: normalized.expected_item,
            source_after: plan.source_after,
            actor_chunk: normalized.actor_chunk,
            affected: normalized.affected,
            kind: normalized.kind,
            client_claim: normalized.client_claim,
            effective,
            target,
            blocks: plan.blocks,
            actors: plan.actors,
            use_transition: plan.use_transition,
            cooldown_transition: plan.cooldown_transition,
            combat: plan.combat,
            projectile: plan.projectile,
            remainder: plan.remainder,
            actor_effects: plan.actor_effects,
            statistics: plan.statistics,
            dependencies: Vec::new(),
        }
    }
}

impl ItemAction {
    pub fn inventory_mutation(&self) -> Result<InventoryMutation, ItemActionRejection> {
        let mut operations = Vec::new();
        if let Some(after) = &self.source_after
            && *after != self.expected_item
        {
            operations.push(SemanticInventoryOp {
                target: self.source.target,
                before: self.expected_item.clone(),
                after: after.clone(),
            });
        }
        if let Some(projectile) = &self.projectile {
            operations.push(SemanticInventoryOp {
                target: projectile.ammo.target,
                before: projectile.ammo_before.clone(),
                after: projectile.ammo_after.clone(),
            });
        }
        if let Some(remainder) = &self.remainder {
            operations.extend(remainder.destinations.iter().cloned());
        }
        let mut combined: Vec<SemanticInventoryOp> = Vec::with_capacity(operations.len());
        for operation in operations {
            if operation.before.loc != operation.after.loc {
                return Err(ItemActionRejection::StaleInventory);
            }
            if let Some(previous) = combined.iter_mut().find(|previous| {
                previous.target == operation.target && previous.before.loc == operation.before.loc
            }) {
                if previous.after != operation.before {
                    return Err(ItemActionRejection::StaleInventory);
                }
                previous.after = operation.after;
            } else {
                combined.push(operation);
            }
        }
        Ok(InventoryMutation {
            expected_revisions: vec![(self.source.target, self.expected_inventory_revision)],
            operations: combined,
        })
    }

    pub fn validate_food_state(
        &self,
        current: Option<ActorFoodState>,
    ) -> Result<(), ItemActionRejection> {
        for effect in &self.actor_effects {
            match effect {
                ItemActorEffect::Food(food) if current != Some(food.before) => {
                    return Err(ItemActionRejection::StaleActor);
                }
                ItemActorEffect::Food(_) => {}
            }
        }
        Ok(())
    }

    pub fn validate_combat_state(
        &self,
        current: Option<ActorCombatState>,
    ) -> Result<(), ItemActionRejection> {
        if self
            .combat
            .is_some_and(|combat| current != Some(combat.before))
        {
            return Err(ItemActionRejection::StaleActor);
        }
        Ok(())
    }

    #[must_use]
    pub fn validate(
        &self,
        inventory: Option<&InventoryStateSnapshot>,
        mut block_state: impl FnMut(WorldId, BlockPos) -> Option<u16>,
        mut actor_state: impl FnMut(ActorId) -> Option<ActorItemSnapshot>,
    ) -> Result<(), ItemActionRejection> {
        if self.effective.actor != self.actor || self.effective.world != self.actor_chunk.world {
            return Err(ItemActionRejection::ActorMismatch);
        }
        if self.statistics.len() > 32
            || self.statistics.iter().any(|mutation| {
                !matches!(self.actor, ActorId::Player(actor) if actor == mutation.actor)
                    || mutation.category < 0
                    || mutation.statistic < 0
                    || mutation.amount <= 0
            })
        {
            return Err(ItemActionRejection::StaleActor);
        }
        match (self.actor, self.source.target) {
            (ActorId::Player(actor), InventoryTarget::Player(owner)) if actor == owner => {}
            (ActorId::Entity(actor), InventoryTarget::Entity(owner)) if owner == actor.into() => {}
            _ => return Err(ItemActionRejection::ActorMismatch),
        }
        if self.effective.tick != self.input_tick {
            return Err(ItemActionRejection::TickMismatch);
        }
        if !finite_pose(self.effective) || self.effective.reach < 0.0 {
            return Err(ItemActionRejection::InvalidPose);
        }
        let inventory = inventory.ok_or(ItemActionRejection::MissingInventory)?;
        if inventory.target != self.source.target {
            return Err(ItemActionRejection::MissingInventory);
        }
        if inventory.revision != self.expected_inventory_revision {
            return Err(ItemActionRejection::StaleInventory);
        }
        let actual = inventory
            .cells
            .iter()
            .find(|cell| cell.loc == self.source.loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(self.source.loc));
        if actual != self.expected_item {
            return Err(ItemActionRejection::StaleItem);
        }
        if self
            .client_claim
            .item_in_hand
            .as_ref()
            .is_some_and(|item| item != &self.expected_item)
            || self.client_claim.hotbar_slot.is_some_and(|slot| {
                self.source.loc.inv == crate::inventory::INV_MAIN
                    && u16::from(slot) != self.source.loc.slot
            })
        {
            return Err(ItemActionRejection::StaleItem);
        }
        if self.client_claim.clicked_block_state.is_some()
            && !matches!(&self.kind, ItemActionKind::UseOnBlock { .. })
        {
            return Err(ItemActionRejection::StaleBlock);
        }
        let expected_after = self.source_after.as_ref().unwrap_or(&self.expected_item);
        let mut container_changes = 0;
        for change in &self.client_claim.changes {
            match change.target {
                ClaimedInventoryTarget::Container(slot) => {
                    container_changes += 1;
                    if slot != self.source
                        || change.before != self.expected_item
                        || change.after != *expected_after
                    {
                        return Err(ItemActionRejection::StaleInventory);
                    }
                }
                ClaimedInventoryTarget::World | ClaimedInventoryTarget::Creative => {
                    return Err(ItemActionRejection::UnsupportedEffect);
                }
            }
        }
        if container_changes > 1 {
            return Err(ItemActionRejection::StaleInventory);
        }
        if self.source_after.as_ref().is_some_and(|after| {
            after.loc != self.source.loc || after.count > crate::inventory::INV_MAX_STACK
        }) {
            return Err(ItemActionRejection::StaleItem);
        }
        if let Some(transition) = &self.use_transition {
            if transition
                .before
                .iter()
                .chain(transition.after.iter())
                .any(|state| {
                    state.actor != self.actor
                        || state.source != self.source
                        || state.item != self.expected_item
                        || state.duration_ticks == 0
                })
            {
                return Err(ItemActionRejection::StaleItem);
            }
            if !matches!(
                (&self.kind, &transition.after),
                (ItemActionKind::Use { .. }, Some(_))
                    | (
                        ItemActionKind::Release { .. } | ItemActionKind::Complete,
                        None
                    )
            ) {
                return Err(ItemActionRejection::UnsupportedEffect);
            }
        }
        if let Some(transition) = &self.cooldown_transition {
            if transition.after.actor != self.actor
                || transition.after.group.is_empty()
                || transition.after.group.len() > 256
                || !transition.after.until.is_after(self.tick)
                || transition.before.as_ref().is_some_and(|before| {
                    before.actor != self.actor
                        || before.group != transition.after.group
                        || before.revision.wrapping_add(1) != transition.after.revision
                })
                || (transition.before.is_none() && transition.after.revision != 0)
            {
                return Err(ItemActionRejection::StaleActor);
            }
        }
        if let Some(combat) = self.combat {
            if !matches!(
                &self.kind,
                ItemActionKind::UseOnActor {
                    interaction: ActorItemInteraction::Attack,
                    ..
                }
            ) || combat.attacker != self.actor
                || self.target.is_none_or(|target| {
                    target.actor != combat.before.actor || target.revision != combat.before.revision
                })
                || ItemCombatEffect::new(
                    combat.attacker,
                    combat.before,
                    combat.damage_milli,
                    combat.knockback_micro,
                ) != Some(combat)
            {
                return Err(ItemActionRejection::StaleActor);
            }
        }
        let mut food_seen = false;
        for effect in &self.actor_effects {
            match effect {
                ItemActorEffect::Food(food) => {
                    if food_seen {
                        return Err(ItemActionRejection::StaleActor);
                    }
                    food_seen = true;
                    if !matches!(self.kind, ItemActionKind::Complete)
                        || food.before.actor != self.actor
                        || food.after
                            != ItemFoodEffect::new(
                                food.before,
                                food.nutrition,
                                food.saturation_gain_milli,
                            )
                            .after
                        || food.before.food > 20
                        || food.before.saturation_milli > u32::from(food.before.food) * 1_000
                        || food.nutrition == 0
                        || food.saturation_gain_milli > 20_000
                    {
                        return Err(ItemActionRejection::StaleActor);
                    }
                }
            }
        }
        if let Some(projectile) = &self.projectile {
            let valid_release = matches!(
                (&self.kind, projectile.kind, &self.use_transition),
                (
                    ItemActionKind::Release { .. },
                    ItemProjectileKind::Bow,
                    Some(ItemUseTransition {
                        before: Some(ItemUseState {
                            mode: ItemUseMode::Bow,
                            ..
                        }),
                        after: None,
                    })
                )
            );
            let current_ammo = inventory
                .cells
                .iter()
                .find(|cell| cell.loc == projectile.ammo.loc);
            if !valid_release
                || projectile.ammo.target != self.source.target
                || projectile.ammo.loc == self.source.loc
                || projectile.weapon_after.loc != self.source.loc
                || self.source_after.as_ref() != Some(&projectile.weapon_after)
                || projectile.ammo_revision != inventory.revision
                || current_ammo != Some(&projectile.ammo_before)
                || projectile.ammo_after.loc != projectile.ammo_before.loc
                || projectile.ammo_before.count == 0
                || projectile.ammo_after.count.checked_add(1) != Some(projectile.ammo_before.count)
                || (projectile.ammo_after.count > 0
                    && projectile.ammo_after.item != projectile.ammo_before.item)
                || (projectile.ammo_after.count == 0
                    && (projectile.ammo_after.item != 0 || !projectile.ammo_after.nbt.is_empty()))
                || projectile.direction.iter().any(|value| !value.is_finite())
                || projectile.direction.iter().all(|value| *value == 0.0)
                || projectile.power_milli == 0
                || projectile.durability_cost == 0
                || matches!(self.actor, ActorId::Player(player) if projectile.spawn.id.origin != player.server)
                || {
                    let yaw = self.effective.yaw.to_radians();
                    let pitch = self.effective.pitch.to_radians();
                    let expected = [
                        -yaw.sin() * pitch.cos(),
                        -pitch.sin(),
                        yaw.cos() * pitch.cos(),
                    ];
                    projectile
                        .direction
                        .iter()
                        .zip(expected)
                        .any(|(actual, expected)| (actual - expected).abs() > 1.0e-5)
                }
                || ItemProjectileSpawn::for_bow(
                    projectile.spawn.id,
                    self.effective,
                    projectile.direction,
                    projectile.power_milli,
                ) != Some(projectile.spawn)
            {
                return Err(ItemActionRejection::StaleInventory);
            }
        }
        if let Some(remainder) = &self.remainder {
            let complete = matches!(
                (&self.kind, &self.use_transition),
                (
                    ItemActionKind::Complete,
                    Some(ItemUseTransition {
                        before: Some(ItemUseState {
                            mode: ItemUseMode::Consume,
                            ..
                        }),
                        after: None,
                    })
                )
            );
            if !complete
                || remainder.source_before != self.expected_item
                || self.source_after.as_ref() != Some(&remainder.source_after)
                || remainder.source_after.loc != self.source.loc
                || remainder.source_before.count == 0
            {
                return Err(ItemActionRejection::StaleInventory);
            }
            if remainder.remainder_item.is_empty() {
                return Err(ItemActionRejection::StaleInventory);
            }
            if remainder.source_before.count == 1 {
                if remainder.source_after != remainder.remainder_item
                    || !remainder.destinations.is_empty()
                {
                    return Err(ItemActionRejection::StaleInventory);
                }
            } else {
                if remainder.source_after.item != remainder.source_before.item
                    || remainder.source_after.count.checked_add(1)
                        != Some(remainder.source_before.count)
                    || remainder.destinations.len() > crate::inventory::INVENTORY_SERVICE_MAX_SLOTS
                {
                    return Err(ItemActionRejection::StaleInventory);
                }
                let mut inserted = 0u16;
                let mut seen = std::collections::HashSet::new();
                for operation in &remainder.destinations {
                    let current = inventory
                        .cells
                        .iter()
                        .find(|cell| cell.loc == operation.before.loc)
                        .cloned()
                        .unwrap_or_else(|| InventoryCell::empty(operation.before.loc));
                    if operation.target != self.source.target
                        || operation.before.loc == self.source.loc
                        || operation.after.loc != operation.before.loc
                        || operation.before != current
                        || operation.after.item != remainder.remainder_item.item
                        || operation.after.count <= operation.before.count
                        || operation.after.count > crate::inventory::INV_MAX_STACK
                        || (!operation.before.is_empty()
                            && operation.before.item != operation.after.item)
                        || !seen.insert(operation.before.loc)
                    {
                        return Err(ItemActionRejection::StaleInventory);
                    }
                    inserted = inserted
                        .checked_add(u16::from(operation.after.count - operation.before.count))
                        .ok_or(ItemActionRejection::StaleInventory)?;
                }
                if inserted != u16::from(remainder.remainder_item.count) {
                    return Err(ItemActionRejection::StaleInventory);
                }
            }
        }
        if matches!(self.kind, ItemActionKind::Complete) {
            let Some(transition) = &self.use_transition else {
                return Err(ItemActionRejection::UnsupportedEffect);
            };
            let Some(before) = &transition.before else {
                return Err(ItemActionRejection::UnsupportedEffect);
            };
            let Some(after) = &self.source_after else {
                return Err(ItemActionRejection::StaleInventory);
            };
            if before.mode != ItemUseMode::Consume
                || transition.after.is_some()
                || before.has_remainder != self.remainder.is_some()
                || u32::from(self.input_tick.distance_since(before.started)) < before.duration_ticks
                || self.expected_item.count == 0
            {
                return Err(ItemActionRejection::UnsupportedEffect);
            }
            if self.remainder.is_none()
                && (after.loc != self.source.loc
                    || after.count.checked_add(1) != Some(self.expected_item.count)
                    || (after.count > 0 && after.item != self.expected_item.item)
                    || (after.count == 0 && (after.item != 0 || !after.nbt.is_empty())))
            {
                return Err(ItemActionRejection::StaleInventory);
            }
        }
        match &self.kind {
            ItemActionKind::Use {
                claimed_yaw,
                claimed_pitch,
            } => {
                if !claimed_yaw.is_finite() || !claimed_pitch.is_finite() {
                    return Err(ItemActionRejection::InvalidPose);
                }
            }
            ItemActionKind::UseOnBlock {
                world,
                block,
                hit,
                expected_block_state,
            } => {
                if *world != self.effective.world || !finite_hit(*hit) {
                    return Err(ItemActionRejection::InvalidPose);
                }
                if distance_to_block(self.effective.eye_position, *block) > self.effective.reach {
                    return Err(ItemActionRejection::InvalidReach);
                }
                let current =
                    block_state(*world, *block).ok_or(ItemActionRejection::MissingBlock)?;
                if current != *expected_block_state {
                    return Err(ItemActionRejection::StaleBlock);
                }
            }
            ItemActionKind::UseOnActor {
                target,
                target_chunk,
                hit,
                ..
            } => {
                if hit.is_some_and(|point| point.iter().any(|coordinate| !coordinate.is_finite())) {
                    return Err(ItemActionRejection::InvalidPose);
                }
                let expected = self.target.ok_or(ItemActionRejection::MissingActor)?;
                if expected.actor != *target || expected.chunk != *target_chunk {
                    return Err(ItemActionRejection::StaleActor);
                }
                let current = actor_state(*target).ok_or(ItemActionRejection::MissingActor)?;
                if current != expected {
                    return Err(ItemActionRejection::StaleActor);
                }
                if distance(self.effective.eye_position, current.position) > self.effective.reach {
                    return Err(ItemActionRejection::InvalidReach);
                }
            }
            ItemActionKind::Release {
                claimed_head_position,
            } => {
                if claimed_head_position
                    .iter()
                    .any(|coordinate| !coordinate.is_finite())
                {
                    return Err(ItemActionRejection::InvalidPose);
                }
            }
            ItemActionKind::Complete => {}
        }
        for mutation in &self.blocks {
            let chunk = ChunkKey::new(
                mutation.world,
                mutation.block.x.div_euclid(16),
                mutation.block.z.div_euclid(16),
            );
            if !self.affected.contains(&chunk)
                || mutation.world != self.effective.world
                || distance_to_block(self.effective.eye_position, mutation.block)
                    > self.effective.reach + 1.0
            {
                return Err(ItemActionRejection::InvalidReach);
            }
            let current = block_state(mutation.world, mutation.block)
                .ok_or(ItemActionRejection::MissingBlock)?;
            if current != mutation.before {
                return Err(ItemActionRejection::StaleBlock);
            }
        }
        for mutation in &self.actors {
            if !self.affected.contains(&mutation.before.chunk)
                || !self.affected.contains(&mutation.after.chunk)
            {
                return Err(ItemActionRejection::StaleActor);
            }
            let current = actor_state(mutation.actor).ok_or(ItemActionRejection::MissingActor)?;
            if current != mutation.before {
                return Err(ItemActionRejection::StaleActor);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn validate_cooldown(
        &self,
        current: Option<&ItemCooldownState>,
    ) -> Result<(), ItemActionRejection> {
        if let Some(transition) = &self.cooldown_transition {
            if transition.before.as_ref() != current {
                return Err(ItemActionRejection::StaleActor);
            }
        }
        Ok(())
    }
}

#[must_use]
pub fn resolve_item_actions(
    cluster_seed: u64,
    tick: TickStamp,
    actions: impl IntoIterator<Item = ItemAction>,
    mut validate: impl FnMut(&ItemAction) -> Result<(), ItemActionRejection>,
) -> ItemActionResolution {
    let mut candidates = Vec::new();
    let mut rejected = Vec::new();
    for action in actions {
        if action.tick != tick {
            rejected.push((action, ItemActionRejection::TickMismatch));
        } else if let Err(reason) = validate(&action) {
            rejected.push((action, reason));
        } else {
            candidates.push(action);
        }
    }
    candidates.sort_by(|left, right| {
        crate::order::order_actors(cluster_seed, tick, left.actor, right.actor)
            .then_with(|| left.sequence.0.cmp(&right.sequence.0))
            .then_with(|| action_discriminator(left).cmp(&action_discriminator(right)))
    });
    let mut occupied = std::collections::BTreeSet::new();
    let mut accepted = Vec::new();
    for action in candidates {
        let resources = action_resources(&action);
        if resources.iter().any(|resource| occupied.contains(resource)) {
            rejected.push((action, ItemActionRejection::LostConflict));
        } else {
            occupied.extend(resources);
            accepted.push(action);
        }
    }
    ItemActionResolution { accepted, rejected }
}

fn action_resources(action: &ItemAction) -> Vec<ItemActionResource> {
    let mut resources = vec![ItemActionResource::Inventory(
        action.source.target,
        action.source.loc.inv,
        action.source.loc.slot,
    )];
    match action.kind {
        ItemActionKind::UseOnBlock { world, block, .. } => {
            resources.push(ItemActionResource::Block(world, block));
        }
        ItemActionKind::UseOnActor { target, .. } => {
            resources.push(ItemActionResource::Actor(target));
        }
        ItemActionKind::Use { .. } | ItemActionKind::Release { .. } | ItemActionKind::Complete => {}
    }
    resources.extend(
        action
            .blocks
            .iter()
            .map(|mutation| ItemActionResource::Block(mutation.world, mutation.block)),
    );
    resources.extend(
        action
            .actors
            .iter()
            .map(|mutation| ItemActionResource::Actor(mutation.actor)),
    );
    if let Some(combat) = action.combat {
        resources.push(ItemActionResource::Actor(combat.before.actor));
    }
    resources.extend(action.actor_effects.iter().map(|effect| match effect {
        ItemActorEffect::Food(food) => ItemActionResource::Actor(food.before.actor),
    }));
    if let Some(projectile) = &action.projectile {
        resources.push(ItemActionResource::Inventory(
            projectile.ammo.target,
            projectile.ammo.loc.inv,
            projectile.ammo.loc.slot,
        ));
    }
    if let Some(remainder) = &action.remainder {
        for operation in &remainder.destinations {
            resources.push(ItemActionResource::Inventory(
                operation.target,
                operation.before.loc.inv,
                operation.before.loc.slot,
            ));
        }
    }
    resources.sort_unstable();
    resources.dedup();
    resources
}

fn action_discriminator(action: &ItemAction) -> (u8, InventoryTarget, u8, u16) {
    let kind = match action.kind {
        ItemActionKind::Use { .. } => 0,
        ItemActionKind::UseOnBlock { .. } => 1,
        ItemActionKind::UseOnActor { .. } => 2,
        ItemActionKind::Release { .. } => 3,
        ItemActionKind::Complete => 4,
    };
    (
        kind,
        action.source.target,
        action.source.loc.inv,
        action.source.loc.slot,
    )
}

fn finite_pose(pose: EffectiveItemPose) -> bool {
    pose.position
        .iter()
        .all(|coordinate| coordinate.is_finite())
        && pose
            .eye_position
            .iter()
            .all(|coordinate| coordinate.is_finite())
        && pose.yaw.is_finite()
        && pose.pitch.is_finite()
        && pose.reach.is_finite()
}

fn finite_hit(hit: ClaimedHit) -> bool {
    hit.face < 6
        && hit.position.iter().all(|coordinate| coordinate.is_finite())
        && hit
            .position
            .iter()
            .all(|coordinate| (0.0..=1.0).contains(coordinate))
}

fn distance(left: [f64; 3], right: [f64; 3]) -> f64 {
    left.into_iter()
        .zip(right)
        .map(|(left, right)| (left - right) * (left - right))
        .sum::<f64>()
        .sqrt()
}

fn distance_to_block(point: [f64; 3], block: BlockPos) -> f64 {
    let minimum = [f64::from(block.x), f64::from(block.y), f64::from(block.z)];
    let closest = [
        point[0].clamp(minimum[0], minimum[0] + 1.0),
        point[1].clamp(minimum[1], minimum[1] + 1.0),
        point[2].clamp(minimum[2], minimum[2] + 1.0),
    ];
    distance(point, closest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::inventory::{INV_MAIN, InvLoc, InventoryTarget};

    fn action(actor: GlobalPlayerId, sequence: u16, block_x: i32) -> ItemAction {
        let world = WorldId::new(0, 1);
        let loc = InvLoc::new(INV_MAIN, 0);
        ItemActionIntent {
            actor: actor.into(),
            tick: TickStamp(8),
            source: InventorySlotRef::new(InventoryTarget::Player(actor), loc),
            expected_inventory_revision: 3,
            expected_item: InventoryCell {
                loc,
                item: 4,
                count: 2,
                nbt: Vec::new(),
            },
            actor_chunk: ChunkKey::new(world, 0, 0),
            affected: Vec::new(),
            kind: ItemActionKind::UseOnBlock {
                world,
                block: BlockPos {
                    x: block_x,
                    y: 64,
                    z: 0,
                },
                hit: ClaimedHit {
                    position: [0.5, 0.5, 0.5],
                    face: 1,
                },
                expected_block_state: 7,
            },
            client_claim: ItemActionClientClaim::default(),
        }
        .capture(
            PlayerSeq(sequence),
            EffectiveItemPose {
                actor: actor.into(),
                tick: TickStamp(8),
                world,
                position: [0.5, 64.0, 0.5],
                eye_position: [0.5, 65.62, 0.5],
                yaw: 0.0,
                pitch: 0.0,
                reach: 5.0,
            },
            None,
            ItemActionPlan::default(),
        )
    }

    #[test]
    fn block_use_routes_to_actor_item_and_target_chunks() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let actor_chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let item_chunk = ChunkKey::new(WorldId::new(0, 1), 4, 5);
        let block = BlockPos {
            x: -17,
            y: 4,
            z: 33,
        };
        let intent = ItemActionIntent {
            actor: ActorId::Player(actor),
            tick: TickStamp(8),
            source: InventorySlotRef::new(
                InventoryTarget::Block {
                    chunk: item_chunk,
                    position: BlockPos { x: 64, y: 1, z: 80 },
                },
                InvLoc::new(INV_MAIN, 0),
            ),
            expected_inventory_revision: 3,
            expected_item: InventoryCell::empty(InvLoc::new(INV_MAIN, 0)),
            actor_chunk,
            affected: Vec::new(),
            kind: ItemActionKind::UseOnBlock {
                world: WorldId::new(0, 1),
                block,
                hit: ClaimedHit {
                    position: [0.5, 0.5, 0.5],
                    face: 1,
                },
                expected_block_state: 7,
            },
            client_claim: ItemActionClientClaim::default(),
        }
        .normalized();

        assert_eq!(
            intent.affected,
            vec![
                ChunkKey::new(WorldId::new(0, 1), -2, 2),
                actor_chunk,
                item_chunk,
            ]
        );
    }

    #[test]
    fn stale_inventory_and_effective_reach_reject_before_resolution() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let captured = action(actor, 0, 2);
        let stale = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: 2,
            cells: vec![captured.expected_item.clone()],
        };
        assert_eq!(
            captured.validate(Some(&stale), |_, _| Some(7), |_| None),
            Err(ItemActionRejection::StaleInventory)
        );
        let exact = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: 3,
            cells: vec![captured.expected_item.clone()],
        };
        let far = action(actor, 1, 20);
        assert_eq!(
            far.validate(Some(&exact), |_, _| Some(7), |_| None),
            Err(ItemActionRejection::InvalidReach)
        );
    }

    #[test]
    fn forged_item_use_cannot_spend_another_players_stack_or_hide_a_world_effect() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let other = GlobalPlayerId::new(ServerId(2), PlayerSlot(3));
        let mut forged = action(actor, 0, 2);
        forged.source.target = InventoryTarget::Player(other);
        let other_inventory = InventoryStateSnapshot {
            target: InventoryTarget::Player(other),
            revision: forged.expected_inventory_revision,
            cells: vec![forged.expected_item.clone()],
        };
        assert_eq!(
            forged.validate(Some(&other_inventory), |_, _| Some(7), |_| None),
            Err(ItemActionRejection::ActorMismatch)
        );

        let mut hidden = action(actor, 1, 15);
        hidden.effective.position[0] = 15.5;
        hidden.effective.eye_position[0] = 15.5;
        hidden.blocks.push(ItemBlockMutation {
            world: hidden.actor_chunk.world,
            block: BlockPos { x: 16, y: 64, z: 0 },
            before: 0,
            after: 1,
        });
        let inventory = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: hidden.expected_inventory_revision,
            cells: vec![hidden.expected_item.clone()],
        };
        assert_eq!(
            hidden.validate(
                Some(&inventory),
                |_, pos| Some((pos.x != 16) as u16 * 7),
                |_| None
            ),
            Err(ItemActionRejection::InvalidReach)
        );
    }

    #[test]
    fn delayed_settlement_keeps_the_original_effective_controller_frame() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut delayed = action(actor, 0, 2);
        delayed.tick = TickStamp(12);
        let inventory = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: delayed.expected_inventory_revision,
            cells: vec![delayed.expected_item.clone()],
        };
        assert_eq!(delayed.input_tick, TickStamp(8));
        assert!(
            delayed
                .validate(Some(&inventory), |_, _| Some(7), |_| None)
                .is_ok()
        );
        delayed.effective.tick = TickStamp(12);
        assert_eq!(
            delayed.validate(Some(&inventory), |_, _| Some(7), |_| None),
            Err(ItemActionRejection::TickMismatch)
        );
    }

    #[test]
    fn client_predicted_stack_change_cannot_override_server_placement_cost() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut placement = action(actor, 0, 2);
        let mut server_after = placement.expected_item.clone();
        server_after.count = 1;
        placement.source_after = Some(server_after.clone());
        placement.client_claim.changes.push(ClaimedInventoryChange {
            target: ClaimedInventoryTarget::Container(placement.source),
            before: placement.expected_item.clone(),
            after: InventoryCell::empty(placement.source.loc),
        });
        let inventory = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: placement.expected_inventory_revision,
            cells: vec![placement.expected_item.clone()],
        };
        assert_eq!(
            placement.validate(Some(&inventory), |_, _| Some(7), |_| None),
            Err(ItemActionRejection::StaleInventory)
        );
        placement.client_claim.changes[0].after = server_after;
        assert!(
            placement
                .validate(Some(&inventory), |_, _| Some(7), |_| None)
                .is_ok()
        );
    }

    #[test]
    fn bow_fire_rejects_ammo_changed_by_an_earlier_inventory_action() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut fire = action(actor, 0, 2);
        fire.kind = ItemActionKind::Release {
            claimed_head_position: [0.5, 65.62, 0.5],
        };
        let state = ItemUseState {
            actor: actor.into(),
            source: fire.source,
            item: fire.expected_item.clone(),
            started: TickStamp(4),
            mode: ItemUseMode::Bow,
            duration_ticks: 72_000,
            cooldown_ticks: 0,
            has_remainder: false,
        };
        fire.use_transition = Some(ItemUseTransition {
            before: Some(state),
            after: None,
        });
        let ammo_loc = InvLoc::new(INV_MAIN, 1);
        let ammo_before = InventoryCell {
            loc: ammo_loc,
            item: 7,
            count: 2,
            nbt: Vec::new(),
        };
        let ammo_after = InventoryCell {
            count: 1,
            ..ammo_before.clone()
        };
        fire.projectile = Some(ItemProjectileEffect {
            kind: ItemProjectileKind::Bow,
            spawn: ItemProjectileSpawn::for_bow(
                crate::protocol::GlobalEntityId {
                    origin: ServerId(1),
                    local_id: 9,
                    generation: 0,
                },
                fire.effective,
                [0.0, 0.0, 1.0],
                1_000,
            )
            .unwrap(),
            weapon_after: fire.expected_item.clone(),
            ammo: InventorySlotRef::new(InventoryTarget::Player(actor), ammo_loc),
            ammo_before: ammo_before.clone(),
            ammo_after,
            ammo_revision: 3,
            direction: [0.0, 0.0, 1.0],
            power_milli: 1000,
            durability_cost: 1,
        });
        fire.source_after = Some(fire.expected_item.clone());
        let current = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: 3,
            cells: vec![fire.expected_item.clone(), ammo_before],
        };
        assert!(
            fire.validate(Some(&current), |_, _| Some(7), |_| None)
                .is_ok()
        );
        let mut service = crate::inventory::InventoryService::new();
        service
            .install(current.target, current.revision, current.cells.clone())
            .unwrap();
        service.mutate(fire.inventory_mutation().unwrap()).unwrap();
        let applied = service.snapshot(current.target).unwrap();
        assert_eq!(
            applied
                .cells
                .iter()
                .find(|cell| cell.loc == ammo_loc)
                .unwrap()
                .count,
            1
        );
        let after_move = InventoryStateSnapshot {
            cells: vec![current.cells[0].clone(), InventoryCell::empty(ammo_loc)],
            ..current
        };
        assert_eq!(
            fire.validate(Some(&after_move), |_, _| Some(7), |_| None),
            Err(ItemActionRejection::StaleInventory)
        );

        let mut second_weapon = fire.clone();
        second_weapon.sequence = PlayerSeq(1);
        second_weapon.source.loc = InvLoc::new(INV_MAIN, 2);
        second_weapon.expected_item.loc = second_weapon.source.loc;
        second_weapon
            .use_transition
            .as_mut()
            .unwrap()
            .before
            .as_mut()
            .unwrap()
            .source = second_weapon.source;
        second_weapon
            .use_transition
            .as_mut()
            .unwrap()
            .before
            .as_mut()
            .unwrap()
            .item = second_weapon.expected_item.clone();
        let result = resolve_item_actions(99, fire.tick, [second_weapon, fire], |_| Ok(()));
        assert_eq!(result.accepted.len(), 1);
        assert_eq!(result.rejected[0].1, ItemActionRejection::LostConflict);
    }

    #[test]
    fn cooldown_started_by_an_earlier_action_invalidates_stale_use() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut use_action = action(actor, 0, 2);
        use_action.kind = ItemActionKind::Use {
            claimed_yaw: 0.0,
            claimed_pitch: 0.0,
        };
        use_action.cooldown_transition = Some(ItemCooldownTransition {
            before: None,
            after: ItemCooldownState {
                actor: actor.into(),
                group: "minecraft:shared_throw".into(),
                until: TickStamp(28),
                revision: 0,
            },
        });
        let inventory = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: 3,
            cells: vec![use_action.expected_item.clone()],
        };
        assert!(
            use_action
                .validate(Some(&inventory), |_, _| None, |_| None)
                .is_ok()
        );
        assert!(use_action.validate_cooldown(None).is_ok());
        let current = ItemCooldownState {
            actor: actor.into(),
            group: "minecraft:shared_throw".into(),
            until: TickStamp(18),
            revision: 1,
        };
        assert_eq!(
            use_action.validate_cooldown(Some(&current)),
            Err(ItemActionRejection::StaleActor)
        );
    }

    #[test]
    fn timed_consumption_cannot_complete_early_or_after_source_moves() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut consume = action(actor, 0, 2);
        consume.kind = ItemActionKind::Complete;
        consume.use_transition = Some(ItemUseTransition {
            before: Some(ItemUseState {
                actor: actor.into(),
                source: consume.source,
                item: consume.expected_item.clone(),
                started: TickStamp(0),
                mode: ItemUseMode::Consume,
                duration_ticks: 9,
                cooldown_ticks: 0,
                has_remainder: false,
            }),
            after: None,
        });
        let mut after = consume.expected_item.clone();
        after.count = 1;
        consume.source_after = Some(after);
        let inventory = InventoryStateSnapshot {
            target: InventoryTarget::Player(actor),
            revision: 3,
            cells: vec![consume.expected_item.clone()],
        };
        assert_eq!(
            consume.validate(Some(&inventory), |_, _| None, |_| None),
            Err(ItemActionRejection::UnsupportedEffect)
        );
        consume
            .use_transition
            .as_mut()
            .unwrap()
            .before
            .as_mut()
            .unwrap()
            .duration_ticks = 8;
        assert!(
            consume
                .validate(Some(&inventory), |_, _| None, |_| None)
                .is_ok()
        );
        let moved = InventoryStateSnapshot {
            cells: vec![InventoryCell::empty(consume.source.loc)],
            ..inventory
        };
        assert_eq!(
            consume.validate(Some(&moved), |_, _| None, |_| None),
            Err(ItemActionRejection::StaleItem)
        );
    }

    #[test]
    fn split_remainder_rejects_a_stale_destination_without_losing_the_other_part() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut consume = action(actor, 0, 2);
        consume.kind = ItemActionKind::Complete;
        consume.use_transition = Some(ItemUseTransition {
            before: Some(ItemUseState {
                actor: actor.into(),
                source: consume.source,
                item: consume.expected_item.clone(),
                started: TickStamp(0),
                mode: ItemUseMode::Consume,
                duration_ticks: 8,
                cooldown_ticks: 0,
                has_remainder: true,
            }),
            after: None,
        });
        let mut source_after = consume.expected_item.clone();
        source_after.count = 1;
        consume.source_after = Some(source_after.clone());
        let destinations: Vec<_> = [1, 2]
            .into_iter()
            .map(|slot| {
                let loc = InvLoc::new(INV_MAIN, slot);
                SemanticInventoryOp {
                    target: consume.source.target,
                    before: InventoryCell::empty(loc),
                    after: InventoryCell {
                        loc,
                        item: 9,
                        count: 1,
                        nbt: Vec::new(),
                    },
                }
            })
            .collect();
        consume.remainder = Some(ItemRemainderEffect {
            source_before: consume.expected_item.clone(),
            source_after,
            remainder_item: InventoryCell {
                loc: consume.source.loc,
                item: 9,
                count: 2,
                nbt: Vec::new(),
            },
            destinations,
        });
        let exact = InventoryStateSnapshot {
            target: consume.source.target,
            revision: 3,
            cells: vec![consume.expected_item.clone()],
        };
        assert!(
            consume
                .validate(Some(&exact), |_, _| None, |_| None)
                .is_ok()
        );
        let mut service = crate::inventory::InventoryService::new();
        service
            .install(exact.target, exact.revision, exact.cells.clone())
            .unwrap();
        service
            .mutate(consume.inventory_mutation().unwrap())
            .unwrap();
        let applied = service.snapshot(exact.target).unwrap();
        assert_eq!(applied.cells.len(), 3);
        assert_eq!(
            applied
                .cells
                .iter()
                .map(|cell| (cell.loc.slot, cell.count))
                .collect::<Vec<_>>(),
            vec![(0, 1), (1, 1), (2, 1)]
        );
        let stale = InventoryStateSnapshot {
            cells: vec![
                consume.expected_item.clone(),
                InventoryCell {
                    loc: InvLoc::new(INV_MAIN, 2),
                    item: 10,
                    count: 1,
                    nbt: Vec::new(),
                },
            ],
            ..exact
        };
        assert_eq!(
            consume.validate(Some(&stale), |_, _| None, |_| None),
            Err(ItemActionRejection::StaleInventory)
        );
    }

    #[test]
    fn linked_source_remainder_coalesces_only_when_intermediate_state_matches() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let mut action = action(actor, 0, 2);
        let empty = InventoryCell::empty(action.source.loc);
        let replacement = InventoryCell {
            loc: action.source.loc,
            item: 9,
            count: 1,
            nbt: Vec::new(),
        };
        action.source_after = Some(empty.clone());
        action.remainder = Some(ItemRemainderEffect {
            source_before: action.expected_item.clone(),
            source_after: empty.clone(),
            remainder_item: replacement.clone(),
            destinations: vec![SemanticInventoryOp {
                target: action.source.target,
                before: empty.clone(),
                after: replacement.clone(),
            }],
        });
        let mutation = action.inventory_mutation().unwrap();
        assert_eq!(mutation.operations.len(), 1);
        assert_eq!(mutation.operations[0].before, action.expected_item);
        assert_eq!(mutation.operations[0].after, replacement);

        action.remainder.as_mut().unwrap().destinations[0]
            .before
            .count = 1;
        assert_eq!(
            action.inventory_mutation(),
            Err(ItemActionRejection::StaleInventory)
        );
    }

    #[test]
    fn competing_food_completions_share_one_actor_revision() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let before = ActorFoodState {
            actor: actor.into(),
            food: 18,
            saturation_milli: 17_000,
            revision: 7,
        };
        let effect = ItemFoodEffect::new(before, 4, 4_800);
        assert_eq!(effect.after.food, 20);
        assert_eq!(effect.after.saturation_milli, 20_000);
        assert_eq!(effect.after.revision, 8);
        let mut first = action(actor, 0, 2);
        first.kind = ItemActionKind::Complete;
        first.actor_effects.push(ItemActorEffect::Food(effect));
        let mut second = first.clone();
        second.sequence = PlayerSeq(1);
        let result = resolve_item_actions(99, first.tick, [second, first], |action| {
            action.validate_food_state(Some(before))
        });
        assert_eq!(result.accepted.len(), 1);
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].1, ItemActionRejection::LostConflict);
        assert_eq!(
            result.accepted[0].validate_food_state(Some(effect.after)),
            Err(ItemActionRejection::StaleActor)
        );
    }

    #[test]
    fn attack_consumes_absorption_before_health_and_preserves_exact_inverse() {
        let attacker = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let target = GlobalPlayerId::new(ServerId(2), PlayerSlot(3));
        let before = ActorCombatState {
            actor: target.into(),
            revision: 7,
            health_milli: 5_000,
            absorption_milli: 1_500,
            velocity_micro: [250_000, 0, -125_000],
        };
        let effect =
            ItemCombatEffect::new(attacker.into(), before, 2_000, [500_000, 0, 250_000]).unwrap();
        assert_eq!(effect.after.absorption_milli, 0);
        assert_eq!(effect.after.health_milli, 4_500);
        assert_eq!(effect.after.velocity_micro, [750_000, 0, 125_000]);
        assert_eq!(effect.after.revision, 8);
        let mut action = action(attacker, 0, 2);
        action.kind = ItemActionKind::UseOnActor {
            target: target.into(),
            target_chunk: ChunkKey::new(WorldId::new(0, 1), 0, 0),
            hit: Some([0.0, 0.0, 0.0]),
            interaction: ActorItemInteraction::Attack,
        };
        action.target = Some(ActorItemSnapshot {
            actor: target.into(),
            chunk: ChunkKey::new(WorldId::new(0, 1), 0, 0),
            position: [1.0, 64.0, 0.0],
            revision: 7,
        });
        action.combat = Some(effect);
        assert_eq!(action.validate_combat_state(Some(before)), Ok(()));
        assert_eq!(
            action.validate_combat_state(Some(effect.after)),
            Err(ItemActionRejection::StaleActor)
        );
        let mut forged = action.clone();
        forged.combat.as_mut().unwrap().after.health_milli += 1;
        let inventory = InventoryStateSnapshot {
            target: action.source.target,
            revision: action.expected_inventory_revision,
            cells: vec![action.expected_item.clone()],
        };
        assert_eq!(
            forged.validate(Some(&inventory), |_, _| None, |_| action.target),
            Err(ItemActionRejection::StaleActor)
        );
    }

    #[test]
    fn three_attackers_converge_on_one_target_revision_after_reordered_delivery() {
        let target = GlobalPlayerId::new(ServerId(4), PlayerSlot(1));
        let before = ActorCombatState {
            actor: target.into(),
            revision: 11,
            health_milli: 10_000,
            absorption_milli: 0,
            velocity_micro: [0; 3],
        };
        let attacks: Vec<_> = [1, 2, 3]
            .into_iter()
            .map(|server| {
                let attacker = GlobalPlayerId::new(ServerId(server), PlayerSlot(1));
                let mut action = action(attacker, 0, i32::from(server));
                action.kind = ItemActionKind::UseOnActor {
                    target: target.into(),
                    target_chunk: ChunkKey::new(WorldId::new(0, 1), 0, 0),
                    hit: None,
                    interaction: ActorItemInteraction::Attack,
                };
                action.target = Some(ActorItemSnapshot {
                    actor: target.into(),
                    chunk: ChunkKey::new(WorldId::new(0, 1), 0, 0),
                    position: [1.0, 64.0, 0.0],
                    revision: before.revision,
                });
                action.combat = ItemCombatEffect::new(
                    attacker.into(),
                    before,
                    1_000 * u32::from(server),
                    [i64::from(server) * 100_000, 0, 0],
                );
                action
            })
            .collect();
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let outcomes: Vec<_> = permutations
            .map(|order| {
                let result = resolve_item_actions(
                    99,
                    attacks[0].tick,
                    order.map(|index| attacks[index].clone()),
                    |action| action.validate_combat_state(Some(before)),
                );
                assert_eq!(result.accepted.len(), 1);
                assert_eq!(result.rejected.len(), 2);
                assert!(
                    result
                        .rejected
                        .iter()
                        .all(|(_, reason)| *reason == ItemActionRejection::LostConflict)
                );
                let winner = &result.accepted[0];
                (winner.actor, winner.combat.unwrap().after)
            })
            .into_iter()
            .collect();
        assert!(outcomes.iter().all(|outcome| *outcome == outcomes[0]));
        assert_eq!(outcomes[0].1.revision, before.revision + 1);
    }

    #[test]
    fn conflicting_resolution_is_identical_for_every_arrival_order() {
        let actors = [
            GlobalPlayerId::new(ServerId(1), PlayerSlot(1)),
            GlobalPlayerId::new(ServerId(2), PlayerSlot(1)),
            GlobalPlayerId::new(ServerId(3), PlayerSlot(1)),
        ];
        let actions: Vec<_> = actors
            .into_iter()
            .enumerate()
            .map(|(index, actor)| action(actor, index as u16, 2))
            .collect();
        let permutations = [
            vec![actions[0].clone(), actions[1].clone(), actions[2].clone()],
            vec![actions[2].clone(), actions[0].clone(), actions[1].clone()],
            vec![actions[1].clone(), actions[2].clone(), actions[0].clone()],
        ];
        let winners: Vec<_> = permutations
            .into_iter()
            .map(|arrival| {
                resolve_item_actions(99, TickStamp(8), arrival, |_| Ok(()))
                    .accepted
                    .into_iter()
                    .map(|action| action.actor)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(winners.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(winners[0].len(), 1);
    }

    #[test]
    fn inventory_move_and_block_use_converge_under_opposite_arrival_orders() {
        let owner = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let editor = GlobalPlayerId::new(ServerId(2), PlayerSlot(3));
        let mut item = action(owner, 0, 2);
        let mut after_use = item.expected_item.clone();
        after_use.count = 1;
        item.source_after = Some(after_use.clone());
        let empty = InventoryCell::empty(item.source.loc);
        let move_action = crate::inventory::capture_inventory_mutation(
            ActorId::Player(editor),
            PlayerSeq(0),
            item.tick,
            vec![(item.source.target, item.expected_inventory_revision)],
            vec![crate::inventory::SemanticInventoryOp {
                target: item.source.target,
                before: item.expected_item.clone(),
                after: empty,
            }],
            item.actor_chunk,
            vec![item.actor_chunk],
        );
        let arrivals = [
            vec![
                InventoryItemAction::Inventory(move_action.clone()),
                InventoryItemAction::Item(item.clone()),
            ],
            vec![
                InventoryItemAction::Item(item.clone()),
                InventoryItemAction::Inventory(move_action),
            ],
        ];
        let outcomes: Vec<_> = arrivals
            .into_iter()
            .map(|mut actions| {
                sort_inventory_item_actions(99, item.tick, &mut actions);
                let mut ledger = crate::inventory::InvLedger::new();
                ledger
                    .install_owned_snapshot(
                        InventoryStateSnapshot {
                            target: item.source.target,
                            revision: item.expected_inventory_revision,
                            cells: vec![item.expected_item.clone()],
                        },
                        vec![item.actor_chunk],
                    )
                    .unwrap();
                let mut block_placed = false;
                let mut wins = 0;
                for action in actions {
                    match action {
                        InventoryItemAction::Inventory(operation) => {
                            wins += usize::from(ledger.apply_op_result(&operation).is_ok());
                        }
                        InventoryItemAction::Item(action) => {
                            let snapshot = ledger.snapshot(action.source.target);
                            if action
                                .validate(snapshot.as_ref(), |_, _| Some(7), |_| None)
                                .is_ok()
                            {
                                let operation = crate::inventory::capture_inventory_mutation(
                                    action.actor,
                                    action.sequence,
                                    action.tick,
                                    vec![(
                                        action.source.target,
                                        action.expected_inventory_revision,
                                    )],
                                    vec![crate::inventory::SemanticInventoryOp {
                                        target: action.source.target,
                                        before: action.expected_item.clone(),
                                        after: action.source_after.unwrap(),
                                    }],
                                    action.actor_chunk,
                                    action.affected,
                                );
                                if ledger.apply_op_result(&operation).is_ok() {
                                    block_placed = true;
                                    wins += 1;
                                }
                            }
                        }
                    }
                }
                (
                    ledger.snapshot(item.source.target).unwrap().cells,
                    block_placed,
                    wins,
                )
            })
            .collect();
        assert_eq!(outcomes[0], outcomes[1]);
        assert_eq!(outcomes[0].2, 1);
    }
}
