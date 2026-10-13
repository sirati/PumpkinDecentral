use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::item_action::ActorLivingState;
use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityBody {
    pub mount: Option<crate::item_contact::ContactIdentity>,
    pub tags: std::collections::BTreeSet<String>,
    pub tags_revision: u64,
    pub relation_versions: std::collections::BTreeMap<crate::actor_relation::ActorRelation, u64>,
    pub passengers: std::collections::BTreeMap<u8, crate::item_contact::ContactIdentity>,
    pub id: GlobalEntityId,
    pub birth: EntityBirth,
    pub owner: crate::realm_completion::PeerGeneration,
    pub kind: u16,
    pub spatial: EntitySpatial,
    pub spatial_epoch: crate::canonical_action::SpatialEpoch,
    pub payload: EntityPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityBirth {
    pub action: crate::canonical_action::ActionRef,
    pub source: crate::realm_completion::PeerGeneration,
    pub uuid: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityGroundEvent {
    pub action: crate::canonical_action::ActionRef,
    pub birth: EntityBirth,
    pub before: Option<(ChunkKey, crate::realm_completion::PeerGeneration)>,
    pub after: Option<(ChunkKey, crate::realm_completion::PeerGeneration)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntitySpatial {
    pub world: WorldId,
    pub position: [u64; 3],
    pub velocity_micro: [i64; 3],
    pub yaw: u32,
    pub pitch: u32,
    pub on_ground: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityPayload {
    Living(ActorLivingState),
    Item(DroppedItem),
    Experience(ExperienceOrbState),
    Projectile(ProjectileBody),
    Vehicle(VehicleState),
    Object(crate::object_state::ObjectState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VehicleKind {
    Boat,
    ChestBoat,
    Minecart,
    ChestMinecart,
    FurnaceMinecart,
    HopperMinecart,
    TntMinecart,
    SpawnerMinecart,
    CommandBlockMinecart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleState {
    pub property_versions:
        std::collections::BTreeMap<crate::vehicle_property::VehicleProperty, u64>,
    pub kind: VehicleKind,
    pub simulation_tick: Option<crate::time::TickStamp>,
    pub lifecycle_tick: Option<crate::time::TickStamp>,
    pub control_sequence: Option<crate::identity::PlayerSeq>,
    pub driver: Option<crate::canonical_action::LivePlayerLease>,
    pub paddle_left: bool,
    pub paddle_right: bool,
    pub underwater_ticks: u16,
    pub wobble_ticks: u16,
    pub wobble_side: i8,
    pub damage_milli: u32,
    pub drop_loot: bool,
    pub hopper_enabled: bool,
    pub fuel_ticks: u32,
    pub furnace_push_micro: [i64; 2],
    pub fuse_ticks: Option<u16>,
}

impl VehicleState {
    pub fn new(kind: VehicleKind) -> Self {
        Self {
            property_versions: Default::default(),
            kind,
            simulation_tick: None,
            lifecycle_tick: None,
            control_sequence: None,
            driver: None,
            paddle_left: false,
            paddle_right: false,
            underwater_ticks: 0,
            wobble_ticks: 0,
            wobble_side: 1,
            damage_milli: 0,
            drop_loot: true,
            hopper_enabled: true,
            fuel_ticks: 0,
            furnace_push_micro: [0; 2],
            fuse_ticks: None,
        }
    }

    pub fn valid(&self) -> bool {
        self.property_versions
            .values()
            .all(|revision| *revision != 0)
            && (self.hopper_enabled || self.kind == VehicleKind::HopperMinecart)
            && self.underwater_ticks <= 60
            && matches!(self.wobble_side, -1 | 1)
            && self
                .driver
                .is_none_or(|lease| lease.incarnation != 0 && !lease.key.0.is_nil())
            && (self.control_sequence.is_none() || self.driver.is_some())
            && (matches!(self.kind, VehicleKind::Boat | VehicleKind::ChestBoat)
                || (!self.paddle_left && !self.paddle_right))
            && (self.fuse_ticks.is_none() || self.kind == VehicleKind::TntMinecart)
            && (self.fuel_ticks == 0 || self.kind == VehicleKind::FurnaceMinecart)
            && self.furnace_push_micro.iter().all(|axis| axis.unsigned_abs() <= 1_000_000)
            && (self.furnace_push_micro == [0;2] || self.kind == VehicleKind::FurnaceMinecart)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceOrbState {
    pub amount: u32,
    pub count: u32,
    pub health_milli: u32,
    pub age: u32,
    pub last_lifetime_tick: Option<crate::time::TickStamp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectileKind {
    Arrow,
    SpectralArrow,
    Trident,
    Snowball,
    Egg,
    EnderPearl,
    ExperienceBottle,
    SplashPotion,
    LingeringPotion,
    Firework,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectileBody {
    pub last_simulation_tick: Option<crate::time::TickStamp>,
    pub embedded_age: u32,
    pub kind: ProjectileKind,
    pub shooter: ActorId,
    pub shooter_birth: Option<EntityBirth>,
    pub shooter_lease: Option<crate::canonical_action::LivePlayerLease>,
    pub shot_tick: crate::time::TickStamp,
    pub carried_item: crate::inventory::InventoryCell,
    pub damage_milli: u32,
    pub fire_ticks: u16,
    pub knockback_level: u16,
    pub critical: bool,
    pub pierce: u8,
    pub age: u32,
    pub embedded: Option<crate::protocol::BlockPos>,
    pub hit_actors: Vec<ActorId>,
    pub pickup_allowed: bool,
    pub pickup_creative_only: bool,
}

impl ProjectileBody {
    pub fn valid(&self) -> bool {
        let provenance = match self.shooter {
            ActorId::Player(gid) => {
                self.shooter_birth.is_none()
                    && self.shooter_lease.is_some_and(|lease| {
                        lease.gid == gid && lease.incarnation != 0 && !lease.key.0.is_nil()
                    })
            }
            ActorId::Entity(id) => {
                self.shooter_lease.is_none()
                    && self.shooter_birth.as_ref().is_some_and(|birth| {
                        birth.source.server == id.origin
                            && birth.source.generation != 0
                            && !birth.uuid.is_nil()
                    })
            }
            ActorId::Peer(_) => false,
        };
        provenance
            && self.carried_item.item != 0
            && self.carried_item.count == 1
            && !self.carried_item.nbt.is_empty()
            && self.carried_item.nbt.len() <= 65_536
            && self.pierce <= 127
            && self.embedded_age <= self.age
            && !(self.pickup_allowed && self.pickup_creative_only)
            && self.hit_actors.len() <= 128
            && self
                .hit_actors
                .iter()
                .enumerate()
                .all(|(index, actor)| !self.hit_actors[..index].contains(actor))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DroppedItem {
    pub item: u16,
    pub count: u8,
    pub components: Vec<u8>,
    pub age: u32,
    pub last_lifetime_tick: Option<crate::time::TickStamp>,
    pub pickup_delay: u16,
    pub health_milli: u32,
    pub never_despawn: bool,
    pub never_pickup: bool,
    pub fire_immune: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DroppedItemChange {
    AdvanceLifetime,
    Damage { amount_milli: u32, fire: bool },
    Expire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DroppedItemCondition {
    Alive,
    PickupReady,
    HopperReady,
    Expired,
}

impl DroppedItem {
    pub fn matches(&self, condition: DroppedItemCondition) -> bool {
        self.count > 0
            && self.health_milli > 0
            && match condition {
                DroppedItemCondition::Alive => true,
                DroppedItemCondition::PickupReady => !self.never_pickup && self.pickup_delay == 0,
                DroppedItemCondition::HopperReady => true,
                DroppedItemCondition::Expired => !self.never_despawn && self.age >= 6000,
            }
    }

    pub fn merge_from(&mut self, source: &Self, max_stack: u8) -> bool {
        let Some(count) = self.count.checked_add(source.count) else {
            return false;
        };
        if self.item != source.item
            || self.components != source.components
            || self.count == 0
            || source.count == 0
            || self.health_milli == 0
            || source.health_milli == 0
            || self.never_despawn
            || source.never_despawn
            || max_stack == 0
            || max_stack > 99
            || count > max_stack
        {
            return false;
        }
        self.count = count;
        self.never_despawn = source.never_despawn;
        if !source.never_despawn {
            self.age = self.age.min(source.age);
        }
        self.never_pickup = source.never_pickup;
        if !source.never_pickup {
            self.pickup_delay = self.pickup_delay.max(source.pickup_delay);
        }
        true
    }

    pub fn apply_change(&mut self, change: DroppedItemChange) -> bool {
        match change {
            DroppedItemChange::AdvanceLifetime => {
                if !self.never_pickup {
                    self.pickup_delay = self.pickup_delay.saturating_sub(1);
                }
                if !self.never_despawn {
                    self.age = self.age.saturating_add(1);
                }
            }
            DroppedItemChange::Damage { amount_milli, fire } => {
                if !(fire && self.fire_immune) {
                    self.health_milli = self.health_milli.saturating_sub(amount_milli);
                }
            }
            DroppedItemChange::Expire => {
                return self.never_despawn || self.age < 6000;
            }
        }
        self.health_milli > 0
    }
}

impl EntityBody {
    pub fn valid_at(&self, tick: crate::time::TickStamp) -> bool {
        if !crate::actor_tags::valid(&self.tags) { return false; }
        if !matches!(
            self.spatial_epoch,
            crate::canonical_action::SpatialEpoch::Action(_)
        ) {
            return false;
        }
        match &self.payload {
            EntityPayload::Item(item) => item
                .last_lifetime_tick
                .is_none_or(|previous| !previous.is_after(tick)),
            EntityPayload::Experience(orb) => orb
                .last_lifetime_tick
                .is_none_or(|previous| !previous.is_after(tick)),
            EntityPayload::Vehicle(vehicle) => vehicle
                .simulation_tick
                .is_none_or(|previous| !previous.is_after(tick)),
            EntityPayload::Projectile(projectile) => projectile.last_simulation_tick
                .is_none_or(|previous| !previous.is_after(tick)),
            _ => true,
        }
    }

    #[must_use]
    pub fn belongs_to(&self, id: GlobalEntityId, chunk: ChunkKey) -> bool {
        if self.relation_versions.iter().any(|(relation, revision)| {
            *revision == 0
                || !matches!(
                    relation,
                    crate::actor_relation::ActorRelation::Mount | crate::actor_relation::ActorRelation::Passenger(0..=31)
                )
        }) {
            return false;
        }
        if self.mount.as_ref().is_some_and(|mount| !crate::actor_relation::identity_valid(mount)
            || matches!(mount, crate::item_contact::ContactIdentity::Player(_))
            || matches!(mount, crate::item_contact::ContactIdentity::Entity { id: mount_id, .. } if *mount_id == id)) {
            return false;
        }
        if self.passengers.len() > 32
            || self.passengers.iter().any(|(seat, identity)| {
                *seat >= 32
                    || match identity {
                        crate::item_contact::ContactIdentity::Player(lease) => {
                            lease.incarnation == 0 || lease.key.0.is_nil()
                        }
                        crate::item_contact::ContactIdentity::Entity {
                            id: passenger,
                            birth,
                        } => {
                            *passenger == id
                                || birth.uuid.is_nil()
                                || birth.source.server != passenger.origin
                                || birth.source.generation == 0
                        }
                    }
            })
            || self
                .passengers
                .values()
                .enumerate()
                .any(|(index, identity)| {
                    self.passengers
                        .values()
                        .skip(index + 1)
                        .any(|other| other == identity)
                })
        {
            return false;
        }
        if self.id != id
            || self.spatial.world != chunk.world
            || self.birth.source.server != id.origin
            || self.birth.uuid.is_nil()
        {
            return false;
        }
        if !self.spatial.belongs_to(chunk) {
            return false;
        }
        match &self.payload {
            EntityPayload::Living(state) => state.belongs_to(ActorId::Entity(id)),
            EntityPayload::Item(item) => {
                item.item != 0
                    && (1..=99).contains(&item.count)
                    && !item.components.is_empty()
                    && item.components.len() <= 65_536
                    && item.health_milli > 0
            }
            EntityPayload::Experience(orb) => {
                orb.amount > 0
                    && orb.amount <= i32::MAX as u32
                    && orb.count > 0
                    && orb.count <= i32::MAX as u32
                    && orb.health_milli > 0
                    && orb.health_milli <= 5_000
            }
            EntityPayload::Projectile(projectile) => projectile.valid(),
            EntityPayload::Vehicle(vehicle) => vehicle.valid(),
            EntityPayload::Object(object) => object.valid(),
        }
    }
}

impl EntitySpatial {
    pub fn belongs_to(&self, chunk: ChunkKey) -> bool {
        let position = self.position.map(f64::from_bits);
        self.world == chunk.world
            && position
                .iter()
                .all(|value| value.is_finite() && value.abs() <= 30_000_000.0)
            && f32::from_bits(self.yaw).is_finite()
            && f32::from_bits(self.pitch).is_finite()
            && (position[0].floor() as i32).div_euclid(16) == chunk.x()
            && (position[2].floor() as i32).div_euclid(16) == chunk.z()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item() -> DroppedItem {
        DroppedItem {
            item: 1,
            count: 3,
            components: vec![0],
            age: 5999,
            last_lifetime_tick: None,
            pickup_delay: 2,
            health_milli: 5000,
            never_despawn: false,
            never_pickup: false,
            fire_immune: false,
        }
    }

    #[test]
    fn lifetime_expiry_and_damage_preserve_stack_identity() {
        let mut value = item();
        assert!(value.apply_change(DroppedItemChange::Expire));
        assert!(value.apply_change(DroppedItemChange::AdvanceLifetime));
        assert_eq!((value.age, value.pickup_delay), (6000, 1));
        assert!(!value.apply_change(DroppedItemChange::Expire));
        value.fire_immune = true;
        assert!(value.apply_change(DroppedItemChange::Damage {
            amount_milli: 6000,
            fire: true
        }));
        assert_eq!(value.health_milli, 5000);
        assert!(!value.apply_change(DroppedItemChange::Damage {
            amount_milli: 6000,
            fire: false
        }));
        assert_eq!(value.health_milli, 0);
        assert_eq!((value.item, value.count, value.components), (1, 3, vec![0]));
    }

    #[test]
    fn protected_lifetime_and_delay_do_not_advance() {
        let mut value = item();
        value.never_pickup = true;
        value.never_despawn = true;
        assert!(value.apply_change(DroppedItemChange::AdvanceLifetime));
        assert_eq!((value.age, value.pickup_delay), (5999, 2));
        assert!(value.apply_change(DroppedItemChange::Expire));
        value.never_pickup = false;
        value.pickup_delay = 0;
        value.never_despawn = false;
        value.age = u32::MAX;
        assert!(value.apply_change(DroppedItemChange::AdvanceLifetime));
        assert_eq!((value.age, value.pickup_delay), (u32::MAX, 0));
    }

    #[test]
    fn merge_is_atomic_and_preserves_destination_health() {
        let mut destination = item();
        let mut source = item();
        source.count = 4;
        source.age = 20;
        source.pickup_delay = 40;
        source.health_milli = 1000;
        let before = destination.clone();
        assert!(!destination.merge_from(&source, 6));
        assert_eq!(destination, before);
        source.components = vec![1];
        assert!(!destination.merge_from(&source, 64));
        assert_eq!(destination, before);
        source.components = vec![0];
        assert!(destination.merge_from(&source, 64));
        assert_eq!(
            (
                destination.count,
                destination.age,
                destination.pickup_delay,
                destination.health_milli
            ),
            (7, 20, 40, 5000)
        );
        assert_eq!(source.count, 4);
    }
}
