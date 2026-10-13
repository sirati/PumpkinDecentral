use crate::{inventory::InventoryCell, protocol::BlockPos, time::TickStamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectState {
    pub simulation_tick: Option<TickStamp>,
    pub property_versions: std::collections::BTreeMap<ObjectProperty, u64>,
    pub kind: ObjectKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ObjectProperty {
    SimulationTick,
    Cloud,
    FrameItem,
    FramePolicy,
    FrameRotation,
    Attachment,
    Fuse,
    Falling,
    Crystal,
    DisplayTransform,
    DisplayContent,
    DisplayPresentation,
    InteractionBounds,
    InteractionAttack,
    InteractionUse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectKind {
    ItemFrame(FrameState),
    Painting(PaintingState),
    LeashKnot { attachment: BlockPos },
    PrimedTnt(TntState),
    AreaEffectCloud(AreaEffectCloudState),
    FallingBlock(FallingBlockState),
    EndCrystal(CrystalState),
    Display(DisplayState),
    Interaction(InteractionState),
    Marker { data: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameState {
    pub attachment: BlockPos,
    pub facing: u8,
    pub glow: bool,
    pub item: InventoryCell,
    pub rotation: u8,
    pub drop_chance: u32,
    pub invisible: bool,
    pub fixed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaintingState {
    pub attachment: BlockPos,
    pub facing: u8,
    pub variant: u16,
    pub width: u16,
    pub height: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TntState {
    pub fuse: u16,
    pub power: u32,
    pub block_state: u16,
    pub igniter: Option<crate::item_contact::ContactIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallingBlockState {
    pub block_state: u16,
    pub start: BlockPos,
    pub age: u32,
    pub drop_item: bool,
    pub hurt_entities: bool,
    pub fall_distance: u32,
    pub damage_per_block: u32,
    pub max_damage: u32,
    pub tile_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrystalState {
    pub beam: Option<BlockPos>,
    pub show_bottom: bool,
    pub invulnerable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayState {
    pub transform: DisplayTransform,
    pub interpolation_start: i32,
    pub interpolation_duration: u32,
    pub teleport_duration: u8,
    pub billboard: u8,
    pub brightness: i32,
    pub view_range: u32,
    pub shadow_radius: u32,
    pub shadow_strength: u32,
    pub width: u32,
    pub height: u32,
    pub glow_color: i32,
    pub content: DisplayContent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayTransform {
    pub translation: [u32; 3],
    pub scale: [u32; 3],
    pub left_rotation: [u32; 4],
    pub right_rotation: [u32; 4],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DisplayContent {
    Block(u16),
    Item {
        item: InventoryCell,
        context: u8,
    },
    Text {
        component: Vec<u8>,
        line_width: i32,
        background: i32,
        opacity: u8,
        flags: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionState {
    pub width: u32,
    pub height: u32,
    pub response: bool,
    pub attack: Option<InteractionRecord>,
    pub interaction: Option<InteractionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionRecord {
    pub player: uuid::Uuid,
    pub tick: TickStamp,
}

impl ObjectState {
    pub fn valid(&self) -> bool {
        self.property_versions.values().all(|version| *version != 0)
            && self
                .property_versions
                .keys()
                .all(|property| self.supports(*property))
            && match &self.kind {
                ObjectKind::ItemFrame(frame) => {
                    frame.facing < 6
                        && frame.rotation < 8
                        && unit(frame.drop_chance)
                        && item(&frame.item, true, 1)
                }
                ObjectKind::Painting(painting) => {
                    (2..=5).contains(&painting.facing)
                        && (1..=64).contains(&painting.width)
                        && (1..=64).contains(&painting.height)
                }
                ObjectKind::LeashKnot { .. } | ObjectKind::EndCrystal(_) => true,
                ObjectKind::AreaEffectCloud(cloud) => cloud.valid(),
                ObjectKind::PrimedTnt(tnt) => {
                    nonnegative(tnt.power, 128.0) && tnt.igniter.as_ref().is_none_or(identity)
                }
                ObjectKind::FallingBlock(falling) => {
                    falling.block_state != 0
                        && nonnegative(falling.fall_distance, 30_000_000.0)
                        && nonnegative(falling.damage_per_block, 1_000_000.0)
                        && nonnegative(falling.max_damage, 1_000_000.0)
                        && falling.tile_data.len() <= 65_536
                }
                ObjectKind::Display(display) => display.valid(),
                ObjectKind::Interaction(interaction) => {
                    nonnegative(interaction.width, 1024.0)
                        && nonnegative(interaction.height, 1024.0)
                        && interaction
                            .attack
                            .as_ref()
                            .is_none_or(|record| !record.player.is_nil())
                        && interaction
                            .interaction
                            .as_ref()
                            .is_none_or(|record| !record.player.is_nil())
                }
                ObjectKind::Marker { data } => data.len() <= 65_536,
            }
    }

    pub fn supports(&self, property: ObjectProperty) -> bool {
        matches!(property, ObjectProperty::SimulationTick)
            || match (&self.kind, property) {
                (
                    ObjectKind::ItemFrame(_),
                    ObjectProperty::FrameItem
                    | ObjectProperty::FramePolicy
                    | ObjectProperty::FrameRotation
                    | ObjectProperty::Attachment,
                )
                | (
                    ObjectKind::Painting(_) | ObjectKind::LeashKnot { .. },
                    ObjectProperty::Attachment,
                )
                | (ObjectKind::AreaEffectCloud(_), ObjectProperty::Cloud)
                | (ObjectKind::PrimedTnt(_), ObjectProperty::Fuse)
                | (ObjectKind::FallingBlock(_), ObjectProperty::Falling)
                | (ObjectKind::EndCrystal(_), ObjectProperty::Crystal)
                | (
                    ObjectKind::Display(_),
                    ObjectProperty::DisplayTransform
                    | ObjectProperty::DisplayContent
                    | ObjectProperty::DisplayPresentation,
                )
                | (
                    ObjectKind::Interaction(_),
                    ObjectProperty::InteractionBounds
                    | ObjectProperty::InteractionAttack
                    | ObjectProperty::InteractionUse,
                ) => true,
                _ => false,
            }
    }
}

impl DisplayState {
    pub fn valid(&self) -> bool {
        self.teleport_duration <= 59
            && self.interpolation_duration <= i32::MAX as u32
            && self.billboard <= 3
            && (self.brightness == -1
                || (self.brightness >= 0 && self.brightness & !0x00f0_00f0 == 0))
            && nonnegative(self.view_range, 1024.0)
            && nonnegative(self.shadow_radius, 64.0)
            && unit(self.shadow_strength)
            && nonnegative(self.width, 1024.0)
            && nonnegative(self.height, 1024.0)
            && self
                .transform
                .translation
                .iter()
                .all(|bits| f32::from_bits(*bits).is_finite())
            && self
                .transform
                .scale
                .iter()
                .all(|bits| f32::from_bits(*bits).is_finite())
            && quaternion(self.transform.left_rotation)
            && quaternion(self.transform.right_rotation)
            && match &self.content {
                DisplayContent::Block(_) => true,
                DisplayContent::Item {
                    item: stack,
                    context,
                } => *context <= 8 && item(stack, true, 99),
                DisplayContent::Text {
                    component,
                    line_width,
                    flags,
                    ..
                } => {
                    !component.is_empty()
                        && component.len() <= 65_536
                        && *line_width >= 0
                        && *flags & !0x1f == 0
                }
            }
    }
}

fn nonnegative(bits: u32, limit: f32) -> bool {
    let value = f32::from_bits(bits);
    value.is_finite() && value >= 0.0 && value <= limit
}

fn unit(bits: u32) -> bool {
    nonnegative(bits, 1.0)
}

fn quaternion(bits: [u32; 4]) -> bool {
    let values = bits.map(f32::from_bits);
    values.iter().all(|value| value.is_finite())
        && values.iter().map(|value| value * value).sum::<f32>() > 0.0
}

fn item(stack: &InventoryCell, empty: bool, maximum: u8) -> bool {
    if stack.item == 0 || stack.count == 0 {
        empty && stack.item == 0 && stack.count == 0 && stack.nbt.is_empty()
    } else {
        stack.count <= maximum && !stack.nbt.is_empty() && stack.nbt.len() <= 65_536
    }
}

fn identity(identity: &crate::item_contact::ContactIdentity) -> bool {
    match identity {
        crate::item_contact::ContactIdentity::Player(lease) => {
            lease.incarnation != 0 && !lease.key.0.is_nil()
        }
        crate::item_contact::ContactIdentity::Entity { id, birth } => {
            id.origin == birth.source.server
                && id.generation != 0
                && birth.source.generation != 0
                && !birth.uuid.is_nil()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AreaEffectCloudState {
    pub potion: InventoryCell,
    pub shooter: crate::item_contact::ContactIdentity,
    pub initial_radius: u32,
    pub radius_per_tick: u32,
    pub radius_on_use: u32,
    pub duration: u32,
    pub wait: u32,
    pub reapply: u16,
    pub continuation: CloudContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudContinuation {
    pub radius: u32,
    pub age: u32,
    pub cooldowns: std::collections::BTreeMap<crate::actor::ActorId, CloudVictimCooldown>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudVictimCooldown {
    pub identity: crate::item_contact::ContactIdentity,
    pub tick: TickStamp,
}

impl CloudContinuation {
    pub fn valid(&self) -> bool {
        nonnegative(self.radius, 64.0)
            && self.cooldowns.len() <= 4096
            && self.cooldowns.iter().all(|(actor, cooldown)| {
                identity(&cooldown.identity)
                    && *actor
                        == match cooldown.identity {
                            crate::item_contact::ContactIdentity::Player(lease) => {
                                crate::actor::ActorId::Player(lease.gid)
                            }
                            crate::item_contact::ContactIdentity::Entity { id, .. } => {
                                crate::actor::ActorId::Entity(id)
                            }
                        }
            })
    }
}

impl AreaEffectCloudState {
    pub fn valid(&self) -> bool {
        self.potion.item != 0
            && self.potion.count == 1
            && self.potion.nbt.len() <= 65_536
            && identity(&self.shooter)
            && nonnegative(self.initial_radius, 64.0)
            && [self.radius_per_tick, self.radius_on_use]
                .iter()
                .all(|bits| {
                    let value = f32::from_bits(*bits);
                    value.is_finite() && (-64.0..=0.0).contains(&value)
                })
            && self.duration > 0
            && self.duration <= 1_000_000
            && self.wait <= 1200
            && (1..=1200).contains(&self.reapply)
            && self.continuation.valid()
            && f32::from_bits(self.continuation.radius) <= f32::from_bits(self.initial_radius)
    }
}
