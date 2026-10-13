use serde::{Deserialize, Serialize};

use crate::mob_continuation::MobSpecies;
use crate::protocol::BlockPos;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MobSpeciesPropertyValue {
    Byte(u8),
    Flag(bool),
    Unsigned(u32),
    Float(u32),
    OptionalUnsigned(Option<u32>),
    OptionalByte(Option<u8>),
    OptionalUuid(Option<uuid::Uuid>),
    OptionalBlockState(Option<u16>),
    Position(BlockPos),
    OptionalPosition(Option<BlockPos>),
    Vector([u64; 3]),
}

impl MobSpeciesPropertyValue {
    pub fn valid(&self) -> bool {
        match self {
            Self::Float(value) => f32::from_bits(*value).is_finite(),
            Self::Vector(value) => value.iter().all(|value| f64::from_bits(*value).is_finite()),
            Self::OptionalUuid(value) => value.is_none_or(|value| !value.is_nil()),
            _ => true,
        }
    }
}

macro_rules! species_properties {
    ($($species:ident { $($field:ident => $property:ident : $value:ident),* $(,)? }),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub enum MobSpeciesProperty { $($($property),*),* }

        impl MobSpeciesProperty {
            pub const ALL: &'static [Self] = &[$($(Self::$property),*),*];

            pub fn read(self, species: &MobSpecies) -> Option<MobSpeciesPropertyValue> {
                match (self, species) {
                    $($( (Self::$property, MobSpecies::$species { $field, .. }) => Some(MobSpeciesPropertyValue::$value($field.clone())), )*)*
                    _ => None,
                }
            }

            pub fn accepts(self, value: &MobSpeciesPropertyValue) -> bool {
                value.valid() && matches!((self, value), $($( (Self::$property, MobSpeciesPropertyValue::$value(_)) )|*)|*)
                    && match (self, value) {
                        (Self::SheepColor, MobSpeciesPropertyValue::Byte(color)) => *color < 16,
                        (Self::SlimeSize, MobSpeciesPropertyValue::Unsigned(size)) => (1..=127).contains(size),
                        (Self::PhantomPhase, MobSpeciesPropertyValue::Byte(phase)) => *phase <= 1,
                        (Self::HorseColor, MobSpeciesPropertyValue::Byte(color)) => *color < 7,
                        (Self::HorseMarkings, MobSpeciesPropertyValue::Byte(markings)) => *markings < 5,
                        (Self::HorseFlags, MobSpeciesPropertyValue::Byte(flags)) => flags & 129 == 0,
                        (Self::LlamaVariant, MobSpeciesPropertyValue::Byte(variant)) => *variant < 4,
                        (Self::LlamaStrength, MobSpeciesPropertyValue::Byte(strength)) => (1..=5).contains(strength),
                        (Self::LlamaCarpetColor, MobSpeciesPropertyValue::OptionalByte(color)) => color.is_none_or(|color| color < 16),
                        (Self::LlamaFlags, MobSpeciesPropertyValue::Byte(flags)) => flags & 133 == 0,
                        (Self::HorseTemper | Self::LlamaTemper, MobSpeciesPropertyValue::Unsigned(temper)) => *temper <= 100,
                        _ => true,
                    }
            }

            pub fn write(self, species: &mut MobSpecies, value: &MobSpeciesPropertyValue) -> bool {
                if !self.accepts(value) { return false; }
                let mut next = species.clone();
                match (self, &mut next, value) {
                    $($( (Self::$property, MobSpecies::$species { $field, .. }, MobSpeciesPropertyValue::$value(value)) => *$field = value.clone(), )*)*
                    _ => return false,
                }
                if !next.valid() { return false; }
                *species = next;
                true
            }
        }
    };
}

species_properties! {
    Horse { color => HorseColor: Byte, markings => HorseMarkings: Byte, flags => HorseFlags: Byte, chested => HorseChested: Flag, temper => HorseTemper: Unsigned, owner => HorseOwner: OptionalUuid },
    Llama { variant => LlamaVariant: Byte, strength => LlamaStrength: Byte, carpet_color => LlamaCarpetColor: OptionalByte, flags => LlamaFlags: Byte, chested => LlamaChested: Flag, temper => LlamaTemper: Unsigned, owner => LlamaOwner: OptionalUuid },
    Sheep { color => SheepColor: Byte, sheared => SheepSheared: Flag },
    Creeper { fuse => CreeperFuse: Unsigned, previous_fuse => CreeperPreviousFuse: Unsigned, maximum_fuse => CreeperMaximumFuse: Unsigned, radius => CreeperRadius: Unsigned, ignited => CreeperIgnited: Flag, powered => CreeperPowered: Flag },
    Slime { size => SlimeSize: Unsigned, jump_delay => SlimeJumpDelay: Unsigned, squish => SlimeSquish: Float, previous_squish => SlimePreviousSquish: Float, target_squish => SlimeTargetSquish: Float },
    Enderman { carried_block => EndermanCarriedBlock: OptionalBlockState, teleport_cooldown => EndermanTeleportCooldown: Unsigned, aggressive => EndermanAggressive: Flag, stared_at => EndermanStaredAt: Flag },
    Zombie { baby => ZombieBaby: Flag, conversion => ZombieConversion: OptionalUnsigned, breaking_doors => ZombieBreakingDoors: Flag },
    Skeleton { ranged_cooldown => SkeletonRangedCooldown: Unsigned, converting => SkeletonConverting: OptionalUnsigned },
    Bat { resting => BatResting: Flag, target => BatTarget: OptionalPosition },
    Phantom { size => PhantomSize: Unsigned, anchor => PhantomAnchor: Position, phase => PhantomPhase: Byte, target => PhantomTarget: Vector, sweep_ticks => PhantomSweepTicks: Unsigned },
    Blaze { attack_step => BlazeAttackStep: Unsigned, attack_time => BlazeAttackTime: Unsigned, unseen_ticks => BlazeUnseenTicks: Unsigned, charged => BlazeCharged: Flag, height_offset => BlazeHeightOffset: Float, height_ticks => BlazeHeightTicks: Unsigned },
}
