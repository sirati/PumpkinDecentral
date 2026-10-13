use crate::time::TickStamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArmorStandState {
    pub flags: u8,
    pub disabled_slots: u32,
    pub poses: [[u32; 3]; 6],
    pub last_hit: Option<TickStamp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StandProperty {
    Flags,
    DisabledSlots,
    Pose(u8),
    LastHit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StandPropertyValue {
    Flags(u8),
    DisabledSlots(u32),
    Pose([u32; 3]),
    LastHit(Option<TickStamp>),
}

impl ArmorStandState {
    pub fn valid(&self) -> bool {
        self.flags & !0x1d == 0
            && self.disabled_slots & !0x003f_3f3f == 0
            && self
                .poses
                .iter()
                .flatten()
                .all(|value| f32::from_bits(*value).is_finite())
    }

    pub fn vanilla() -> Self {
        Self {
            flags: 0,
            disabled_slots: 0,
            last_hit: None,
            poses: [
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [-10.0, 0.0, -10.0],
                [-15.0, 0.0, 10.0],
                [-1.0, 0.0, -1.0],
                [1.0, 0.0, 1.0],
            ]
            .map(|pose: [f32; 3]| pose.map(f32::to_bits)),
        }
    }
}

impl StandProperty {
    pub const ALL: [Self; 9] = [
        Self::Flags,
        Self::DisabledSlots,
        Self::Pose(0),
        Self::Pose(1),
        Self::Pose(2),
        Self::Pose(3),
        Self::Pose(4),
        Self::Pose(5),
        Self::LastHit,
    ];

    pub fn accepts(self, value: &StandPropertyValue) -> bool {
        match (self, value) {
            (Self::Flags, StandPropertyValue::Flags(flags)) => *flags & !0x1d == 0,
            (Self::DisabledSlots, StandPropertyValue::DisabledSlots(slots)) => {
                *slots & !0x003f_3f3f == 0
            }
            (Self::Pose(index), StandPropertyValue::Pose(pose)) => {
                index < 6 && pose.iter().all(|bits| f32::from_bits(*bits).is_finite())
            }
            (Self::LastHit, StandPropertyValue::LastHit(_)) => true,
            _ => false,
        }
    }

    pub fn read(self, stand: &ArmorStandState) -> Option<StandPropertyValue> {
        Some(match self {
            Self::Flags => StandPropertyValue::Flags(stand.flags),
            Self::DisabledSlots => StandPropertyValue::DisabledSlots(stand.disabled_slots),
            Self::Pose(index) => StandPropertyValue::Pose(*stand.poses.get(usize::from(index))?),
            Self::LastHit => StandPropertyValue::LastHit(stand.last_hit),
        })
    }

    pub fn write(self, stand: &mut ArmorStandState, value: &StandPropertyValue) -> bool {
        if !self.accepts(value) {
            return false;
        }
        match (self, value) {
            (Self::Flags, StandPropertyValue::Flags(flags)) => stand.flags = *flags,
            (Self::DisabledSlots, StandPropertyValue::DisabledSlots(slots)) => {
                stand.disabled_slots = *slots
            }
            (Self::Pose(index), StandPropertyValue::Pose(pose)) => {
                stand.poses[usize::from(index)] = *pose
            }
            (Self::LastHit, StandPropertyValue::LastHit(tick)) => stand.last_hit = *tick,
            _ => return false,
        }
        true
    }
}
