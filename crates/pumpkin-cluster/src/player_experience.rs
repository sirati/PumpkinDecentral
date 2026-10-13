use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerExperienceState {
    pub total: u32,
    pub level: u32,
    pub progress: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_curve_crosses_both_vanilla_growth_boundaries() {
        for (points, level, into, cost) in [
            (351, 15, 36, 37),
            (352, 16, 0, 42),
            (393, 16, 41, 42),
            (394, 17, 0, 47),
            (1506, 30, 111, 112),
            (1507, 31, 0, 121),
            (1627, 31, 120, 121),
            (1628, 32, 0, 130),
        ] {
            let state = PlayerExperienceState::from_total(points).unwrap();
            assert_eq!(state.level, level);
            assert_eq!(f32::from_bits(state.progress), into as f32 / cost as f32);
        }
        assert!(
            PlayerExperienceState::from_total(i32::MAX as u32)
                .unwrap()
                .is_valid()
        );
        assert!(PlayerExperienceState::from_total(u32::MAX).is_none());
    }

    #[test]
    fn level_spending_keeps_fraction_and_total_and_rejects_insufficient_levels() {
        let before = PlayerExperienceState::from_total(400).unwrap();
        let after = before.spend_levels(3).unwrap();
        assert_eq!(after.level, 14);
        assert_eq!(after.total, 400);
        assert_eq!(after.progress, before.progress);
        assert!(before.spend_levels(18).is_none());
        assert_eq!(before.spend_levels(0), Some(before));
    }

    #[test]
    fn collecting_after_level_spending_advances_current_level_without_restoring_spent_levels() {
        let spent = PlayerExperienceState::from_total(394)
            .unwrap()
            .spend_levels(5)
            .unwrap();
        let collected = spent.collect(31).unwrap();
        assert_eq!(collected.total, 425);
        assert_eq!(collected.level, 13);
        assert_eq!(f32::from_bits(collected.progress), 0.0);
        assert_ne!(
            collected.level,
            PlayerExperienceState::from_total(425).unwrap().level
        );
        let partial = collected.collect(11).unwrap();
        assert_eq!(partial.level, 13);
        assert!((f32::from_bits(partial.progress) - 11.0 / 33.0).abs() < f32::EPSILON);
        assert!(
            PlayerExperienceState::from_total(i32::MAX as u32)
                .unwrap()
                .collect(1)
                .is_none()
        );
    }
}

impl PlayerExperienceState {
    pub fn is_valid(self) -> bool {
        self.total <= i32::MAX as u32
            && self.level <= i32::MAX as u32
            && f32::from_bits(self.progress).is_finite()
            && (0.0..=1.0).contains(&f32::from_bits(self.progress))
    }

    pub fn from_total(total: u32) -> Option<Self> {
        if total > i32::MAX as u32 {
            return None;
        }
        let mut low = 0_u32;
        let mut high = 65_536_u32;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if Self::points_at_level(middle) <= u64::from(total) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let into_level = u64::from(total) - Self::points_at_level(low);
        let progress = (into_level as f32 / Self::points_for_next_level(low) as f32).to_bits();
        Some(Self {
            total,
            level: low,
            progress,
        })
    }

    pub fn spend_levels(self, count: u32) -> Option<Self> {
        if !self.is_valid() {
            return None;
        }
        Some(Self {
            level: self.level.checked_sub(count)?,
            ..self
        })
    }

    pub fn collect(self, points: u32) -> Option<Self> {
        if !self.is_valid() {
            return None;
        }
        let total = self
            .total
            .checked_add(points)
            .filter(|total| *total <= i32::MAX as u32)?;
        let mut level = self.level;
        let mut progress = f32::from_bits(self.progress)
            + points as f32 / Self::points_for_next_level(level) as f32;
        while progress >= 1.0 {
            progress = (progress - 1.0) * Self::points_for_next_level(level) as f32;
            level = level
                .checked_add(1)
                .filter(|level| *level <= i32::MAX as u32)?;
            progress /= Self::points_for_next_level(level) as f32;
        }
        let next = Self {
            total,
            level,
            progress: progress.to_bits(),
        };
        next.is_valid().then_some(next)
    }

    pub fn points_at_level(level: u32) -> u64 {
        let level = u128::from(level);
        let points = match level {
            0..=16 => level * level + 6 * level,
            17..=31 => (5 * level * level + 720 - 81 * level) / 2,
            _ => (9 * level * level + 4440 - 325 * level) / 2,
        };
        u64::try_from(points).unwrap_or(u64::MAX)
    }

    pub fn points_for_next_level(level: u32) -> u64 {
        let level = u64::from(level);
        match level {
            0..=15 => 2 * level + 7,
            16..=30 => 5 * level - 38,
            _ => 9 * level - 158,
        }
    }
}
