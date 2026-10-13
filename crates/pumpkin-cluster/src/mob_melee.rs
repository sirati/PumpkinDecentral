use crate::actor_occupancy::CollisionShape;
use crate::canonical_action::ApplyFailure;

pub const DEFAULT_MOB_ATTACK_RANGE: f64 = 0.828_427_12;

#[derive(Clone, Copy)]
pub struct MobMeleeShape<'a> {
    pub position: [u64; 3],
    pub shape: &'a CollisionShape,
}

impl MobMeleeShape<'_> {
    fn bounds(self) -> Result<[[f64; 3]; 2], ApplyFailure> {
        if !self.shape.valid() || self.position.iter().any(|bits| {
            let value = f64::from_bits(*bits);
            !value.is_finite() || value.abs() > 30_000_000.0
        }) { return Err(ApplyFailure::InvalidAction); }
        Ok(std::array::from_fn(|side| std::array::from_fn(|axis|
            f64::from_bits(self.position[axis]) + f64::from_bits(self.shape.offsets[side][axis]))))
    }
}

pub fn in_range(source: MobMeleeShape<'_>, mount: Option<MobMeleeShape<'_>>, target: MobMeleeShape<'_>)
    -> Result<bool, ApplyFailure> {
    let [mut minimum, mut maximum] = source.bounds()?;
    if let Some(mount) = mount {
        let [mount_minimum, mount_maximum] = mount.bounds()?;
        for axis in [0, 2] {
            minimum[axis] = minimum[axis].min(mount_minimum[axis]);
            maximum[axis] = maximum[axis].max(mount_maximum[axis]);
        }
    }
    for axis in [0, 2] {
        minimum[axis] -= DEFAULT_MOB_ATTACK_RANGE;
        maximum[axis] += DEFAULT_MOB_ATTACK_RANGE;
    }
    let [target_minimum, target_maximum] = target.bounds()?;
    Ok((0..3).all(|axis| minimum[axis] < target_maximum[axis] && maximum[axis] > target_minimum[axis]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(width: f64, height: f64) -> CollisionShape {
        CollisionShape { offsets: [[-width / 2.0, 0.0, -width / 2.0].map(f64::to_bits),
            [width / 2.0, height, width / 2.0].map(f64::to_bits)], blocks_building: true, hanging: false }
    }

    #[test]
    fn mob_attack_uses_horizontal_expansion_and_real_vertical_overlap() {
        let body = shape(0.6, 1.8);
        let source = MobMeleeShape { position: [0.0_f64, 64.0, 0.0].map(f64::to_bits), shape: &body };
        let target = |x: f64, y: f64| MobMeleeShape { position: [x, y, 0.0].map(f64::to_bits), shape: &body };
        assert!(in_range(source, None, target(1.4, 64.0)).unwrap());
        assert!(!in_range(source, None, target(1.5, 64.0)).unwrap());
        assert!(!in_range(source, None, target(0.0, 65.8)).unwrap());
        assert!(in_range(source, None, target(0.0, 65.79)).unwrap());
    }

    #[test]
    fn mount_enlarges_only_horizontal_attack_bounds() {
        let body = shape(0.6, 1.8);
        let vehicle = shape(2.0, 0.6);
        let source = MobMeleeShape { position: [0.0_f64, 65.0, 0.0].map(f64::to_bits), shape: &body };
        let mount = MobMeleeShape { position: [0.0_f64, 64.0, 0.0].map(f64::to_bits), shape: &vehicle };
        let target = |x: f64, y: f64| MobMeleeShape { position: [x, y, 0.0].map(f64::to_bits), shape: &body };
        assert!(!in_range(source, None, target(2.0, 65.0)).unwrap());
        assert!(in_range(source, Some(mount), target(2.0, 65.0)).unwrap());
        assert!(!in_range(source, Some(mount), target(0.0, 63.0)).unwrap());
    }
}
