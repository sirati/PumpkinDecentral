use super::vector3::Vector3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemMedium {
    Air,
    Water,
    Lava,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct FluidContact {
    pub height: f64,
    pub touching: bool,
    pub push: Vector3<f64>,
    pub samples: usize,
}

impl FluidContact {
    pub fn observe(&mut self, marginal_height: f64, flow: Option<Vector3<f64>>) {
        self.height = self.height.max(marginal_height);
        self.touching = true;
        if let Some(flow) = flow {
            self.push += if self.height < 0.4 {
                flow * self.height
            } else {
                flow
            };
            self.samples += 1;
        }
    }
}

pub fn apply_fluid_push(
    velocity: Vector3<f64>,
    mut push: Vector3<f64>,
    samples: usize,
    speed: f64,
    normalize_flow: bool,
) -> Vector3<f64> {
    if push.length_squared() == 0.0 {
        return velocity;
    }
    if samples > 0 {
        push = push * (1.0 / samples as f64);
    }
    if normalize_flow {
        push = push.normalize();
    }
    push = push * speed;
    if velocity.x.abs() < 0.003
        && velocity.z.abs() < 0.003
        && velocity.length_squared() < 0.000_020_25
    {
        push = push.normalize() * 0.0045;
    }
    velocity + push
}

pub fn apply_item_drag(
    mut velocity: Vector3<f64>,
    medium: ItemMedium,
    gravity: f64,
) -> Vector3<f64> {
    match medium {
        ItemMedium::Air => velocity.y -= gravity,
        ItemMedium::Water | ItemMedium::Lava => {
            let drag = if medium == ItemMedium::Water {
                0.99
            } else {
                0.95
            };
            velocity.x *= drag;
            velocity.z *= drag;
            if velocity.y < 0.06 {
                velocity.y += 5.0e-4;
            }
        }
    }
    velocity
}

pub fn apply_item_friction(
    mut velocity: Vector3<f64>,
    ground_slipperiness: Option<f64>,
) -> Vector3<f64> {
    let horizontal = ground_slipperiness.map_or(0.98, |slipperiness| 0.98 * (slipperiness * 0.98));
    velocity = velocity.multiply(horizontal, 0.98, horizontal);
    if ground_slipperiness.is_some() && velocity.y < 0.0 {
        velocity.y = 0.0;
    }
    velocity
}

pub fn item_movement_due(
    age: u32,
    entity_id: i32,
    on_ground: bool,
    velocity: Vector3<f64>,
) -> bool {
    !on_ground
        || velocity.horizontal_length_squared() > 1.0e-5
        || (i64::from(age) + i64::from(entity_id)).rem_euclid(4) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shallow_contacts_use_running_depth_and_keep_water_lava_independent() {
        let mut water = FluidContact::default();
        let mut lava = FluidContact::default();
        water.observe(0.2, Some(Vector3::new(1.0, 0.0, 0.0)));
        water.observe(0.1, Some(Vector3::new(1.0, 0.0, 0.0)));
        assert_eq!(water.height, 0.2);
        assert_eq!(water.push, Vector3::new(0.4, 0.0, 0.0));
        assert_eq!(water.samples, 2);
        lava.observe(0.6, None);
        assert!(lava.touching);
        assert_eq!(lava.height, 0.6);
        assert_eq!(lava.samples, 0);
        assert_eq!(lava.push, Vector3::default());
        water.observe(0.5, Some(Vector3::new(0.0, 0.0, 1.0)));
        assert_eq!(water.push, Vector3::new(0.4, 0.0, 1.0));
    }

    #[test]
    fn fluid_push_preserves_no_flow_and_distinguishes_normalized_item_flow() {
        let velocity = Vector3::new(0.1, 0.0, 0.0);
        let zero = Vector3::new(0.0, 0.0, 0.0);
        assert_eq!(apply_fluid_push(velocity, zero, 0, 0.014, true), velocity);
        let flow = Vector3::new(2.0, 0.0, 0.0);
        assert_eq!(
            apply_fluid_push(velocity, flow, 4, 0.014, false),
            Vector3::new(0.1 + 0.5 * 0.014, 0.0, 0.0)
        );
        assert_eq!(
            apply_fluid_push(velocity, flow, 4, 0.014, true),
            Vector3::new(0.1 + 0.014, 0.0, 0.0)
        );
    }

    #[test]
    fn slow_velocity_receives_minimum_fluid_impulse_only_when_flow_exists() {
        let zero = Vector3::new(0.0, 0.0, 0.0);
        assert_eq!(
            apply_fluid_push(zero, Vector3::new(1.0, 0.0, 0.0), 1, 0.002333333, true),
            Vector3::new(0.0045, 0.0, 0.0)
        );
        assert_eq!(apply_fluid_push(zero, zero, 0, 0.014, true), zero);
    }

    #[test]
    fn fluid_motion_buoys_slow_items_without_accelerating_fast_upward_items() {
        for medium in [ItemMedium::Water, ItemMedium::Lava] {
            let slow = apply_item_drag(Vector3::new(1.0, -0.1, 1.0), medium, 0.04);
            assert_eq!(slow.y, -0.0995);
            assert_eq!(
                slow.x,
                if medium == ItemMedium::Water {
                    0.99
                } else {
                    0.95
                }
            );
            let fast = apply_item_drag(Vector3::new(0.0, 0.1, 0.0), medium, 0.04);
            assert_eq!(fast.y, 0.1);
        }
        assert_eq!(
            apply_item_drag(Vector3::new(0.0, 0.0, 0.0), ItemMedium::Air, 0.04).y,
            -0.04
        );
    }

    #[test]
    fn ground_stops_downward_velocity_and_surface_controls_horizontal_slide() {
        let stone = apply_item_friction(Vector3::new(1.0, -0.04, 1.0), Some(0.6));
        let ice = apply_item_friction(Vector3::new(1.0, -0.04, 1.0), Some(0.98));
        assert_eq!(stone.y, 0.0);
        assert!(ice.x > stone.x);
        let air = apply_item_friction(Vector3::new(1.0, -0.04, 1.0), None);
        assert!(air.y < 0.0);
    }

    #[test]
    fn stationary_schedule_is_bounded_and_id_addition_does_not_overflow() {
        let still = Vector3::new(0.0, 0.0, 0.0);
        assert_eq!(
            (0..4)
                .filter(|age| item_movement_due(*age, 7, true, still))
                .count(),
            1
        );
        assert!(item_movement_due(u32::MAX, i32::MAX, false, still));
        assert!(!item_movement_due(u32::MAX, i32::MAX, true, still));
        assert!(item_movement_due(0, 1, true, Vector3::new(1.0, 0.0, 0.0)));
    }
}
