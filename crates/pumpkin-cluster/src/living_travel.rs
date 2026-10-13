
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TravelInput {
    pub position: [f64; 3],
    pub velocity: [f64; 3],
    pub movement: [f64; 3],
    pub yaw: f64,
    pub on_ground: bool,
    pub sprinting: bool,
    pub climbing: bool,
    pub jumping: bool,
    pub flying: bool,
    pub movement_speed: f64,
    pub air_speed: f64,
    pub slipperiness: f64,
    pub gravity: f64,
    pub vertical_drag: f64,
    pub slow_falling: bool,
    pub levitation: Option<u8>,
    pub fluid: TravelFluid,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TravelFluid {
    Air,
    Water { friction: f64, efficiency: f64, dolphins_grace: bool },
    Lava { shallow: bool },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TravelMove {
    pub displacement: [f64; 3],
    pub horizontal_collision: bool,
    pub vertical_collision: bool,
    pub on_ground: bool,
    pub fluid_exit_clear: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TravelOutput {
    pub position: [f64; 3],
    pub velocity: [f64; 3],
    pub collision: TravelMove,
    pub reset_fall_distance: bool,
}

impl TravelInput {
    pub fn valid(self) -> bool {
        self.position.into_iter().chain(self.velocity).chain(self.movement)
            .chain([self.yaw, self.movement_speed, self.air_speed, self.slipperiness,
                self.gravity, self.vertical_drag]).all(f64::is_finite)
            && self.slipperiness > 0.0 && self.movement_speed >= 0.0 && self.air_speed >= 0.0
            && (0.0..=1.0).contains(&self.vertical_drag)
            && match self.fluid {
                TravelFluid::Water { friction, efficiency, .. } =>
                    friction.is_finite() && (0.0..=1.0).contains(&friction)
                        && efficiency.is_finite() && (0.0..=1.0).contains(&efficiency),
                _ => true,
            }
    }
}

pub fn travel<E>(input: TravelInput,
    collide: impl FnOnce([f64; 3], [f64; 3]) -> Result<TravelMove, E>,
) -> Result<Option<TravelOutput>, E> {
    if !input.valid() { return Ok(None); }
    let falling = input.velocity[1] <= 0.0;
    let gravity = if falling && input.slow_falling { input.gravity.min(0.01) } else { input.gravity };
    let (speed, friction) = match input.fluid {
        TravelFluid::Air if input.on_ground =>
            (input.movement_speed * 0.216_000_02 / input.slipperiness.powi(3), input.slipperiness * 0.91),
        TravelFluid::Air => (input.air_speed, 0.91),
        TravelFluid::Water { friction, efficiency, dolphins_grace } => {
            let efficiency = efficiency * if input.on_ground { 1.0 } else { 0.5 };
            (0.02 + (input.movement_speed - 0.02) * efficiency,
                if dolphins_grace { 0.96 } else {
                    let base = if input.sprinting { 0.9 } else { friction };
                    base + (0.546_000_06 - base) * efficiency
                })
        }
        TravelFluid::Lava { .. } => (0.02, 0.5),
    };
    let mut velocity = input.velocity;
    let length = input.movement.iter().map(|value| value * value).sum::<f64>();
    if length >= 1.0e-7 {
        let scale = speed / length.sqrt().max(1.0);
        let movement = input.movement.map(|value| value * scale);
        let (sin, cos) = input.yaw.to_radians().sin_cos();
        velocity[0] += movement[0] * cos - movement[2] * sin;
        velocity[1] += movement[1];
        velocity[2] += movement[2] * cos + movement[0] * sin;
    }
    if input.climbing && matches!(input.fluid, TravelFluid::Air) {
        velocity[0] = velocity[0].clamp(-0.15, 0.15);
        velocity[1] = velocity[1].max(-0.15);
        velocity[2] = velocity[2].clamp(-0.15, 0.15);
    }
    if !velocity.into_iter().all(f64::is_finite) { return Ok(None); }
    let collision = collide(input.position, velocity)?;
    if !collision.displacement.into_iter().all(f64::is_finite) { return Ok(None); }
    let position = std::array::from_fn(|axis| input.position[axis] + collision.displacement[axis]);
    for axis in 0..3 {
        if collision.displacement[axis] != velocity[axis] { velocity[axis] = 0.0; }
    }
    match input.fluid {
        TravelFluid::Air => {
            if (collision.horizontal_collision || input.jumping) && input.climbing { velocity[1] = 0.2; }
            if input.flying {
                velocity[1] *= 0.6;
            } else if let Some(amplifier) = input.levitation {
                velocity[1] += (0.05 * (f64::from(amplifier) + 1.0) - velocity[1]) * 0.2;
            } else { velocity[1] -= gravity; }
            velocity[0] *= friction; velocity[2] *= friction;
            velocity[1] *= input.vertical_drag;
        }
        TravelFluid::Water { .. } => {
            if collision.horizontal_collision && input.climbing { velocity[1] = 0.2; }
            velocity[0] *= friction; velocity[1] *= 0.8; velocity[2] *= friction;
            fluid_gravity(&mut velocity[1], gravity, falling, input.sprinting);
        }
        TravelFluid::Lava { shallow } => {
            velocity[0] *= 0.5; velocity[2] *= 0.5;
            velocity[1] *= if shallow { 0.8 } else { 0.5 };
            if shallow { fluid_gravity(&mut velocity[1], gravity, falling, input.sprinting); }
            velocity[1] -= gravity / 4.0;
        }
    }
    if !matches!(input.fluid, TravelFluid::Air) && collision.horizontal_collision && collision.fluid_exit_clear {
        velocity[1] = 0.3;
    }
    if !position.into_iter().chain(velocity).all(f64::is_finite) { return Ok(None); }
    Ok(Some(TravelOutput { position, velocity, collision,
        reset_fall_distance: input.climbing || input.flying || input.slow_falling
            || input.levitation.is_some() || !matches!(input.fluid, TravelFluid::Air) }))
}

fn fluid_gravity(dy: &mut f64, gravity: f64, falling: bool, sprinting: bool) {
    if gravity != 0.0 && !sprinting {
        if falling && (*dy - 0.005).abs() >= 0.003 && (*dy - gravity / 16.0).abs() < 0.003 {
            *dy = -0.003;
        } else { *dy -= gravity / 16.0; }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> TravelInput {
        TravelInput { position: [0.0, 10.0, 0.0], velocity: [0.0; 3], movement: [0.0; 3],
            yaw: 0.0, on_ground: false, sprinting: false, climbing: false, jumping: false,
            flying: false, movement_speed: 0.1, air_speed: 0.02, slipperiness: 0.6,
            gravity: 0.08, vertical_drag: 0.98, slow_falling: false,
            levitation: None, fluid: TravelFluid::Air }
    }
    fn clear(displacement: [f64; 3]) -> Result<TravelMove, ()> {
        Ok(TravelMove { displacement, horizontal_collision: false, vertical_collision: false,
            on_ground: false, fluid_exit_clear: true })
    }
    #[test]
    fn gravity_applies_after_displacement_and_slow_fall_limits_acceleration() {
        let source = input();
        let output = travel(source, |_, movement| clear(movement)).unwrap().unwrap();
        assert_eq!(output.position, source.position);
        assert!((output.velocity[1] + 0.0784).abs() < 1.0e-12);
        let slow = travel(TravelInput { slow_falling: true, ..source }, |_, movement| clear(movement)).unwrap().unwrap();
        assert!((slow.velocity[1] + 0.0098).abs() < 1.0e-12);
        assert!(slow.reset_fall_distance);
    }
    #[test]
    fn collision_clips_only_blocked_axes_and_propagates_original_read_failure() {
        let source = TravelInput { velocity: [1.0, -0.5, 0.25], ..input() };
        let output = travel(source, |position, movement| {
            assert_eq!(position, source.position);
            assert_eq!(movement, source.velocity);
            Ok::<_, ()>(TravelMove { displacement: [0.0, 0.0, 0.25], horizontal_collision: true,
                vertical_collision: true, on_ground: true, fluid_exit_clear: false })
        }).unwrap().unwrap();
        assert_eq!(output.position, [0.0, 10.0, 0.25]);
        assert_eq!(output.velocity[0], 0.0);
        assert!((output.velocity[2] - 0.2275).abs() < 1.0e-12);
        assert_eq!(travel(source, |_, _| Err("original chunk absent")), Err("original chunk absent"));
    }
    #[test]
    fn water_efficiency_and_fluid_step_use_frozen_contact_facts() {
        let source = TravelInput { movement: [0.0, 0.0, 1.0],
            fluid: TravelFluid::Water { friction: 0.8, efficiency: 1.0, dolphins_grace: false }, ..input() };
        let output = travel(source, |_, movement| {
            assert!((movement[2] - 0.06).abs() < 1.0e-12);
            Ok::<_, ()>(TravelMove { displacement: movement, horizontal_collision: true,
                vertical_collision: false, on_ground: false, fluid_exit_clear: true })
        }).unwrap().unwrap();
        assert_eq!(output.velocity[1], 0.3);
        assert!(output.reset_fall_distance);
    }
}
