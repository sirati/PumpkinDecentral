use crate::actor_relation::VehicleControlInput;
use crate::entity_body::{EntityBody, EntityPayload, EntitySpatial, VehicleKind, VehicleState};
pub fn passenger_position(
    kind: VehicleKind,
    seat: u8,
    spatial: &EntitySpatial,
) -> Option<[f64; 3]> {
    let mut position = spatial.position.map(f64::from_bits);
    let yaw = f32::from_bits(spatial.yaw) as f64;
    if !position.iter().all(|value| value.is_finite()) || !yaw.is_finite() {
        return None;
    }
    let (lateral, height) = match kind {
        VehicleKind::Boat if seat < 2 => (if seat == 0 { 0.2 } else { -0.6 }, -0.1),
        VehicleKind::ChestBoat if seat == 0 => (0.15, -0.1),
        VehicleKind::Minecart if seat == 0 => (0.0, 0.0),
        _ => return None,
    };
    let angle = yaw.to_radians();
    position[0] -= lateral * angle.sin();
    position[1] += height;
    position[2] += lateral * angle.cos();
    Some(position)
}
use crate::item_contact::ContactIdentity;
use crate::realm_completion::PeerGeneration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VehicleControllerFrame {
    pub claimed: Option<VehicleControlInput>,
    pub accepted_tick: crate::time::TickStamp,
    pub effective_spatial: EntitySpatial,
    pub state: VehicleState,
    pub filter: VehicleFilterDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VehicleFilterDecision {
    Physics,
    AcceptedClaim,
    ClippedClaim,
    RejectedDistance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VehicleControllerRecord {
    pub birth: crate::entity_body::EntityBirth,
    pub owner: PeerGeneration,
    pub claimed: Option<VehicleControlInput>,
    pub effective: EntitySpatial,
    pub tick: crate::time::TickStamp,
    pub filter: VehicleFilterDecision,
}

#[derive(Default)]
pub struct VehicleControllerBank {
    records: std::collections::BTreeMap<crate::protocol::GlobalEntityId, VehicleControllerRecord>,
}

impl VehicleControllerBank {
    pub fn get(&self, id: crate::protocol::GlobalEntityId) -> Option<&VehicleControllerRecord> {
        self.records.get(&id)
    }

    pub fn accept(&mut self, body: &EntityBody, frame: &VehicleControllerFrame) -> bool {
        let previous = self.records.get(&body.id);
        if previous.is_some_and(|record| {
            record.birth == body.birth
                && record.owner == body.owner
                && !frame.accepted_tick.is_after(record.tick)
        }) {
            return false;
        }
        let claimed = frame.claimed.clone().or_else(|| {
            previous
                .filter(|record| record.birth == body.birth && record.owner == body.owner)
                .and_then(|record| record.claimed.clone())
        });
        self.records.insert(
            body.id,
            VehicleControllerRecord {
                birth: body.birth.clone(),
                owner: body.owner,
                claimed,
                effective: frame.effective_spatial.clone(),
                tick: frame.accepted_tick,
                filter: frame.filter,
            },
        );
        true
    }

    pub fn retain(
        &mut self,
        mut present: impl FnMut(
            crate::protocol::GlobalEntityId,
            &crate::entity_body::EntityBirth,
            PeerGeneration,
        ) -> bool,
    ) {
        self.records
            .retain(|id, record| present(*id, &record.birth, record.owner));
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VehicleEnvironment {
    pub water_surface: Option<f64>,
    pub submerged: bool,
    pub ground: bool,
    pub friction: f64,
    pub rail_direction: Option<[f64; 3]>,
    pub rail_position: Option<[f64; 3]>,
    pub powered_rail: bool,
    pub braking_rail: bool,
}

pub trait VehicleCollisionView {
    fn environment(&self, spatial: &EntitySpatial, kind: VehicleKind)
    -> Option<VehicleEnvironment>;
    fn clip(
        &self,
        spatial: &EntitySpatial,
        displacement: [f64; 3],
        kind: VehicleKind,
    ) -> Option<([f64; 3], bool)>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VehicleControlError {
    InvalidInput,
    ForeignOwner,
    WrongMount,
    WrongDriver,
    StaleInput,
    UnavailableClaim,
}

pub fn capture(
    body: &EntityBody,
    source: PeerGeneration,
    input: &VehicleControlInput,
    driver_mount: Option<&ContactIdentity>,
) -> Result<VehicleControllerFrame, VehicleControlError> {
    if !input.is_valid() {
        return Err(VehicleControlError::InvalidInput);
    }
    if body.owner != source {
        return Err(VehicleControlError::ForeignOwner);
    }
    let identity = ContactIdentity::Entity {
        id: body.id,
        birth: body.birth.clone(),
    };
    if input.mount != identity || driver_mount != Some(&identity) {
        return Err(VehicleControlError::WrongMount);
    }
    if body.passengers.get(&0) != Some(&ContactIdentity::Player(input.driver)) {
        return Err(VehicleControlError::WrongDriver);
    }
    let EntityPayload::Vehicle(before) = &body.payload else {
        return Err(VehicleControlError::WrongMount);
    };
    if before.driver == Some(input.driver)
        && before
            .control_sequence
            .is_some_and(|sequence| !input.input_sequence.is_newer_than(sequence))
    {
        return Err(VehicleControlError::StaleInput);
    }
    if before
        .simulation_tick
        .is_some_and(|tick| !input.simulation_tick.is_after(tick))
    {
        return Err(VehicleControlError::StaleInput);
    }
    let mut state = before.clone();
    state.driver = Some(input.driver);
    state.control_sequence = Some(input.input_sequence);
    if input.paddles_changed && matches!(state.kind, VehicleKind::Boat | VehicleKind::ChestBoat) {
        state.paddle_left = input.paddle_left;
        state.paddle_right = input.paddle_right;
    }
    Ok(VehicleControllerFrame {
        claimed: Some(input.clone()),
        accepted_tick: input.simulation_tick,
        effective_spatial: body.spatial.clone(),
        state,
        filter: VehicleFilterDecision::Physics,
    })
}

impl VehicleControllerFrame {
    pub fn idle(body: &EntityBody, accepted_tick: crate::time::TickStamp) -> Option<Self> {
        let EntityPayload::Vehicle(state) = &body.payload else {
            return None;
        };
        if state
            .simulation_tick
            .is_some_and(|tick| !accepted_tick.is_after(tick))
        {
            return None;
        }
        let mut state = state.clone();
        if state
            .driver
            .is_some_and(|driver| body.passengers.get(&0) != Some(&ContactIdentity::Player(driver)))
        {
            state.driver = None;
            state.control_sequence = None;
            state.paddle_left = false;
            state.paddle_right = false;
        }
        Some(Self {
            claimed: None,
            accepted_tick,
            effective_spatial: body.spatial.clone(),
            state,
            filter: VehicleFilterDecision::Physics,
        })
    }

    pub fn simulate(
        &mut self,
        view: &impl VehicleCollisionView,
    ) -> Result<(), VehicleControlError> {
        let mut spatial = self.effective_spatial.clone();
        let environment = view
            .environment(&spatial, self.state.kind)
            .ok_or(VehicleControlError::UnavailableClaim)?;
        if !environment.friction.is_finite()
            || !(0.0..=1.0).contains(&environment.friction)
            || environment
                .water_surface
                .is_some_and(|height| !height.is_finite())
        {
            return Err(VehicleControlError::InvalidInput);
        }
        if let Some(position) = environment.rail_position {
            if position.iter().any(|value| !value.is_finite()) {
                return Err(VehicleControlError::InvalidInput);
            }
            spatial.position = position.map(f64::to_bits);
        }
        let mut velocity = spatial
            .velocity_micro
            .map(|value| value as f64 / 1_000_000.0);
        let mut yaw = f32::from_bits(spatial.yaw);
        let boat = matches!(self.state.kind, VehicleKind::Boat | VehicleKind::ChestBoat);
        if boat {
            let sideways = self.claimed.as_ref().map_or(0, |input| input.sideways);
            let forward = self.claimed.as_ref().map_or(0, |input| input.forward);
            yaw += f32::from(sideways) * 1.0;
            let acceleration = if forward > 0 { 0.04 } else { 0.005 };
            let radians = f64::from(yaw).to_radians();
            velocity[0] -= radians.sin() * acceleration * f64::from(forward);
            velocity[2] += radians.cos() * acceleration * f64::from(forward);
            let drag = if environment.water_surface.is_some() {
                0.9
            } else if environment.ground {
                environment.friction
            } else {
                0.9
            };
            velocity[0] *= drag;
            velocity[2] *= drag;
            velocity[1] -= 0.04;
            if let Some(surface) = environment.water_surface {
                let height = f64::from_bits(spatial.position[1]);
                velocity[1] += ((surface - height).max(0.0) * 0.06153846).min(0.2);
                velocity[1] *= 0.75;
            }
            self.state.underwater_ticks = if environment.submerged {
                self.state.underwater_ticks.saturating_add(1).min(60)
            } else {
                self.state.underwater_ticks.saturating_sub(1)
            };
        } else {
            velocity[1] -= 0.04;
            if let Some(direction) = environment.rail_direction {
                let norm = direction[0].hypot(direction[2]);
                if direction.iter().any(|axis| !axis.is_finite())
                    || !norm.is_finite()
                    || norm <= 0.0
                {
                    return Err(VehicleControlError::InvalidInput);
                }
                let unit = [
                    direction[0] / norm,
                    direction[1] / norm,
                    direction[2] / norm,
                ];
                let mut speed = velocity[0] * unit[0] + velocity[2] * unit[2];
                speed -= unit[1] * 0.0078125;
                if speed.abs() < 0.01
                    && self.state.kind == VehicleKind::Minecart
                    && let Some(input) = &self.claimed
                    && input.forward != 0
                {
                    let look = f64::from(f32::from_bits(input.driver_effective_yaw)).to_radians();
                    speed += (-look.sin() * unit[0] + look.cos() * unit[2])
                        * f64::from(input.forward)
                        * 0.1;
                }
                if environment.braking_rail {
                    speed *= 0.5;
                }
                if environment.powered_rail {
                    if speed.abs() > 0.01 {
                        speed += speed.signum() * 0.06;
                    } else {
                        let heading = f64::from(yaw).to_radians();
                        speed = (-heading.sin() * unit[0] + heading.cos() * unit[2]) * 0.1;
                    }
                }
                if self.state.kind == VehicleKind::FurnaceMinecart && self.state.fuel_ticks > 0 {
                    let push = self.state.furnace_push_micro;
                    speed += ((push[0] as f64 / 1_000_000.0) * unit[0]
                        + (push[1] as f64 / 1_000_000.0) * unit[2])
                        * 0.04;
                }
                speed = speed.clamp(-0.4, 0.4);
                velocity = unit.map(|component| component * speed);
                velocity[0] *= 0.996;
                velocity[2] *= 0.996;
                if speed.abs() > 0.000001 {
                    yaw = (-velocity[0]).atan2(velocity[2]).to_degrees() as f32;
                }
            } else {
                velocity[0] = velocity[0].clamp(-0.4, 0.4);
                velocity[2] = velocity[2].clamp(-0.4, 0.4);
                let drag = if environment.ground { 0.5 } else { 0.95 };
                velocity[0] *= drag;
                velocity[2] *= drag;
            }
        }
        let (delta, grounded) = view
            .clip(&spatial, velocity, self.state.kind)
            .ok_or(VehicleControlError::UnavailableClaim)?;
        if delta.iter().any(|value| !value.is_finite()) {
            return Err(VehicleControlError::InvalidInput);
        }
        let position = spatial.position.map(f64::from_bits);
        let next = core::array::from_fn(|axis| position[axis] + delta[axis]);
        if next
            .iter()
            .any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        {
            return Err(VehicleControlError::InvalidInput);
        }
        self.effective_spatial.position = next.map(f64::to_bits);
        self.effective_spatial.velocity_micro = core::array::from_fn(|axis| {
            if delta[axis] == velocity[axis] {
                (velocity[axis] * 1_000_000.0).round() as i64
            } else {
                0
            }
        });
        self.effective_spatial.yaw = yaw.to_bits();
        self.effective_spatial.on_ground = grounded;
        if let Some(claimed) = self
            .claimed
            .as_ref()
            .and_then(|input| input.claimed.as_ref())
        {
            let initial = spatial.position.map(f64::from_bits);
            let destination = claimed.position.map(f64::from_bits);
            let requested =
                core::array::from_fn::<_, 3, _>(|axis| destination[axis] - initial[axis]);
            let distance = requested.into_iter().map(|axis| axis * axis).sum::<f64>();
            let speed = velocity
                .into_iter()
                .map(|axis| axis * axis)
                .sum::<f64>()
                .sqrt();
            let budget = (speed + 0.125).clamp(0.125, 0.8);
            if distance <= budget * budget {
                let (clipped, grounded) = view
                    .clip(&spatial, requested, self.state.kind)
                    .ok_or(VehicleControlError::UnavailableClaim)?;
                if clipped.iter().any(|value| !value.is_finite()) {
                    return Err(VehicleControlError::InvalidInput);
                }
                self.filter = if clipped == requested {
                    VehicleFilterDecision::AcceptedClaim
                } else {
                    VehicleFilterDecision::ClippedClaim
                };
                self.effective_spatial.position =
                    core::array::from_fn(|axis| (initial[axis] + clipped[axis]).to_bits());
                self.effective_spatial.velocity_micro =
                    clipped.map(|value| (value * 1_000_000.0).round() as i64);
                self.effective_spatial.yaw = claimed.yaw;
                self.effective_spatial.pitch =
                    f32::from_bits(claimed.pitch).clamp(-90.0, 90.0).to_bits();
                self.effective_spatial.on_ground = grounded;
            } else {
                self.filter = VehicleFilterDecision::RejectedDistance;
            }
        }
        self.state.simulation_tick = Some(self.accepted_tick);
        Ok(())
    }
    pub fn control_action(
        &self,
        id: crate::canonical_action::ActionId,
        body: &EntityBody,
        chunk: crate::protocol::ChunkKey,
    ) -> Option<crate::canonical_action::Action> {
        use crate::canonical_action::{
            Action, ActionClass, ActionKind, ChunkPart, ClientOrigin, Effect, EntityMoveAuthority,
            Stakeholder, Target, UndoProcedure,
        };
        if self.claimed.as_ref().is_some_and(|input| {
            input.mount
                != (ContactIdentity::Entity {
                    id: body.id,
                    birth: body.birth.clone(),
                })
        }) || id.actor != crate::actor::ActorId::Entity(body.id)
            || id.incarnation != u64::from(body.id.generation)
            || !body.belongs_to(body.id, chunk)
        {
            return None;
        }
        let mut after = body.clone();
        after.payload = EntityPayload::Vehicle(self.state.clone());
        let after = crate::canonical_action::Value::Entity((after).into());
        let action = Action {
            edit_authority: None,
            id,
            tick: self.accepted_tick,
            class: ActionClass::Infallible,
            chunk_transition: false,
            actor_chunk: Some(chunk),
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            dependencies: Vec::new(),
            conditions: Vec::new(),
            effects: [
                crate::vehicle_property::VehicleProperty::SimulationTick,
                crate::vehicle_property::VehicleProperty::Control,
                crate::vehicle_property::VehicleProperty::Underwater,
            ]
            .into_iter()
            .map(|property| Effect::UpdateVehicleProperty {
                target: Target::Chunk {
                    key: chunk,
                    part: ChunkPart::VehicleProperty {
                        actor: body.id,
                        property,
                    },
                    inventory: None,
                },
                authority: EntityMoveAuthority {
                    birth: body.birth.clone(),
                    body_owner: body.owner,
                    issuer: body.owner,
                },
                value: crate::vehicle_property::read(&after, property).unwrap(),
            })
            .collect(),
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

    pub fn filter_position(&mut self, position: [f64; 3], on_ground: bool) -> bool {
        if position
            .iter()
            .any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        {
            return false;
        }
        self.effective_spatial.position = position.map(f64::to_bits);
        self.effective_spatial.on_ground = on_ground;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spatial(yaw: f32) -> EntitySpatial {
        EntitySpatial {
            world: crate::protocol::WorldId::new(0, 1),
            position: [10.0_f64, 64.0, 20.0].map(f64::to_bits),
            velocity_micro: [0; 3],
            yaw: yaw.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
        }
    }

    struct Rail {
        direction: [f64; 3],
        invalid_clip: bool,
    }

    impl VehicleCollisionView for Rail {
        fn environment(&self, _: &EntitySpatial, _: VehicleKind) -> Option<VehicleEnvironment> {
            Some(VehicleEnvironment {
                water_surface: None,
                submerged: false,
                ground: true,
                friction: 0.6,
                rail_direction: Some(self.direction),
                rail_position: None,
                powered_rail: true,
                braking_rail: false,
            })
        }
        fn clip(
            &self,
            _: &EntitySpatial,
            displacement: [f64; 3],
            _: VehicleKind,
        ) -> Option<([f64; 3], bool)> {
            Some((
                if self.invalid_clip {
                    [f64::NAN, 0.0, 0.0]
                } else {
                    displacement
                },
                true,
            ))
        }
    }

    fn frame(yaw: f32) -> VehicleControllerFrame {
        VehicleControllerFrame {
            claimed: None,
            accepted_tick: crate::time::TickStamp(1),
            effective_spatial: spatial(yaw),
            state: VehicleState::new(VehicleKind::Minecart),
            filter: VehicleFilterDecision::Physics,
        }
    }

    #[test]
    fn powered_rail_launches_stationary_cart_in_heading_direction() {
        for (yaw, direction, expected) in [
            (0.0, [0.0, 0.0, 1.0], [0, 0, 99_600]),
            (90.0, [1.0, 0.0, 0.0], [-99_600, 0, 0]),
            (180.0, [0.0, 0.0, 1.0], [0, 0, -99_600]),
        ] {
            let mut frame = frame(yaw);
            frame
                .simulate(&Rail {
                    direction,
                    invalid_clip: false,
                })
                .unwrap();
            assert_eq!(frame.effective_spatial.velocity_micro, expected);
            assert!((f32::from_bits(frame.effective_spatial.yaw) - yaw).abs() < 0.001);
        }
    }

    #[test]
    fn invalid_collision_result_does_not_publish_effective_position() {
        let mut frame = frame(0.0);
        let before = frame.effective_spatial.clone();
        assert_eq!(
            frame.simulate(&Rail {
                direction: [0.0, 0.0, 1.0],
                invalid_clip: true
            }),
            Err(VehicleControlError::InvalidInput)
        );
        assert_eq!(frame.effective_spatial, before);
    }

    #[test]
    fn boat_passengers_follow_heading_and_keep_distinct_seats() {
        for (yaw, axis, sign) in [(0.0, 2, 1.0), (90.0, 0, -1.0)] {
            let spatial = spatial(yaw);
            let front = passenger_position(VehicleKind::Boat, 0, &spatial).unwrap();
            let rear = passenger_position(VehicleKind::Boat, 1, &spatial).unwrap();
            assert!(((front[axis] - rear[axis]) - sign * 0.8).abs() < 1e-9);
            assert!((front[1] - 63.9).abs() < 1e-9);
            assert!(passenger_position(VehicleKind::Boat, 2, &spatial).is_none());
        }
        assert!(passenger_position(VehicleKind::ChestBoat, 1, &spatial(0.0)).is_none());
        assert!(passenger_position(VehicleKind::Boat, 0, &spatial(f32::NAN)).is_none());
    }
}
