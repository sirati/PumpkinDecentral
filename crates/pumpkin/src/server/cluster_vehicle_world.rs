use std::collections::BTreeSet;

use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::ActionState;
use pumpkin_cluster::entity_body::{EntitySpatial, VehicleKind};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_cluster::vehicle_control::{VehicleCollisionView, VehicleEnvironment};
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

pub(super) struct VehicleWorldView<'a, S> {
    view: super::cluster_owned_mob_world::MobWorldView<'a, S>,
}

impl<'a, S: ActionState> VehicleWorldView<'a, S> {
    pub(super) fn with_rules(
        mut self,
        rules: super::cluster_owned_mob_world::MobWorldRules,
    ) -> Self {
        self.view = self.view.with_rules(rules);
        self
    }
    pub(super) fn new(
        source: &'a S,
        draft: &'a ActorActionDraft,
        spatial: &EntitySpatial,
        loaded: &'a BTreeSet<ChunkKey>,
    ) -> Self {
        Self {
            view: super::cluster_owned_mob_world::MobWorldView::new(
                source,
                draft,
                spatial.world,
                loaded,
            ),
        }
    }
}

pub(super) fn bounds(spatial: &EntitySpatial, kind: VehicleKind) -> BoundingBox {
    let position = spatial.position.map(f64::from_bits);
    let boat = matches!(kind, VehicleKind::Boat | VehicleKind::ChestBoat);
    let radius = if boat { 0.6875 } else { 0.49 };
    let height = if boat { 0.5625 } else { 0.7 };
    BoundingBox::new_array(
        [position[0] - radius, position[1], position[2] - radius],
        [
            position[0] + radius,
            position[1] + height,
            position[2] + radius,
        ],
    )
}

impl<S: ActionState> VehicleCollisionView for VehicleWorldView<'_, S> {
    fn environment(
        &self,
        spatial: &EntitySpatial,
        kind: VehicleKind,
    ) -> Option<VehicleEnvironment> {
        let box_ = bounds(spatial, kind);
        let contacts = self.view.fluid_contacts(box_, 128).ok()?;
        let water = contacts.contacts[0];
        let position = spatial.position.map(f64::from_bits);
        let below = self
            .view
            .block(BlockPos {
                x: position[0].floor() as i32,
                y: (position[1] - 0.001).floor() as i32,
                z: position[2].floor() as i32,
            })
            .ok()?;
        let at = self
            .view
            .block(BlockPos {
                x: position[0].floor() as i32,
                y: position[1].floor() as i32,
                z: position[2].floor() as i32,
            })
            .ok()?;
        let rail = if is_rail(at) {
            Some((at, position[1].floor()))
        } else if is_rail(below) {
            Some((below, (position[1] - 0.001).floor()))
        } else {
            None
        };
        let (rail_direction, rail_position, powered_rail, braking_rail) = rail
            .map(|(state, y)| rail_environment(state, position, y))
            .unwrap_or((None, None, false, false));
        let collision = self
            .view
            .move_bounds(box_, Vector3::new(0.0, -0.001, 0.0), 256)
            .ok()?;
        Some(VehicleEnvironment {
            water_surface: water.touching.then_some(box_.min.y + water.height),
            submerged: water.touching && water.height >= box_.max.y - box_.min.y,
            ground: collision.movement.y > -0.001,
            friction: f64::from(pumpkin_data::Block::from_state_id(below).slipperiness),
            rail_direction,
            rail_position,
            powered_rail,
            braking_rail,
        })
    }

    fn clip(
        &self,
        spatial: &EntitySpatial,
        displacement: [f64; 3],
        kind: VehicleKind,
    ) -> Option<([f64; 3], bool)> {
        let movement = self
            .view
            .move_bounds(
                bounds(spatial, kind),
                Vector3::new(displacement[0], displacement[1], displacement[2]),
                512,
            )
            .ok()?;
        Some((
            [
                movement.movement.x,
                movement.movement.y,
                movement.movement.z,
            ],
            displacement[1] < 0.0 && movement.movement.y != displacement[1],
        ))
    }
}

fn is_rail(state: pumpkin_data::BlockStateId) -> bool {
    matches!(
        pumpkin_data::Block::from_state_id(state).name,
        "rail" | "powered_rail" | "detector_rail" | "activator_rail"
    )
}

fn rail_environment(
    state: pumpkin_data::BlockStateId,
    position: [f64; 3],
    rail_y: f64,
) -> (Option<[f64; 3]>, Option<[f64; 3]>, bool, bool) {
    use pumpkin_data::block_properties::{
        PoweredRailLikeProperties, RailLikeProperties, RailShape, RailShapeStraight,
    };
    let kind = pumpkin_data::Block::from_state_id(state).name;
    let (shape, powered) = if kind == "rail" {
        (RailLikeProperties::from_state_id(state).r#shape, false)
    } else {
        let properties = PoweredRailLikeProperties::from_state_id(state);
        (
            match properties.r#shape {
                RailShapeStraight::NorthSouth => RailShape::NorthSouth,
                RailShapeStraight::EastWest => RailShape::EastWest,
                RailShapeStraight::AscendingEast => RailShape::AscendingEast,
                RailShapeStraight::AscendingWest => RailShape::AscendingWest,
                RailShapeStraight::AscendingNorth => RailShape::AscendingNorth,
                RailShapeStraight::AscendingSouth => RailShape::AscendingSouth,
            },
            properties.r#powered,
        )
    };
    let (first, second): ([f64; 3], [f64; 3]) = match shape {
        RailShape::NorthSouth => ([0.0, 0.0, -0.5], [0.0, 0.0, 0.5]),
        RailShape::EastWest => ([-0.5, 0.0, 0.0], [0.5, 0.0, 0.0]),
        RailShape::AscendingEast => ([-0.5, 0.0, 0.0], [0.5, 1.0, 0.0]),
        RailShape::AscendingWest => ([-0.5, 1.0, 0.0], [0.5, 0.0, 0.0]),
        RailShape::AscendingNorth => ([0.0, 1.0, -0.5], [0.0, 0.0, 0.5]),
        RailShape::AscendingSouth => ([0.0, 0.0, -0.5], [0.0, 1.0, 0.5]),
        RailShape::SouthEast => ([0.0, 0.0, 0.5], [0.5, 0.0, 0.0]),
        RailShape::SouthWest => ([0.0, 0.0, 0.5], [-0.5, 0.0, 0.0]),
        RailShape::NorthWest => ([0.0, 0.0, -0.5], [-0.5, 0.0, 0.0]),
        RailShape::NorthEast => ([0.0, 0.0, -0.5], [0.5, 0.0, 0.0]),
    };
    let center = [
        position[0].floor() + 0.5,
        rail_y + 0.0625,
        position[2].floor() + 0.5,
    ];
    let direction = core::array::from_fn::<_, 3, _>(|axis| second[axis] - first[axis]);
    let parameter = ((position[0] - center[0] - first[0]) * direction[0]
        + (position[2] - center[2] - first[2]) * direction[2])
        / (direction[0] * direction[0] + direction[2] * direction[2]);
    let snapped =
        core::array::from_fn(|axis| center[axis] + first[axis] + parameter * direction[axis]);
    (
        Some(direction),
        Some(snapped),
        kind == "powered_rail" && powered,
        kind == "powered_rail" && !powered,
    )
}

pub(super) fn activator(
    state: u16,
) -> Result<Option<bool>, pumpkin_cluster::canonical_action::ApplyFailure> {
    let state = pumpkin_data::BlockStateId::new(state)
        .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
    if pumpkin_data::Block::from_state_id(state).name != "activator_rail" {
        return Ok(None);
    }
    Ok(Some(
        pumpkin_data::block_properties::PoweredRailLikeProperties::from_state_id(state).r#powered,
    ))
}

impl<S: ActionState> VehicleWorldView<'_, S> {
    pub(super) fn activation(
        &self,
        spatial: &EntitySpatial,
        kind: VehicleKind,
    ) -> Result<
        Option<pumpkin_cluster::vehicle_tick::VehicleActivation>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target};
        if !matches!(
            kind,
            VehicleKind::Minecart | VehicleKind::HopperMinecart | VehicleKind::TntMinecart
        ) {
            return Ok(None);
        }
        let position = spatial.position.map(f64::from_bits);
        for y in [
            position[1].floor() as i32,
            (position[1] - 0.001).floor() as i32,
        ] {
            let at = BlockPos {
                x: position[0].floor() as i32,
                y,
                z: position[2].floor() as i32,
            };
            let state = self.view.block(at).map_err(|_| ApplyFailure::Capacity)?;
            if activator(state.as_u16())?.is_some() {
                return Ok(Some(pumpkin_cluster::vehicle_tick::VehicleActivation {
                    detach: None,
                    block: Target::Chunk {
                        key: ChunkKey::new(spatial.world, at.x.div_euclid(16), at.z.div_euclid(16)),
                        part: ChunkPart::Block(at),
                        inventory: None,
                    },
                    kind,
                    position: spatial.position,
                }));
            }
        }
        Ok(None)
    }
}
