use std::{collections::BTreeMap, sync::Arc};

use pumpkin_cluster::{
    actor_occupancy::{ActorOccupancyWitness, CollisionBox},
    canonical_action::*,
    canonical_inventory::InventorySlotBinding,
    entity_body::{EntityBody, EntityPayload, EntitySpatial},
    entity_inventory_admission::EntityInventoryAdmission,
    entity_origin::{EntityOriginAllocator, EntityOriginIdentity},
    entity_placement::{EntityPlacementGeometry, EntityPlacementIntent},
    inventory::{INV_MAIN, InvLoc, InventoryCell},
    item_action::ItemActionKind,
    item_contact::ContactObservation,
    mob_breeding::MobBirthAdmission,
    object_state::{FrameState, ObjectKind, ObjectState, PaintingState},
    protocol::{BlockPos, ChunkKey},
    realm_completion::PeerGeneration,
    time::TickStamp,
};
use pumpkin_data::{Block, BlockDirection, BlockStateId, entity::EntityType, item::Item};

#[derive(Clone)]
enum BirthSpec {
    Stand(EntitySpatial),
    Object(EntityType, EntitySpatial, ObjectState),
    Vehicle(EntityType, EntitySpatial),
}

impl BirthSpec {
    fn spatial(&self) -> &EntitySpatial {
        match self {
            Self::Stand(spatial) | Self::Object(_, spatial, _) | Self::Vehicle(_, spatial) => {
                spatial
            }
        }
    }
    fn bounds(&self) -> CollisionBox {
        let at = self.spatial().position.map(f64::from_bits);
        let half = match self {
            Self::Stand(_) => [0.25, 1.975, 0.25],
            Self::Vehicle(kind, _) => [
                f64::from(kind.width()) / 2.0,
                f64::from(kind.height()),
                f64::from(kind.width()) / 2.0,
            ],
            Self::Object(_, _, object) => {
                let (facing, width, height) = match &object.kind {
                    ObjectKind::ItemFrame(frame) => (frame.facing, 0.75, 0.75),
                    ObjectKind::Painting(painting) => (
                        painting.facing,
                        f64::from(painting.width),
                        f64::from(painting.height),
                    ),
                    _ => unreachable!(),
                };
                let half = match facing {
                    0 | 1 => [width / 2.0, 0.03125, height / 2.0],
                    2 | 3 => [width / 2.0, height / 2.0, 0.03125],
                    _ => [0.03125, height / 2.0, width / 2.0],
                };
                return CollisionBox {
                    bounds: [
                        core::array::from_fn(|axis| (at[axis] - half[axis]).to_bits()),
                        core::array::from_fn(|axis| (at[axis] + half[axis]).to_bits()),
                    ],
                };
            }
        };
        CollisionBox {
            bounds: [
                [at[0] - half[0], at[1], at[2] - half[2]].map(f64::to_bits),
                [at[0] + half[0], at[1] + half[1], at[2] + half[2]].map(f64::to_bits),
            ],
        }
    }
    fn body(
        &self,
        identity: &EntityOriginIdentity,
        reference: ActionRef,
        owner: PeerGeneration,
    ) -> Result<EntityBody, ApplyFailure> {
        let at = self.spatial().position.map(f64::from_bits);
        let chunk = ChunkKey::new(
            self.spatial().world,
            (at[0].floor() as i32).div_euclid(16),
            (at[2].floor() as i32).div_euclid(16),
        );
        match self {
            Self::Stand(spatial) => super::cluster_object_birth::prepare_reserved_stand(
                identity,
                reference,
                owner,
                spatial.clone(),
                pumpkin_cluster::armor_stand::ArmorStandState::vanilla(),
                chunk,
            ),
            Self::Object(kind, spatial, object) => super::cluster_object_birth::prepare_reserved(
                identity,
                reference,
                owner,
                kind.clone(),
                spatial.clone(),
                object.clone(),
                chunk,
            ),
            Self::Vehicle(kind, spatial) => {
                let initial = super::cluster_vehicle_birth::parse_initialization(
                    kind,
                    &pumpkin_nbt::compound::NbtCompound::new(),
                )?;
                Ok(super::cluster_vehicle_birth::prepare_reserved_body(
                    identity,
                    reference,
                    owner,
                    kind,
                    spatial.clone(),
                    &initial,
                )?
                .0)
            }
        }
    }
}

pub(super) struct FrozenEntityPlacement {
    rules: pumpkin_cluster::world_rules::WorldRulesWitness,
    source: ContactObservation,
    slot: InventorySlotBinding,
    equipment: pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding,
    cell: Cell,
    selected: Condition,
    eye_height: u64,
    yaw: u32,
    pitch: u32,
    reach: u64,
    geometry: EntityPlacementGeometry,
    spec: BirthSpec,
    blocks: BTreeMap<Target, Cell>,
    occupancy: Vec<ActorOccupancyWitness>,
    bound: Option<Box<EntityPlacementIntent>>,
}

pub(super) fn supports(proposal: &ActionProposal) -> bool {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return false;
    };
    let item = claim.intent.expected_item.item;
    match claim.intent.kind {
        ItemActionKind::UseOnBlock { .. } => {
            object_item(item) || super::cluster_vehicle_birth::item_kind(item).is_some()
        }
        ItemActionKind::Use { .. } => {
            super::cluster_vehicle_birth::item_kind(item).is_some_and(|kind| {
                matches!(
                    super::cluster_vehicle_birth::vehicle_kind(kind),
                    Some(
                        pumpkin_cluster::entity_body::VehicleKind::Boat
                            | pumpkin_cluster::entity_body::VehicleKind::ChestBoat
                    )
                )
            })
        }
        _ => false,
    }
}

fn object_item(item: u16) -> bool {
    [
        Item::ARMOR_STAND.id,
        Item::ITEM_FRAME.id,
        Item::GLOW_ITEM_FRAME.id,
        Item::PAINTING.id,
    ]
    .contains(&item)
}

fn reach_attribute() -> u16 {
    u16::from(pumpkin_data::attributes::Attributes::BLOCK_INTERACTION_RANGE.id)
}

fn spatial(world: pumpkin_cluster::protocol::WorldId, at: [f64; 3], yaw: f32) -> EntitySpatial {
    EntitySpatial {
        world,
        position: at.map(f64::to_bits),
        velocity_micro: [0; 3],
        yaw: yaw.to_bits(),
        pitch: 0.0_f32.to_bits(),
        on_ground: false,
    }
}

fn direction(face: u8) -> Result<BlockDirection, ApplyFailure> {
    BlockDirection::from_index(face).ok_or(ApplyFailure::InvalidAction)
}

fn block_target(world: pumpkin_cluster::protocol::WorldId, position: BlockPos) -> Target {
    Target::Chunk {
        key: ChunkKey::new(world, position.x.div_euclid(16), position.z.div_euclid(16)),
        part: ChunkPart::Block(position),
        inventory: None,
    }
}

fn block<S: ActionState>(
    view: &S,
    world: pumpkin_cluster::protocol::WorldId,
    position: BlockPos,
    facts: &mut BTreeMap<Target, Cell>,
) -> Result<BlockStateId, ApplyFailure> {
    let target = block_target(world, position);
    let cell = view
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?;
    let Value::Block(id) = cell.value else {
        return Err(ApplyFailure::Type(target));
    };
    facts.insert(target, cell);
    BlockStateId::new(id).ok_or(ApplyFailure::Type(target))
}

fn spec(
    item: u16,
    world: pumpkin_cluster::protocol::WorldId,
    geometry: &EntityPlacementGeometry,
    yaw: f32,
    variant: Option<u16>,
    facts: &BTreeMap<Target, Cell>,
) -> Result<BirthSpec, ApplyFailure> {
    let (clicked, face) = match *geometry {
        EntityPlacementGeometry::Clicked { block, face }
        | EntityPlacementGeometry::BoatRay { block, face, .. } => (block, face),
    };
    let facing = direction(face)?;
    let offset = facing.to_offset();
    let target = BlockPos {
        x: clicked.x + offset.x,
        y: clicked.y + offset.y,
        z: clicked.z + offset.z,
    };
    if let Some(kind) = super::cluster_vehicle_birth::item_kind(item) {
        if matches!(
            super::cluster_vehicle_birth::vehicle_kind(kind),
            Some(
                pumpkin_cluster::entity_body::VehicleKind::Boat
                    | pumpkin_cluster::entity_body::VehicleKind::ChestBoat
            )
        ) {
            return Ok(BirthSpec::Vehicle(
                kind.clone(),
                spatial(
                    world,
                    [
                        f64::from(clicked.x) + 0.5,
                        f64::from(clicked.y) + 1.0,
                        f64::from(clicked.z) + 0.5,
                    ],
                    yaw,
                ),
            ));
        }
        let Some(Cell {
            value: Value::Block(state),
            ..
        }) = facts.get(&block_target(world, clicked))
        else {
            return Err(ApplyFailure::Condition(block_target(world, clicked)));
        };
        let state = BlockStateId::new(*state).ok_or(ApplyFailure::InvalidAction)?;
        let at = super::cluster_vehicle_birth::cart_placement(state, clicked, world)
            .ok_or(ApplyFailure::InvalidAction)?;
        return Ok(BirthSpec::Vehicle(kind.clone(), at));
    }
    if item == Item::ARMOR_STAND.id {
        if face == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let wrapped = (yaw - 180.0 + 180.0).rem_euclid(360.0) - 180.0;
        let rotation = ((wrapped + 22.5) / 45.0).floor() * 45.0;
        return Ok(BirthSpec::Stand(spatial(
            world,
            [
                f64::from(target.x) + 0.5,
                f64::from(target.y),
                f64::from(target.z) + 0.5,
            ],
            rotation,
        )));
    }
    let mut at = [
        f64::from(target.x) + 0.5,
        f64::from(target.y) + 0.5,
        f64::from(target.z) + 0.5,
    ];
    at[0] -= f64::from(offset.x) * 0.46875;
    at[1] -= f64::from(offset.y) * 0.46875;
    at[2] -= f64::from(offset.z) * 0.46875;
    let (kind, object) = if item == Item::PAINTING.id {
        if face < 2 {
            return Err(ApplyFailure::InvalidAction);
        }
        let variant = variant.ok_or(ApplyFailure::InvalidAction)?;
        let (_, width, height) = super::cluster_object_metadata::painting_variant(variant)
            .ok_or(ApplyFailure::InvalidAction)?;
        if width % 2 == 0 {
            at[0] -= f64::from(offset.z) * 0.5;
            at[2] += f64::from(offset.x) * 0.5;
        }
        if height % 2 == 0 {
            at[1] += 0.5;
        }
        (
            EntityType::PAINTING,
            ObjectKind::Painting(PaintingState {
                attachment: target,
                facing: face,
                variant,
                width,
                height,
            }),
        )
    } else if item == Item::ITEM_FRAME.id || item == Item::GLOW_ITEM_FRAME.id {
        let glow = item == Item::GLOW_ITEM_FRAME.id;
        (
            if glow {
                EntityType::GLOW_ITEM_FRAME
            } else {
                EntityType::ITEM_FRAME
            },
            ObjectKind::ItemFrame(FrameState {
                attachment: target,
                facing: face,
                glow,
                item: InventoryCell::empty(InvLoc::new(INV_MAIN, 0)),
                rotation: 0,
                drop_chance: 1.0_f32.to_bits(),
                invisible: false,
                fixed: false,
            }),
        )
    } else {
        return Err(ApplyFailure::InvalidAction);
    };
    Ok(BirthSpec::Object(
        kind,
        spatial(world, at, 0.0),
        ObjectState {
            simulation_tick: None,
            property_versions: Default::default(),
            kind: object,
        },
    ))
}

fn cells_in(bounds: &CollisionBox) -> impl Iterator<Item = BlockPos> {
    let min = bounds.bounds[0].map(f64::from_bits);
    let max = bounds.bounds[1].map(f64::from_bits);
    let a = min.map(|value| value.floor() as i32);
    let b = max.map(|value| (value - 1e-7).floor() as i32);
    (a[0]..=b[0]).flat_map(move |x| {
        (a[1]..=b[1]).flat_map(move |y| (a[2]..=b[2]).map(move |z| BlockPos { x, y, z }))
    })
}

fn clear_blocks<S: ActionState>(
    view: &S,
    spec: &BirthSpec,
    geometry: &EntityPlacementGeometry,
    facts: &mut BTreeMap<Target, Cell>,
    rules: &pumpkin_cluster::world_rules::WorldRulesSnapshot,
) -> Result<bool, ApplyFailure> {
    let bounds = spec.bounds();
    let min = bounds.bounds[0].map(f64::from_bits);
    let max = bounds.bounds[1].map(f64::from_bits);
    let mut clear = true;
    if min[1] < f64::from(rules.min_y) || max[1] > f64::from(rules.min_y) + f64::from(rules.height)
    {
        return Ok(false);
    }
    for position in cells_in(&bounds) {
        let state = block(view, spec.spatial().world, position, facts)?;
        let offset =
            state
                .to_block()
                .shape_offset_delta(&pumpkin_util::math::position::BlockPos::new(
                    position.x, position.y, position.z,
                ));
        for index in state.to_state().collision_shapes.iter() {
            let shape =
                pumpkin_data::block_properties::COLLISION_SHAPES[*index as usize].shift(offset);
            let origin = [
                f64::from(position.x),
                f64::from(position.y),
                f64::from(position.z),
            ];
            let lo = [shape.min.x, shape.min.y, shape.min.z];
            let hi = [shape.max.x, shape.max.y, shape.max.z];
            if (0..3).all(|axis| {
                hi[axis] + origin[axis] > min[axis] && lo[axis] + origin[axis] < max[axis]
            }) {
                clear = false;
            }
        }
    }
    if let BirthSpec::Object(_, _, object) = spec {
        let face = match object.kind {
            ObjectKind::ItemFrame(ref frame) => frame.facing,
            ObjectKind::Painting(ref painting) => painting.facing,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let offset = direction(face)?.to_offset();
        let support = CollisionBox {
            bounds: [
                core::array::from_fn(|axis| {
                    (min[axis] - f64::from([offset.x, offset.y, offset.z][axis]) * 0.5).to_bits()
                }),
                core::array::from_fn(|axis| {
                    (max[axis] - f64::from([offset.x, offset.y, offset.z][axis]) * 0.5).to_bits()
                }),
            ],
        };
        for position in cells_in(&support) {
            let state = block(view, spec.spatial().world, position, facts)?;
            if !state.to_state().is_solid()
                && ![&Block::REPEATER, &Block::COMPARATOR].contains(&state.to_block())
            {
                clear = false;
            }
        }
    }
    let _ = geometry;
    Ok(clear)
}

impl FrozenEntityPlacement {
    pub(super) fn capture<S: ActionState>(
        view: &S,
        original: &Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        proposal: &ActionProposal,
        metadata: &super::cluster_item_use::ItemUseSourceMetadata,
    ) -> Result<Option<Self>, ApplyFailure> {
        if !supports(proposal) {
            return Ok(None);
        }
        let ClaimedAction::ItemUse(claim) = &proposal.claim else {
            return Err(ApplyFailure::InvalidAction);
        };
        let (_, slot, cell, selected, _) =
            super::cluster_item_use::normalize_captured_source(view, proposal, metadata)?;
        let Value::Inventory(item) = &cell.value else {
            return Err(ApplyFailure::InvalidAction);
        };
        if !object_item(item.item) && super::cluster_vehicle_birth::item_kind(item.item).is_none() {
            return Err(ApplyFailure::InvalidAction);
        }
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if stack
            .get_data_component::<pumpkin_data::data_component_impl::EntityDataImpl>()
            .is_some()
            || stack
                .get_data_component::<pumpkin_data::data_component_impl::CustomNameImpl>()
                .is_some()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let source = metadata
            .source_observation
            .clone()
            .ok_or(ApplyFailure::InvalidAction)?;
        let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = source.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        let equipment = pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
            lease,
            source.chunk,
            view.inventory_equipment_attribute_ids(),
        );
        let reach_target = Target::Chunk {
            key: source.chunk,
            part: ChunkPart::LivingProperty {
                actor: source.actor(),
                property: pumpkin_cluster::living_property::LivingProperty::Attribute(
                    reach_attribute(),
                ),
            },
            inventory: None,
        };
        let Some(Cell {
            value:
                Value::LivingProperty(
                    pumpkin_cluster::living_property::LivingPropertyValue::Attribute {
                        base: Some(base),
                        modifiers,
                        ..
                    },
                ),
            ..
        }) = view.read(TruthView::Local, reach_target)
        else {
            return Err(ApplyFailure::Condition(reach_target));
        };
        let reach = pumpkin_cluster::combat_action::CombatEquipmentFacts {
            modifiers,
            ..Default::default()
        }
        .attribute(reach_attribute(), f64::from_bits(base));
        if !reach.is_finite() || !(0.0..=64.0).contains(&reach) {
            return Err(ApplyFailure::InvalidAction);
        }
        let yaw = claim.effective_frame.effective.yaw;
        let pitch = claim.effective_frame.effective.pitch;
        let (rules, policy) = original
            .world_rules_at(source.world, proposal.tick)
            .ok_or(ApplyFailure::InvalidAction)?;
        let mut facts = BTreeMap::new();
        let geometry = match claim.intent.kind {
            ItemActionKind::UseOnBlock {
                world,
                block: clicked,
                hit,
                expected_block_state,
            } => {
                if world != source.world || hit.face > 5 {
                    return Err(ApplyFailure::InvalidAction);
                }
                if block(original.as_ref(), world, clicked, &mut facts)?.as_u16()
                    != expected_block_state
                {
                    return Err(ApplyFailure::Condition(block_target(world, clicked)));
                }
                EntityPlacementGeometry::Clicked {
                    block: clicked,
                    face: hit.face,
                }
            }
            ItemActionKind::Use { .. } => {
                let (start, end) = super::cluster_item_raycast::segment(
                    source.position.map(f64::from_bits),
                    claim.eye_height,
                    yaw,
                    pitch,
                    reach,
                )?;
                let ray = super::cluster_item_raycast::capture_with_rules(
                    original.as_ref(),
                    source.world,
                    start,
                    end,
                    super::cluster_item_raycast::FluidRayPolicy::Any,
                    policy,
                )?;
                facts.extend(ray.facts);
                let hit = ray.hit.ok_or(ApplyFailure::InvalidAction)?;
                EntityPlacementGeometry::BoatRay {
                    start: start.map(f64::to_bits),
                    end: end.map(f64::to_bits),
                    block: hit.position,
                    face: hit.face.to_index(),
                }
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let variants = if item.item == Item::PAINTING.id {
            painting_variants(&stack)?
        } else {
            vec![None]
        };
        let mut candidates = Vec::new();
        for variant in variants {
            let candidate = spec(item.item, source.world, &geometry, yaw, variant, &facts)?;
            let clear = clear_blocks(original.as_ref(), &candidate, &geometry, &mut facts, policy)?;
            if !clear {
                continue;
            }
            let bounds = candidate.bounds();
            let keys = occupancy_keys(source.world, &bounds)?;
            let witnesses = keys
                .into_iter()
                .map(|key| original.capture_actor_occupancy(key, vec![bounds.clone()]))
                .collect::<Result<Vec<_>, _>>()?;
            let hanging = matches!(candidate, BirthSpec::Object(..));
            let stand = matches!(candidate, BirthSpec::Stand(_));
            let clear = clear
                && witnesses.iter().all(|witness| {
                    original
                        .read(
                            TruthView::Local,
                            Target::Chunk {
                                key: witness.key,
                                part: ChunkPart::ActorOccupancy,
                                inventory: None,
                            },
                        )
                        .is_some_and(|cell| {
                            if hanging {
                                witness.matches_hanging_clear(&cell)
                            } else if stand {
                                witness.matches_entities_clear(&cell)
                            } else {
                                witness.matches_clear(&cell)
                            }
                        })
                });
            if clear {
                let area = match &candidate {
                    BirthSpec::Object(
                        _,
                        _,
                        ObjectState {
                            kind: ObjectKind::Painting(painting),
                            ..
                        },
                    ) => u32::from(painting.width) * u32::from(painting.height),
                    _ => 1,
                };
                candidates.push((area, candidate, witnesses));
            }
        }
        let max = candidates
            .iter()
            .map(|candidate| candidate.0)
            .max()
            .ok_or(ApplyFailure::InvalidAction)?;
        candidates.retain(|candidate| candidate.0 == max);
        let index = proposal.id.sequence as usize % candidates.len();
        let (_, spec, occupancy) = candidates.swap_remove(index);
        Ok(Some(Self {
            rules,
            equipment,
            source,
            slot,
            cell,
            selected,
            eye_height: claim.eye_height.to_bits(),
            yaw: yaw.to_bits(),
            pitch: pitch.to_bits(),
            reach: reach.to_bits(),
            geometry,
            spec,
            blocks: facts,
            occupancy,
            bound: None,
        }))
    }

    pub(super) fn targets(&self) -> Vec<Target> {
        let mut targets = vec![self.source.target(), self.slot.target, self.selected.target];
        targets.extend(self.blocks.keys().copied());
        targets.extend(self.equipment.targets());
        targets.extend(self.occupancy.iter().map(|witness| Target::Chunk {
            key: witness.key,
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        }));
        for property in [
            pumpkin_cluster::living_property::LivingProperty::Health,
            pumpkin_cluster::living_property::LivingProperty::GameMode,
            pumpkin_cluster::living_property::LivingProperty::Attribute(reach_attribute()),
        ] {
            targets.push(Target::Chunk {
                key: self.source.chunk,
                part: ChunkPart::LivingProperty {
                    actor: self.source.actor(),
                    property,
                },
                inventory: None,
            });
        }
        if let Some(bound) = &self.bound {
            targets.extend(bound.guards().iter().map(|guard| guard.target));
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    pub(super) fn bind(
        &mut self,
        id: ActionId,
        tick: TickStamp,
        allocator: &mut EntityOriginAllocator,
    ) -> Result<(), ApplyFailure> {
        let reference = ActionRef { id, tick };
        let identities = allocator
            .reserve(self.source.owner, reference, 1)
            .map_err(|_| ApplyFailure::Capacity)?;
        let body = self
            .spec
            .body(&identities[0], reference, self.source.owner)?;
        let inventory = empty_inventory(&body)?;
        self.bound = Some(Box::new(EntityPlacementIntent {
            rules: self.rules,
            source: self.source.clone(),
            source_slot: self.slot.clone(),
            equipment: self.equipment.clone(),
            source_cell: self.cell.clone(),
            selected: (
                self.selected.target,
                match &self.selected.expected {
                    Expected::Exact(cell) => cell.clone(),
                    _ => return Err(ApplyFailure::InvalidAction),
                },
            ),
            eye_height: self.eye_height,
            yaw: self.yaw,
            pitch: self.pitch,
            reach: self.reach,
            reach_attribute: reach_attribute(),
            geometry: self.geometry.clone(),
            body,
            inventory,
            collision: self.spec.bounds(),
            blocks: self
                .blocks
                .iter()
                .map(|(target, cell)| (*target, cell.clone()))
                .collect(),
            occupancy: self.occupancy.clone(),
        }));
        Ok(())
    }

    pub(super) fn admission(&self) -> Result<MobBirthAdmission, ApplyFailure> {
        let bound = self.bound.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        Ok(MobBirthAdmission {
            chunk: bound.spawn_chunk(),
            body: bound.body.clone(),
            inventory: bound.inventory.clone(),
        })
    }
    pub(super) fn materialize<S: ActionState>(
        &self,
        view: &S,
        id: ActionId,
        tick: TickStamp,
        origin: ClientOrigin,
    ) -> Result<Action, ApplyFailure> {
        self.bound
            .as_ref()
            .ok_or(ApplyFailure::InvalidAction)?
            .capture(view, id, tick, origin)
    }
}

fn occupancy_keys(
    world: pumpkin_cluster::protocol::WorldId,
    bounds: &CollisionBox,
) -> Result<std::collections::BTreeSet<ChunkKey>, ApplyFailure> {
    if !bounds.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let min = bounds.bounds[0].map(f64::from_bits);
    let max = bounds.bounds[1].map(f64::from_bits);
    let a = [
        (min[0].floor() as i32).div_euclid(16) - 64,
        (min[2].floor() as i32).div_euclid(16) - 64,
    ];
    let b = [
        (max[0].floor() as i32).div_euclid(16) + 64,
        (max[2].floor() as i32).div_euclid(16) + 64,
    ];
    if b[0] - a[0] > 7 || b[1] - a[1] > 7 {
        return Err(ApplyFailure::ResourceLimit);
    }
    Ok((a[0]..=b[0])
        .flat_map(|x| (a[1]..=b[1]).map(move |z| ChunkKey::new(world, x, z)))
        .collect())
}

fn empty_inventory(body: &EntityBody) -> Result<EntityInventoryAdmission, ApplyFailure> {
    Ok(EntityInventoryAdmission {
        imported_revision: 0,
        cells: super::cluster_owned_mob_birth::inventory_layout(body)?
            .into_iter()
            .map(|loc| Cell {
                revision: 0,
                value: Value::Inventory(InventoryCell::empty(loc)),
            })
            .collect(),
    })
}

struct FrozenCells<'a>(&'a BTreeMap<Target, Cell>);
impl ActionState for FrozenCells<'_> {
    fn read(&self, _: TruthView, target: Target) -> Option<Cell> {
        self.0.get(&target).cloned()
    }
    fn commit(
        &mut self,
        _: ActionContext,
        _: TruthView,
        _: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}

pub(super) fn factory_matches(
    intent: &EntityPlacementIntent,
    rules: &pumpkin_cluster::world_rules::WorldRulesSnapshot,
) -> bool {
    fn validate(
        intent: &EntityPlacementIntent,
        rules: &pumpkin_cluster::world_rules::WorldRulesSnapshot,
    ) -> Result<bool, ApplyFailure> {
        let Value::Inventory(item) = &intent.source_cell.value else {
            return Ok(false);
        };
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if stack
            .get_data_component::<pumpkin_data::data_component_impl::EntityDataImpl>()
            .is_some()
            || stack
                .get_data_component::<pumpkin_data::data_component_impl::CustomNameImpl>()
                .is_some()
        {
            return Ok(false);
        }
        let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = intent.source.identity
        else {
            return Ok(false);
        };
        if intent.equipment
            != pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                lease,
                intent.source.chunk,
                pumpkin_data::attributes::Attributes::ALL
                    .iter()
                    .map(|attribute| u16::from(attribute.id))
                    .collect(),
            )
        {
            return Ok(false);
        }
        if intent.reach_attribute != reach_attribute() {
            return Ok(false);
        }
        let facts = intent.blocks.iter().cloned().collect::<BTreeMap<_, _>>();
        if let EntityPlacementGeometry::BoatRay {
            start,
            end,
            block,
            face,
        } = intent.geometry
        {
            let (eye, expected) = super::cluster_item_raycast::segment(
                intent.source.position.map(f64::from_bits),
                f64::from_bits(intent.eye_height),
                f32::from_bits(intent.yaw),
                f32::from_bits(intent.pitch),
                f64::from_bits(intent.reach),
            )?;
            if start != eye.map(f64::to_bits) || end != expected.map(f64::to_bits) {
                return Ok(false);
            }
            let ray = super::cluster_item_raycast::capture_with_rules(
                &FrozenCells(&facts),
                intent.source.world,
                eye,
                expected,
                super::cluster_item_raycast::FluidRayPolicy::Any,
                rules,
            )?;
            if !ray
                .hit
                .is_some_and(|hit| hit.position == block && hit.face.to_index() == face)
            {
                return Ok(false);
            }
        }
        let variant = match &intent.body.payload {
            EntityPayload::Object(ObjectState {
                kind: ObjectKind::Painting(painting),
                ..
            }) => Some(painting.variant),
            _ => None,
        };
        if item.item == Item::PAINTING.id && !painting_variants(&stack)?.contains(&variant) {
            return Ok(false);
        }
        let spec = spec(
            item.item,
            intent.source.world,
            &intent.geometry,
            f32::from_bits(intent.yaw),
            variant,
            &facts,
        )?;
        let identity = EntityOriginIdentity {
            id: intent.body.id,
            uuid: intent.body.birth.uuid,
            source: intent.body.birth.source,
        };
        let body = spec.body(&identity, intent.body.birth.action, intent.body.owner)?;
        if body != intent.body
            || spec.bounds() != intent.collision
            || empty_inventory(&body)? != intent.inventory
        {
            return Ok(false);
        }
        let mut captured = BTreeMap::new();
        if !clear_blocks(
            &FrozenCells(&facts),
            &spec,
            &intent.geometry,
            &mut captured,
            rules,
        )? {
            return Ok(false);
        }
        Ok(captured
            .iter()
            .all(|(target, cell)| facts.get(target) == Some(cell)))
    }
    validate(intent, rules).unwrap_or(false)
}

fn painting_variants(
    stack: &pumpkin_data::item_stack::ItemStack,
) -> Result<Vec<Option<u16>>, ApplyFailure> {
    let registry = pumpkin_data::registry::REGISTRY_V_26_2
        .iter()
        .find(|registry| registry.registry_id == "painting_variant")
        .ok_or(ApplyFailure::InvalidAction)?;
    if let Some(component) =
        stack.get_data_component::<pumpkin_data::data_component_impl::PaintingVariantImpl>()
    {
        let id = registry
            .entries
            .iter()
            .position(|entry| {
                entry.name.strip_prefix("minecraft:").unwrap_or(entry.name)
                    == component
                        .value
                        .strip_prefix("minecraft:")
                        .unwrap_or(&component.value)
            })
            .ok_or(ApplyFailure::InvalidAction)?;
        return Ok(vec![Some(
            u16::try_from(id).map_err(|_| ApplyFailure::InvalidAction)?,
        )]);
    }
    let ids = pumpkin_data::tag::get_tag_ids(
        pumpkin_data::tag::RegistryKey::PaintingVariant,
        "minecraft:placeable",
    )
    .ok_or(ApplyFailure::InvalidAction)?;
    if ids.is_empty()
        || ids
            .iter()
            .any(|id| usize::from(*id) >= registry.entries.len())
    {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(ids.iter().copied().map(Some).collect())
}

impl FrozenEntityPlacement {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        let (kind, spatial, object) = match &self.spec {
            BirthSpec::Stand(spatial) => (EntityType::ARMOR_STAND.id, spatial, None),
            BirthSpec::Object(kind, spatial, object) => (kind.id, spatial, Some(object)),
            BirthSpec::Vehicle(kind, spatial) => (kind.id, spatial, None),
        };
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(
            (
                &self.rules,
                &self.source,
                &self.slot,
                &self.cell,
                &self.selected,
                &self.equipment,
                self.eye_height,
                self.yaw,
                self.pitch,
                self.reach,
            ),
            (&self.geometry, kind, spatial, object),
            (&self.blocks, &self.occupancy, &self.bound),
        ))
    }
}
