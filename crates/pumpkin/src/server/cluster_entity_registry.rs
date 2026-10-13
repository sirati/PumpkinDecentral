use std::sync::Arc;

use pumpkin_cluster::{entity_body::EntityBody, protocol::WorldId};
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

use crate::{
    entity::{Entity, EntityBase},
    world::World,
};

#[derive(Clone)]
pub(super) struct PreparedEntityQuery {
    root: Arc<pumpkin_nbt::compound::NbtCompound>,
    body_source: Arc<(pumpkin_cluster::protocol::ChunkKey, pumpkin_cluster::canonical_action::Cell)>,
    inventory_revision: Option<(u64, u64)>,
}

impl PreparedEntityQuery {
    pub(super) fn prepare(body: &EntityBody,
        body_source: Arc<(pumpkin_cluster::protocol::ChunkKey, pumpkin_cluster::canonical_action::Cell)>,
        inventory_revision: Option<(u64, u64)>,
        cells: Option<&[pumpkin_cluster::inventory::InventoryCell]>) -> Option<Self> {
        if matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Living(_)) {
            let equipment = cells?.iter().filter(|cell| cell.loc.inv == pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT)
                .map(|cell| cell.loc.slot).collect::<std::collections::BTreeSet<_>>();
            if equipment != (0..8).collect() { return None; }
        }
        let mut root = pumpkin_nbt::compound::NbtCompound::new();
        write_query_nbt(body, &mut root, cells).ok()?;
        Some(Self { root: Arc::new(root), body_source, inventory_revision })
    }
}


pub(crate) struct CanonicalEntityShell {
    entity: Entity,
    pub(crate) body: EntityBody,
    query: PreparedEntityQuery,
}

impl CanonicalEntityShell {
    pub(super) fn cached_query(&self, source: &Arc<(pumpkin_cluster::protocol::ChunkKey, pumpkin_cluster::canonical_action::Cell)>, revision: Option<(u64, u64)>) -> Option<PreparedEntityQuery> {
        (Arc::ptr_eq(source, &self.query.body_source) && revision == self.query.inventory_revision).then(|| self.query.clone())
    }
    pub(super) fn prepare(
        world: Arc<World>,
        body: EntityBody,
        client_id: i32,
        bounds: BoundingBox,
        biome: &'static pumpkin_data::biome::Biome,
        query: PreparedEntityQuery,
    ) -> Option<Arc<dyn EntityBase>> {
        let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)?;
        let position = body.spatial.position.map(f64::from_bits);
        if position.iter().any(|coordinate| !coordinate.is_finite()) {
            return None;
        }
        let mut entity = Entity::from_uuid_with_id_in_biome(
            client_id,
            body.birth.uuid,
            world,
            Vector3::new(position[0], position[1], position[2]),
            kind,
            biome,
        );
        entity.bounding_box = crossbeam::atomic::AtomicCell::new(bounds);
        entity.yaw = crossbeam::atomic::AtomicCell::new(f32::from_bits(body.spatial.yaw));
        entity.head_yaw = crossbeam::atomic::AtomicCell::new(f32::from_bits(body.spatial.yaw));
        entity.pitch = crossbeam::atomic::AtomicCell::new(f32::from_bits(body.spatial.pitch));
        entity.velocity = crossbeam::atomic::AtomicCell::new(Vector3::new(
            body.spatial.velocity_micro[0] as f64 / 1_000_000.0,
            body.spatial.velocity_micro[1] as f64 / 1_000_000.0,
            body.spatial.velocity_micro[2] as f64 / 1_000_000.0,
        ));
        entity.on_ground = std::sync::atomic::AtomicBool::new(body.spatial.on_ground);
        entity.no_physics = std::sync::atomic::AtomicBool::new(true);
        Some(Arc::new(Self { entity, body, query }))
    }
}

impl EntityBase for CanonicalEntityShell {
    fn write_nbt(&self, nbt: &mut pumpkin_nbt::compound::NbtCompound) {
        for (key, value) in &self.query.root.child_tags { nbt.put(key.as_ref(), value.clone()); }
        write_query_identity_nbt(&self.body, nbt);
    }
    fn get_entity(&self) -> &Entity {
        &self.entity
    }
    fn get_living_entity(&self) -> Option<&crate::entity::living::LivingEntity> {
        None
    }
    fn tick(&self, _: &dyn EntityBase, _: &super::Server) {}
    fn cast_any(&self) -> &dyn std::any::Any {
        self
    }
    fn can_hit(&self) -> bool {
        !matches!(&self.body.payload,
            pumpkin_cluster::entity_body::EntityPayload::Object(object)
                if matches!(object.kind, pumpkin_cluster::object_state::ObjectKind::Marker { .. }
                    | pumpkin_cluster::object_state::ObjectKind::Display(_)))
    }
    fn is_pushable(&self) -> bool {
        false
    }
    fn is_pushed_by_fluids(&self) -> bool {
        false
    }
}

pub(crate) type CanonicalEntityRoot = pumpkin_cluster::structural_map::StructuralMap<
    pumpkin_cluster::protocol::GlobalEntityId,
    Arc<dyn EntityBase>,
>;
pub(crate) type CanonicalIdentityRoot =
    pumpkin_cluster::structural_map::StructuralMap<(i32, uuid::Uuid), ()>;

pub(crate) type FrameViewerKey = (pumpkin_cluster::canonical_action::LivePlayerLease, u64, u32);

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DeliveredFrame {
    pub actor: pumpkin_cluster::action_command::ActorReference,
    pub facing: u8,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DeliveredFrameView {
    pub viewer: FrameViewerKey,
    pub world: WorldId,
    pub frames: pumpkin_cluster::structural_map::StructuralMap<pumpkin_cluster::protocol::BlockPos, DeliveredFrame>,
}

#[derive(Clone, Default)]
pub(crate) struct CanonicalEntityDirectory {
    frame_views: pumpkin_cluster::structural_map::StructuralMap<FrameViewerKey, Arc<DeliveredFrameView>>,
    frames: pumpkin_cluster::structural_map::StructuralMap<(WorldId, pumpkin_cluster::protocol::BlockPos), pumpkin_cluster::structural_map::StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, ()>>,
    worlds: pumpkin_cluster::structural_map::StructuralMap<WorldId, CanonicalEntityRoot>,
    identities: pumpkin_cluster::structural_map::StructuralMap<WorldId, CanonicalIdentityRoot>,
    entities: CanonicalEntityRoot,
    births: pumpkin_cluster::structural_map::StructuralMap<
        pumpkin_cluster::canonical_action::ActionRef,
        pumpkin_cluster::structural_map::StructuralMap<
            pumpkin_cluster::protocol::GlobalEntityId,
            (),
        >,
    >,
}

impl CanonicalEntityDirectory {
    pub(crate) fn delivered_frame_view(&self, viewer: FrameViewerKey) -> Option<Arc<DeliveredFrameView>> { self.frame_views.get(&viewer).cloned() }

    pub(super) fn set_frame_view(&mut self, view: DeliveredFrameView) -> bool {
        if self.frame_views.get(&view.viewer).is_some_and(|old| old.as_ref() == &view) { return false; }
        self.frame_views.insert(view.viewer, Arc::new(view));
        true
    }

    pub(super) fn remove_frame_view(&mut self, viewer: FrameViewerKey) -> bool { self.frame_views.remove(&viewer).is_some() }

    pub(super) fn retain_frame_views(&mut self, active: &std::collections::BTreeSet<FrameViewerKey>) -> bool {
        let retired = self.frame_views.keys().filter(|key| !active.contains(key)).copied().collect::<Vec<_>>();
        for key in &retired { self.frame_views.remove(key); }
        !retired.is_empty()
    }

    pub(crate) fn frame_at(&self, world: WorldId, position: pumpkin_cluster::protocol::BlockPos) -> Option<pumpkin_cluster::protocol::GlobalEntityId> {
        self.frames.get(&(world, position))?.keys().next().copied()
    }

    pub(crate) fn entity_by_id(
        &self,
        id: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> Option<&Arc<dyn EntityBase>> {
        self.entities.get(&id)
    }

    pub(crate) fn entity(
        &self,
        world: WorldId,
        id: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> Option<Arc<dyn EntityBase>> {
        let entity = self.entities.get(&id)?;
        let shell = entity.cast_any().downcast_ref::<CanonicalEntityShell>()?;
        (shell.body.spatial.world == world).then(|| entity.clone())
    }

    pub(crate) fn birth_entities(
        &self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
    ) -> impl Iterator<Item = &Arc<dyn EntityBase>> {
        self.births
            .get(&reference)
            .into_iter()
            .flat_map(|ids| ids.keys())
            .filter_map(|id| self.entities.get(id))
    }

    pub(crate) fn world_entities(&self, world: WorldId) -> Option<CanonicalEntityRoot> {
        self.worlds.get(&world).cloned()
    }

    pub(crate) fn world_identities(&self, world: WorldId) -> Option<CanonicalIdentityRoot> {
        self.identities.get(&world).cloned()
    }

    pub(super) fn remove(&mut self, id: pumpkin_cluster::protocol::GlobalEntityId) {
        let Some(old) = self.entities.remove(&id) else {
            return;
        };
        let Some(shell) = old.cast_any().downcast_ref::<CanonicalEntityShell>() else {
            return;
        };
        let world = shell.body.spatial.world;
        if let pumpkin_cluster::entity_body::EntityPayload::Object(object) = &shell.body.payload
            && let pumpkin_cluster::object_state::ObjectKind::ItemFrame(frame) = &object.kind {
            let key = (world, frame.attachment);
            if let Some(ids) = self.frames.get_mut(&key) {
                ids.remove(&id);
                if ids.is_empty() { self.frames.remove(&key); }
            }
        }
        if let Some(entities) = self.worlds.get_mut(&world) {
            entities.remove(&id);
        }
        if let Some(identities) = self.identities.get_mut(&world) {
            identities.remove(&(old.get_entity().entity_id, old.get_entity().entity_uuid));
        }
        if let Some(ids) = self.births.get_mut(&shell.body.birth.action) {
            ids.remove(&id);
            if ids.is_empty() {
                self.births.remove(&shell.body.birth.action);
            }
        }
    }

    pub(super) fn insert(&mut self, shell: Arc<dyn EntityBase>) {
        let Some(canonical) = shell.cast_any().downcast_ref::<CanonicalEntityShell>() else {
            return;
        };
        let id = canonical.body.id;
        let world = canonical.body.spatial.world;
        let reference = canonical.body.birth.action;
        self.remove(id);
        if let pumpkin_cluster::entity_body::EntityPayload::Object(object) = &canonical.body.payload
            && let pumpkin_cluster::object_state::ObjectKind::ItemFrame(frame) = &object.kind {
            self.frames.get_or_insert_default((world, frame.attachment)).insert(id, ());
        }
        self.worlds
            .get_or_insert_default(world)
            .insert(id, shell.clone());
        self.identities.get_or_insert_default(world).insert(
            (shell.get_entity().entity_id, shell.get_entity().entity_uuid),
            (),
        );
        self.births.get_or_insert_default(reference).insert(id, ());
        self.entities.insert(id, shell);
    }
}

pub(super) fn object_is_hanging(body: &EntityBody) -> bool {
    matches!(&body.payload, pumpkin_cluster::entity_body::EntityPayload::Object(object)
        if matches!(object.kind, pumpkin_cluster::object_state::ObjectKind::ItemFrame(_)
            | pumpkin_cluster::object_state::ObjectKind::Painting(_)))
}

pub(super) fn object_blocks_building(body: &EntityBody) -> Option<bool> {
    use pumpkin_cluster::{entity_body::EntityPayload, object_state::ObjectKind};
    match &body.payload {
        EntityPayload::Object(object) => Some(matches!(
            &object.kind,
            ObjectKind::PrimedTnt(_) | ObjectKind::FallingBlock(_) | ObjectKind::EndCrystal(_)
        )),
        EntityPayload::Living(living) => living.stand.as_ref().map(|stand| stand.flags & 0x10 == 0),
        _ => None,
    }
}

pub(super) fn bounds(body: &EntityBody) -> Option<BoundingBox> {
    bounds_at(body, body.spatial.position.map(f64::from_bits))
}

pub(super) fn relative_bounds(body: &EntityBody) -> Option<BoundingBox> {
    bounds_at(body, [0.0; 3])
}

fn bounds_at(body: &EntityBody, at: [f64; 3]) -> Option<BoundingBox> {
    use pumpkin_cluster::entity_body::EntityPayload;
    match &body.payload {
        EntityPayload::Object(_) => super::cluster_object_metadata::bounds_at(body, at),
        EntityPayload::Living(living) if living.stand.is_some() => {
            super::cluster_object_metadata::bounds_at(body, at)
        }
        EntityPayload::Vehicle(vehicle) => Some(super::cluster_vehicle_world::bounds(
            &pumpkin_cluster::entity_body::EntitySpatial {
                position: at.map(f64::to_bits),
                ..body.spatial.clone()
            },
            vehicle.kind,
        )),
        EntityPayload::Living(living) => {
            super::cluster_owned_mob_nbt::write_living_nbt(body, nbt, cells)?;
            let dimensions = super::cluster_owned_mob_metadata::dimensions(living, body.kind)?;
            Some(BoundingBox::new_from_pos(at[0], at[1], at[2], &dimensions))
        }
        EntityPayload::Item(_) | EntityPayload::Experience(_) | EntityPayload::Projectile(_) => {
            let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)?;
            let radius = f64::from(kind.width()) / 2.0;
            Some(BoundingBox::new_array(
                [at[0] - radius, at[1], at[2] - radius],
                [
                    at[0] + radius,
                    at[1] + f64::from(kind.height()),
                    at[2] + radius,
                ],
            ))
        }
    }
}

fn write_query_nbt(body: &EntityBody, nbt: &mut pumpkin_nbt::compound::NbtCompound, cells: Option<&[pumpkin_cluster::inventory::InventoryCell]>) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::entity_body::EntityPayload;
    use pumpkin_cluster::object_state::ObjectKind;
    use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};
    write_query_identity_nbt(body, nbt);
    if !body.tags.is_empty() {
        nbt.put(
            "Tags",
            NbtTag::List(
                body.tags
                    .iter()
                    .map(|tag| NbtTag::String(tag.clone().into()))
                    .collect(),
            ),
        );
    }
    match &body.payload {
        EntityPayload::Living(living) => {
            nbt.put_float("Health", living.combat.health_milli as f32 / 1000.0);
            nbt.put_float(
                "AbsorptionAmount",
                living.combat.absorption_milli as f32 / 1000.0,
            );
            nbt.put_short(
                "Fire",
                living.combat.timing.fire_ticks.min(i16::MAX as u32) as i16,
            );
            if let Some(stand) = &living.stand {
                nbt.put_bool("Small", stand.flags & 1 != 0);
                nbt.put_bool("ShowArms", stand.flags & 4 != 0);
                nbt.put_bool("NoBasePlate", stand.flags & 8 != 0);
                nbt.put_bool("Marker", stand.flags & 16 != 0);
                nbt.put_int("DisabledSlots", stand.disabled_slots as i32);
                let mut pose = NbtCompound::new();
                for (name, rotation) in
                    ["Head", "Body", "LeftArm", "RightArm", "LeftLeg", "RightLeg"]
                        .into_iter()
                        .zip(stand.poses)
                {
                    pose.put(
                        name,
                        NbtTag::List(
                            rotation
                                .into_iter()
                                .map(|angle| NbtTag::Float(f32::from_bits(angle)))
                                .collect(),
                        ),
                    );
                }
                nbt.put_compound("Pose", pose);
            }
        }
        EntityPayload::Object(object) => match &object.kind {
            ObjectKind::AreaEffectCloud(cloud) => {
                nbt.put_int("Age", cloud.continuation.age as i32);
                nbt.put_int("Duration", cloud.duration as i32);
                nbt.put_int("WaitTime", cloud.wait as i32);
                nbt.put_int("ReapplicationDelay", i32::from(cloud.reapply));
                nbt.put_float("Radius", f32::from_bits(cloud.continuation.radius));
                nbt.put_float("RadiusPerTick", f32::from_bits(cloud.radius_per_tick));
                nbt.put_float("RadiusOnUse", f32::from_bits(cloud.radius_on_use));
            }
            ObjectKind::PrimedTnt(tnt) => nbt.put_short("Fuse", tnt.fuse as i16),
            ObjectKind::FallingBlock(falling) => {
                nbt.put_int("Time", falling.age.min(i32::MAX as u32) as i32);
                nbt.put_bool("DropItem", falling.drop_item);
                nbt.put_bool("HurtEntities", falling.hurt_entities);
                nbt.put_float("FallDistance", f32::from_bits(falling.fall_distance));
                nbt.put_float("FallHurtAmount", f32::from_bits(falling.damage_per_block));
                nbt.put_int(
                    "FallHurtMax",
                    falling.max_damage.min(i32::MAX as u32) as i32,
                );
            }
            ObjectKind::EndCrystal(crystal) => {
                nbt.put_bool("ShowBottom", crystal.show_bottom);
                nbt.put_bool("Invulnerable", crystal.invulnerable);
                if let Some(beam) = crystal.beam {
                    let mut target = NbtCompound::new();
                    target.put_int("X", beam.x);
                    target.put_int("Y", beam.y);
                    target.put_int("Z", beam.z);
                    nbt.put_compound("BeamTarget", target);
                }
            }
            ObjectKind::ItemFrame(frame) => {
                nbt.put_byte("ItemRotation", frame.rotation as i8);
                nbt.put_float("ItemDropChance", f32::from_bits(frame.drop_chance));
                nbt.put_bool("Fixed", frame.fixed);
                nbt.put_bool("Invisible", frame.invisible);
                nbt.put_byte("Facing", frame.facing as i8);
                nbt.put_int("TileX", frame.attachment.x);
                nbt.put_int("TileY", frame.attachment.y);
                nbt.put_int("TileZ", frame.attachment.z);
            }
            ObjectKind::Painting(painting) => {
                nbt.put_byte("facing", painting.facing as i8);
                nbt.put_int("TileX", painting.attachment.x);
                nbt.put_int("TileY", painting.attachment.y);
                nbt.put_int("TileZ", painting.attachment.z);
            }
            ObjectKind::LeashKnot { attachment } => {
                nbt.put_int("TileX", attachment.x);
                nbt.put_int("TileY", attachment.y);
                nbt.put_int("TileZ", attachment.z);
            }
            ObjectKind::Interaction(interaction) => {
                nbt.put_float("width", f32::from_bits(interaction.width));
                nbt.put_float("height", f32::from_bits(interaction.height));
                nbt.put_bool("response", interaction.response);
            }
            ObjectKind::Display(display) => {
                nbt.put_float("view_range", f32::from_bits(display.view_range));
                nbt.put_float("shadow_radius", f32::from_bits(display.shadow_radius));
                nbt.put_float("shadow_strength", f32::from_bits(display.shadow_strength));
                nbt.put_float("width", f32::from_bits(display.width));
                nbt.put_float("height", f32::from_bits(display.height));
            }
            ObjectKind::Marker { data } => {
                let mut cursor = std::io::Cursor::new(data.as_slice());
                let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
                if let Ok(document) = pumpkin_nbt::Nbt::read_unnamed(&mut reader) {
                    nbt.put_compound("data", document.root_tag);
                }
            }
        },
        EntityPayload::Item(item) => {
            nbt.put_short("Age", item.age.min(i16::MAX as u32) as i16);
            nbt.put_short("PickupDelay", item.pickup_delay.min(i16::MAX as u16) as i16);
            nbt.put_short(
                "Health",
                (item.health_milli / 1000).min(i16::MAX as u32) as i16,
            );
        }
        EntityPayload::Experience(orb) => {
            nbt.put_short("Age", orb.age.min(i16::MAX as u32) as i16);
            nbt.put_short("Value", orb.amount.min(i16::MAX as u32) as i16);
            nbt.put_int("Count", orb.count.min(i32::MAX as u32) as i32);
        }
        EntityPayload::Vehicle(_) | EntityPayload::Projectile(_) => {}
    }
    Ok(())
}

fn write_query_identity_nbt(body: &EntityBody, nbt: &mut pumpkin_nbt::compound::NbtCompound) {
    use pumpkin_nbt::tag::NbtTag;
    if let Some(kind) = pumpkin_data::entity::EntityType::from_raw(body.kind) {
        nbt.put_string("id", format!("minecraft:{}", kind.resource_name));
    }
    nbt.put_uuid("UUID", body.birth.uuid);
    nbt.put(
        "Pos",
        NbtTag::List(
            body.spatial
                .position
                .iter()
                .map(|coordinate| NbtTag::Double(f64::from_bits(*coordinate)))
                .collect(),
        ),
    );
    nbt.put(
        "Motion",
        NbtTag::List(
            body.spatial
                .velocity_micro
                .iter()
                .map(|velocity| NbtTag::Double(*velocity as f64 / 1_000_000.0))
                .collect(),
        ),
    );
    nbt.put(
        "Rotation",
        NbtTag::List(vec![
            NbtTag::Float(f32::from_bits(body.spatial.yaw)),
            NbtTag::Float(f32::from_bits(body.spatial.pitch)),
        ]),
    );
    nbt.put_bool("OnGround", body.spatial.on_ground);
}
