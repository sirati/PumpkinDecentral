use std::collections::BTreeMap;

use pumpkin_cluster::action_graph::LocalDeltaStep;
use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target, Value};
use pumpkin_cluster::entity_body::{EntityBirth, EntityBody};
use pumpkin_cluster::protocol::GlobalEntityId;
use uuid::Uuid;

#[derive(Clone)]
enum BodyPresentationEvent {
    Head {
        body: EntityBody,
        yaw: u32,
    },
    Spawn(EntityBody),
    Update {
        before: EntityBody,
        after: EntityBody,
    },
    Remove(EntityBody),
    Collect {
        body: EntityBody,
        collector: i32,
        amount: u32,
    },
}

struct FrameRenderDomain<'a> {
    health_refresh: bool,
    health: super::cluster_entity_health::HealthRenderContext,
    ready: &'a crate::net::chunk_sender::ReadyChunkView,
    winners: BTreeMap<pumpkin_cluster::protocol::BlockPos, GlobalEntityId>,
}

fn item_frame(body: &EntityBody) -> Option<&pumpkin_cluster::object_state::FrameState> {
    let pumpkin_cluster::entity_body::EntityPayload::Object(object) = &body.payload else { return None; };
    let pumpkin_cluster::object_state::ObjectKind::ItemFrame(frame) = &object.kind else { return None; };
    Some(frame)
}

impl FrameRenderDomain<'_> {
    fn ready(&self, position: pumpkin_cluster::protocol::BlockPos) -> bool {
        self.ready.chunks.contains(&pumpkin_util::math::vector2::Vector2::new(position.x.div_euclid(16), position.z.div_euclid(16)))
    }
}

#[derive(Clone, Default)]
struct BodyView {
    health_policy: Option<(Option<pumpkin_cluster::realm_completion::PeerGeneration>, pumpkin_cluster::health_policy::HealthPolicySnapshot)>,
    passenger_ids: BTreeMap<
        GlobalEntityId,
        BTreeMap<u8, (pumpkin_cluster::item_contact::ContactIdentity, i32)>,
    >,
    bodies: BTreeMap<GlobalEntityId, EntityBody>,
    head_yaws: BTreeMap<GlobalEntityId, u32>,
    failed: bool,
    revision: Option<u64>,
    spatial_revision: Option<u64>,
    world: Option<pumpkin_cluster::protocol::WorldId>,
    ready: Option<std::sync::Arc<crate::net::chunk_sender::ReadyChunkView>>,
}

impl BodyView {
    fn replace(
        &mut self,
        id: GlobalEntityId,
        next: Option<&EntityBody>,
        collector: Option<i32>,
        events: &mut Vec<BodyPresentationEvent>,
    ) {
        let previous = self.bodies.get(&id);
        if let (Some(previous), Some(collector)) = (previous, collector) {
            let before = body_item_count(previous);
            let after = next.map(body_item_count).unwrap_or(0);
            if before > after {
                events.push(BodyPresentationEvent::Collect {
                    body: previous.clone(),
                    collector,
                    amount: before - after,
                });
            }
        }
        if let (Some(before), Some(after)) = (previous, next) {
            if rendered_body_equal(before, after) {
                self.bodies.insert(id, after.clone());
                return;
            }
            if before.birth == after.birth
                && before.kind == after.kind
                && (before.payload == after.payload
                    || object_update_compatible(&before.payload, &after.payload)
                    || matches!((&before.payload, &after.payload),
                        (pumpkin_cluster::entity_body::EntityPayload::Vehicle(before),
                         pumpkin_cluster::entity_body::EntityPayload::Vehicle(after)) if before.kind == after.kind)
                    || matches!(
                        (&before.payload, &after.payload),
                        (
                            pumpkin_cluster::entity_body::EntityPayload::Living(_),
                            pumpkin_cluster::entity_body::EntityPayload::Living(_)
                        )
                    ))
            {
                events.push(BodyPresentationEvent::Update {
                    before: before.clone(),
                    after: after.clone(),
                });
                self.bodies.insert(id, after.clone());
                return;
            }
        }
        if let Some(previous) = previous {
            events.push(BodyPresentationEvent::Remove(previous.clone()));
        }
        if let Some(next) = next {
            events.push(BodyPresentationEvent::Spawn(next.clone()));
            self.bodies.insert(id, next.clone());
        } else {
            self.bodies.remove(&id);
        }
    }
}

impl BodyView {
    fn refresh_passengers(
        &mut self,
        previous: &Self,
        resolved: BTreeMap<
            GlobalEntityId,
            BTreeMap<u8, (pumpkin_cluster::item_contact::ContactIdentity, i32)>,
        >,
        events: &mut Vec<BodyPresentationEvent>,
    ) {
        for (id, passengers) in &resolved {
            if previous.passenger_ids.get(id) == Some(passengers) {
                continue;
            }
            let Some(body) = self.bodies.get(id) else {
                continue;
            };
            let mut before = body.clone();
            before.passengers = previous
                .passenger_ids
                .get(id)
                .into_iter()
                .flat_map(|passengers| passengers.iter())
                .map(|(seat, (identity, _))| (*seat, identity.clone()))
                .collect();
            let mut after = body.clone();
            after.passengers = passengers
                .iter()
                .map(|(seat, (identity, _))| (*seat, identity.clone()))
                .collect();
            if before.passengers == after.passengers {
                before.passengers.clear();
            }
            if before.passengers != after.passengers {
                events.push(BodyPresentationEvent::Update { before, after });
            }
        }
        self.passenger_ids = resolved;
    }
}

fn body_item_count(body: &EntityBody) -> u32 {
    match &body.payload {
        pumpkin_cluster::entity_body::EntityPayload::Item(item) => u32::from(item.count),
        pumpkin_cluster::entity_body::EntityPayload::Experience(orb) => orb.count,
        pumpkin_cluster::entity_body::EntityPayload::Projectile(projectile) => {
            u32::from(projectile.carried_item.count)
        }
        _ => 0,
    }
}

fn object_update_compatible(
    before: &pumpkin_cluster::entity_body::EntityPayload,
    after: &pumpkin_cluster::entity_body::EntityPayload,
) -> bool {
    use pumpkin_cluster::{entity_body::EntityPayload, object_state::ObjectKind};
    let (EntityPayload::Object(before), EntityPayload::Object(after)) = (before, after) else {
        return false;
    };
    super::cluster_object_metadata::spawn_data(before)
        == super::cluster_object_metadata::spawn_data(after)
        && match (&before.kind, &after.kind) {
            (ObjectKind::Painting(before), ObjectKind::Painting(after)) => {
                before.variant == after.variant
            }
            _ => std::mem::discriminant(&before.kind) == std::mem::discriminant(&after.kind),
        }
}

fn rendered_object_equal(
    before: &pumpkin_cluster::object_state::ObjectKind,
    after: &pumpkin_cluster::object_state::ObjectKind,
) -> bool {
    use pumpkin_cluster::object_state::ObjectKind;
    match (before, after) {
        (ObjectKind::ItemFrame(before), ObjectKind::ItemFrame(after)) => {
            before.facing == after.facing
                && before.item == after.item
                && before.rotation == after.rotation
                && before.invisible == after.invisible
                && before.glow == after.glow
        }
        (ObjectKind::Painting(before), ObjectKind::Painting(after)) => {
            before.facing == after.facing && before.variant == after.variant
        }
        (ObjectKind::LeashKnot { .. }, ObjectKind::LeashKnot { .. })
        | (ObjectKind::Marker { .. }, ObjectKind::Marker { .. }) => true,
        (ObjectKind::PrimedTnt(before), ObjectKind::PrimedTnt(after)) => {
            before.fuse == after.fuse && before.block_state == after.block_state
        }
        (ObjectKind::FallingBlock(before), ObjectKind::FallingBlock(after)) => {
            before.block_state == after.block_state
        }
        (ObjectKind::EndCrystal(before), ObjectKind::EndCrystal(after)) => {
            before.beam == after.beam && before.show_bottom == after.show_bottom
        }
        (ObjectKind::Display(before), ObjectKind::Display(after)) => before == after,
        (ObjectKind::Interaction(before), ObjectKind::Interaction(after)) => {
            before.width == after.width
                && before.height == after.height
                && before.response == after.response
        }
        (ObjectKind::AreaEffectCloud(before), ObjectKind::AreaEffectCloud(after)) => {
            before.continuation.radius == after.continuation.radius
                && (before.continuation.age < before.wait) == (after.continuation.age < after.wait)
                && before.potion == after.potion
        }
        _ => false,
    }
}

fn rendered_body_equal(before: &EntityBody, after: &EntityBody) -> bool {
    if before.birth != after.birth
        || before.kind != after.kind
        || before.spatial != after.spatial
        || before.passengers != after.passengers
    {
        return false;
    }
    match (&before.payload, &after.payload) {
        (
            pumpkin_cluster::entity_body::EntityPayload::Object(before),
            pumpkin_cluster::entity_body::EntityPayload::Object(after),
        ) => rendered_object_equal(&before.kind, &after.kind),
        (
            pumpkin_cluster::entity_body::EntityPayload::Item(before),
            pumpkin_cluster::entity_body::EntityPayload::Item(after),
        ) => {
            before.item == after.item
                && before.count == after.count
                && before.components == after.components
        }
        (
            pumpkin_cluster::entity_body::EntityPayload::Experience(before),
            pumpkin_cluster::entity_body::EntityPayload::Experience(after),
        ) => before.amount == after.amount,
        (
            pumpkin_cluster::entity_body::EntityPayload::Projectile(before),
            pumpkin_cluster::entity_body::EntityPayload::Projectile(after),
        ) => {
            before.kind == after.kind
                && before.carried_item == after.carried_item
                && before.critical == after.critical
                && before.pierce == after.pierce
                && (before.fire_ticks != 0) == (after.fire_ticks != 0)
        }
        (
            pumpkin_cluster::entity_body::EntityPayload::Living(before),
            pumpkin_cluster::entity_body::EntityPayload::Living(after),
        ) => {
            before.combat.health_milli == after.combat.health_milli
                && before.combat.absorption_milli == after.combat.absorption_milli
                && (before.combat.timing.fire_ticks != 0) == (after.combat.timing.fire_ticks != 0)
                && before.combat.equipment == after.combat.equipment
                && before.use_state == after.use_state
                && before.effects == after.effects
                && before.stand == after.stand
                && super::cluster_owned_mob_metadata::same_visible_state(before, after)
        }
        (
            pumpkin_cluster::entity_body::EntityPayload::Vehicle(before),
            pumpkin_cluster::entity_body::EntityPayload::Vehicle(after),
        ) => {
            before.kind == after.kind
                && before.paddle_left == after.paddle_left
                && before.paddle_right == after.paddle_right
                && before.wobble_ticks == after.wobble_ticks
                && before.wobble_side == after.wobble_side
                && before.damage_milli == after.damage_milli
                && (before.fuel_ticks != 0) == (after.fuel_ticks != 0)
                && before.fuse_ticks == after.fuse_ticks
        }
        _ => before.payload == after.payload,
    }
}

pub(super) struct EntityPresentationChanges {
    bodies: pumpkin_cluster::structural_map::StructuralMap<
        GlobalEntityId,
        (
            Option<pumpkin_cluster::protocol::ChunkKey>,
            Option<pumpkin_cluster::protocol::ChunkKey>,
        ),
    >,
    ids: std::collections::BTreeSet<GlobalEntityId>,
}

impl EntityPresentationChanges {
    pub(super) fn ids(&self) -> impl Iterator<Item = GlobalEntityId> + '_ {
        self.ids.iter().copied()
    }

    pub(super) fn body_changes(
        &self,
    ) -> &pumpkin_cluster::structural_map::StructuralMap<
        GlobalEntityId,
        (
            Option<pumpkin_cluster::protocol::ChunkKey>,
            Option<pumpkin_cluster::protocol::ChunkKey>,
        ),
    > {
        &self.bodies
    }

    pub(super) fn capture(
        blocks: &mut super::cluster_chunk_state::OwnedChunkStore,
        spatial: &mut pumpkin_cluster::item_spatial::OwnedItemSpatial,
    ) -> Self {
        let bodies = blocks.take_entity_changes();
        let mut ids = spatial.take_changes();
        ids.extend(bodies.keys().copied());
        Self { bodies, ids }
    }
}

#[derive(Default)]
pub(super) struct OwnerBodyPresentation {
    frame_palette: super::cluster_object_frame::BedrockFramePalette,
    homes: BTreeMap<GlobalEntityId, pumpkin_cluster::protocol::ChunkKey>,
    pending_ids: std::collections::BTreeSet<GlobalEntityId>,
    views: BTreeMap<(pumpkin_cluster::identity::GlobalPlayerId, u64, u32), BodyView>,
    identity_warning: bool,
    registry_failures: std::collections::BTreeSet<GlobalEntityId>,
    revision: Option<u64>,
    item_spatial_revision: Option<u64>,
    bodies: BTreeMap<pumpkin_cluster::protocol::ChunkKey, BTreeMap<GlobalEntityId, EntityBody>>,
}

impl OwnerBodyPresentation {
    pub(super) fn publish(
        &mut self,
        server: &super::Server,
        blocks: &super::cluster_chunk_state::OwnedChunkStore,
        item_spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
        ids: &mut OwnerEntityClientIds,
        inventory: &super::cluster_world_inventory::InventoryOwner,
        actors: &super::cluster_world_actor::OwnedActors,
        remote_players: &super::cluster_remote_player_presentation::OwnedRemotePlayerPresentation,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        changes: &EntityPresentationChanges,
        steps: &[LocalDeltaStep],
    ) {
        let health_policy = super::cluster_health_policy::render_cut(server);
        let mut dirty = changes.ids().collect::<std::collections::BTreeSet<_>>();
        if self.revision.is_none() {
            dirty.extend(blocks.local_entity_bodies().filter_map(|(_, cell)| {
                if let Value::Entity(body) = &cell.value {
                    Some(body.id)
                } else {
                    None
                }
            }));
        }
        for id in &dirty {
            let old = self
                .homes
                .remove(id)
                .or_else(|| changes.bodies.get(id).and_then(|(old, _)| *old));
            if let Some(old) = old {
                if let Some(bodies) = self.bodies.get_mut(&old) {
                    bodies.remove(id);
                    if bodies.is_empty() {
                        self.bodies.remove(&old);
                    }
                }
            }
            if let Some(chunk) = blocks.entity_body_chunk(false, *id)
                && let Some(cell) = blocks.entity_body(false, chunk, *id)
                && let Value::Entity(body) = &cell.value
            {
                self.bodies
                    .entry(chunk)
                    .or_default()
                    .insert(*id, item_spatial.project(body));
                self.homes.insert(*id, chunk);
            }
        }
        self.pending_ids.extend(dirty);
        self.pending_ids
            .extend(self.registry_failures.iter().copied());
        self.revision = Some(blocks.local_entity_revision());
        self.item_spatial_revision = Some(item_spatial.revision());
        if !self.pending_ids.is_empty() {
            let bodies = self
                .pending_ids
                .iter()
                .filter_map(|id| {
                    let chunk = *self.homes.get(id)?;
                    Some((chunk, self.bodies.get(&chunk)?.get(id)?))
                })
                .collect::<Vec<_>>();
            ids.ensure_capacity(
                bodies
                    .iter()
                    .filter(|(_, body)| !ids.bindings.contains_key(&body.id))
                    .count(),
            );
            match ids.prepare_bodies(bodies.iter().map(|(chunk, body)| {
                (
                    Target::Chunk {
                        key: *chunk,
                        part: ChunkPart::Entity(body.id),
                        inventory: None,
                    },
                    *body,
                )
            })) {
                Ok(prepared) => {
                    ids.commit(prepared);
                    self.identity_warning = false;
                }
                Err(reason) => {
                    if !self.identity_warning {
                        tracing::error!(
                            ?reason,
                            "cluster entity client identity preparation failed"
                        );
                        self.identity_warning = true;
                    }
                }
            }
            if !self.identity_warning {
                let mut directory = server.cluster_canonical_entities.load().as_ref().clone();
                let worlds = server.worlds.load();
                for id in &self.pending_ids {
                    let previous = directory.entity_by_id(*id).cloned();
                    directory.remove(*id);
                    let Some(chunk) = self.homes.get(id).copied() else {
                        self.registry_failures.remove(id);
                        continue;
                    };
                    let Some(body) = self.bodies.get(&chunk).and_then(|bodies| bodies.get(id))
                    else {
                        continue;
                    };
                    let world = worlds
                        .iter()
                        .find(|world| super::cluster_world_delta::world_id(world) == chunk.world);
                    let position = body.spatial.position.map(f64::from_bits);
                    let position = pumpkin_cluster::protocol::BlockPos {
                        x: position[0].floor() as i32,
                        y: position[1].floor() as i32,
                        z: position[2].floor() as i32,
                    };
                    let prepared = ids
                        .client_id(body)
                        .zip(super::cluster_entity_registry::bounds(body))
                        .zip(
                            blocks
                                .rough_biome_id(chunk, position)
                                .and_then(pumpkin_data::biome::Biome::from_id),
                        )
                        .zip(world)
                        .and_then(|(((id, bounds), biome), world)| {
                            let body_source = blocks.entity_body_shared(false, body.id)?;
                            let living = matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Living(_));
                            let target = pumpkin_cluster::inventory::InventoryTarget::Entity(body.id.into());
                            let revision = if living {
                                let root = inventory.canonical_entity_admission_root(body.id)?;
                                if root.body.birth != body.birth || root.body.kind != body.kind
                                    || !inventory.local_target_chunks(target)?.contains(&chunk) { return None; }
                                Some(inventory.phase_revision(pumpkin_cluster::canonical_action::TruthView::Local, target)?)
                            } else { None };
                            let query = previous.as_ref().and_then(|shell| shell.cast_any()
                                .downcast_ref::<super::cluster_entity_registry::CanonicalEntityShell>())
                                .and_then(|shell| shell.cached_query(&body_source, revision))
                                .or_else(|| {
                                    let snapshot = if living { Some(inventory.phase_snapshot(
                                        pumpkin_cluster::canonical_action::TruthView::Local, target)?) } else { None };
                                    super::cluster_entity_registry::PreparedEntityQuery::prepare(body,
                                        body_source, revision, snapshot.as_ref().map(|snapshot| snapshot.cells.as_slice()))
                                })?;
                            super::cluster_entity_registry::CanonicalEntityShell::prepare(
                                world.clone(),
                                body.clone(),
                                id,
                                bounds,
                                biome,
                                query,
                            )
                        });
                    if let Some(shell) = prepared {
                        self.registry_failures.remove(id);
                        directory.insert(shell);
                    } else if self.registry_failures.insert(*id) {
                        tracing::error!(entity = ?id, birth = ?body.birth, "cluster canonical entity registry preparation failed");
                    }
                }
                server
                    .cluster_canonical_entities
                    .store(std::sync::Arc::new(directory));
                self.pending_ids.retain(|id| self.registry_failures.contains(id));
            }
        }
        let mut active = std::collections::BTreeSet::new();
        for world in server.worlds.load().iter() {
            let world_id = pumpkin_cluster::protocol::WorldId::new(
                world.level.cluster_dimension,
                world.level.cluster_world_key,
            );
            for player in world.players.load().iter() {
                let Some(gid) = player.cluster_gid() else {
                    continue;
                };
                let Some(lease) = inventory.player_lease_for_gid(gid) else {
                    continue;
                };
                let Some(generation) = inventory.player_install_generation(gid) else {
                    continue;
                };
                if lease.key.0 != player.gameprofile.id
                    || generation
                        != player
                            .cluster_owner_install_generation
                            .load(std::sync::atomic::Ordering::Acquire)
                    || player.client.closed()
                {
                    continue;
                }
                let epoch = player
                    .chunk_send_epoch
                    .load(std::sync::atomic::Ordering::Acquire);
                let key = (gid, generation, epoch);
                active.insert(key);
                let mut retired_events = Vec::new();
                let retired_keys = self
                    .views
                    .range((gid, generation, u32::MIN)..=(gid, generation, u32::MAX))
                    .map(|(key, _)| *key)
                    .filter(|previous| {
                        previous.0 == gid && previous.1 == generation && previous.2 != epoch
                    })
                    .collect::<Vec<_>>();
                for retired in &retired_keys {
                    if let Some(previous) = self.views.get(retired) {
                        retired_events.extend(
                            previous
                                .bodies
                                .values()
                                .cloned()
                                .map(BodyPresentationEvent::Remove),
                        );
                    }
                }
                let visible = player.ready_chunk_view.load_full();
                let is_visible = |chunk: pumpkin_cluster::protocol::ChunkKey| {
                    chunk.world == world_id
                        && visible.contains(
                            epoch,
                            &pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()),
                        )
                };
                let visible_entities = visible
                    .chunks
                    .iter()
                    .filter(|_| visible.epoch == Some(epoch))
                    .filter_map(|position| {
                        self.bodies.get(&pumpkin_cluster::protocol::ChunkKey::new(
                            world_id, position.x, position.y,
                        ))
                    })
                    .flat_map(|bodies| bodies.iter())
                    .collect::<BTreeMap<_, _>>();
                let identity_id =
                    |identity: &pumpkin_cluster::item_contact::ContactIdentity| match identity {
                        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => {
                            if lease.gid == gid {
                                inventory
                                    .player_install_generation(lease.gid)
                                    .and_then(|install| {
                                        actors.local_client_entity_id(*lease, install)
                                    })
                            } else {
                                server
                                    .cluster_peer_incarnations
                                    .load()
                                    .get(&lease.gid.server)
                                    .copied()
                                    .and_then(|source_generation| {
                                        remote_players.visible_player_id(
                                            gid,
                                            generation,
                                            epoch,
                                            *lease,
                                            pumpkin_cluster::realm_completion::PeerGeneration {
                                                server: lease.gid.server,
                                                generation: source_generation,
                                            },
                                        )
                                    })
                            }
                        }
                        pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } => {
                            visible_entities
                                .get(id)
                                .filter(|body| &body.birth == birth)
                                .and_then(|_| ids.bindings.get(id))
                                .and_then(|(current, id)| (current == birth).then_some(*id))
                        }
                    };
                let passenger_ids = visible_entities
                    .iter()
                    .filter(|(_, body)| !body.passengers.is_empty())
                    .map(|(id, body)| {
                        (
                            **id,
                            body.passengers
                                .iter()
                                .filter_map(|(seat, identity)| {
                                    identity_id(identity).map(|id| (*seat, (identity.clone(), id)))
                                })
                                .collect::<BTreeMap<_, _>>(),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                let view = self.views.entry(key).or_default();

                if view.failed {
                    continue;
                }
                if passenger_ids == view.passenger_ids
                    && steps.is_empty()
                    && retired_events.is_empty()
                    && view.health_policy == Some(health_policy)
                    && view.revision == self.revision
                    && view.spatial_revision == self.item_spatial_revision
                    && view.world == Some(world_id)
                    && view
                        .ready
                        .as_ref()
                        .is_some_and(|previous| std::sync::Arc::ptr_eq(previous, &visible))
                {
                    continue;
                }
                let mut candidate = view.clone();
                let mut events = retired_events;
                for step in steps {
                    let mut changes = BTreeMap::<
                        GlobalEntityId,
                        (
                            Option<(pumpkin_cluster::protocol::ChunkKey, &EntityBody)>,
                            Option<(pumpkin_cluster::protocol::ChunkKey, &EntityBody)>,
                        ),
                    >::new();
                    for write in &step.writes {
                        let Target::Chunk {
                            key: chunk,
                            part: ChunkPart::Entity(id),
                            inventory: None,
                        } = write.target
                        else {
                            continue;
                        };
                        let change = changes.entry(id).or_default();
                        if let Some(Value::Entity(body)) =
                            write.before.as_ref().map(|cell| &cell.value)
                        {
                            change.0 = Some((chunk, body));
                        }
                        if let Some(Value::Entity(body)) =
                            write.after.as_ref().map(|cell| &cell.value)
                        {
                            change.1 = Some((chunk, body));
                        }
                    }
                    let full_body_ids = changes
                        .keys()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>();
                    for (id, (before, after)) in changes {
                        let collector = if step.direction
                            == pumpkin_cluster::action_graph::LocalDeltaDirection::Apply
                        {
                            before
                                .filter(|(chunk, _)| is_visible(*chunk))
                                .and_then(|(chunk, _)| {
                                    pickup_collector(
                                        server,
                                        inventory,
                                        actors,
                                        graphs,
                                        gid,
                                        player.gameprofile.id,
                                        &is_visible,
                                        step.reference,
                                        Target::Chunk {
                                            key: chunk,
                                            part: ChunkPart::Entity(id),
                                            inventory: None,
                                        },
                                    )
                                })
                        } else {
                            None
                        };
                        let next = after
                            .filter(|(chunk, _)| is_visible(*chunk))
                            .map(|(_, body)| body);
                        candidate.replace(id, next, collector, &mut events);
                    }
                    let mut property_bodies = BTreeMap::<GlobalEntityId, EntityBody>::new();
                    let property_targets = step
                        .writes
                        .iter()
                        .filter_map(|write| {
                            pumpkin_cluster::derived_living::actor_body_target(write.target)
                                .or_else(|| {
                                    pumpkin_cluster::frame_inventory_projection::body_target(
                                        write.target,
                                    )
                                })
                        })
                        .collect::<std::collections::BTreeSet<_>>();
                    for target in property_targets {
                        let Target::Chunk {
                            key: chunk,
                            part: ChunkPart::Entity(id),
                            inventory: None,
                        } = target
                        else {
                            continue;
                        };
                        if full_body_ids.contains(&id) || !is_visible(chunk) {
                            continue;
                        }
                        let Some(body) = candidate.bodies.get(&id).cloned() else {
                            continue;
                        };
                        let cell = pumpkin_cluster::canonical_action::Cell {
                            revision: 0,
                            value: Value::Entity((body).into()),
                        };
                        match pumpkin_cluster::derived_living::project_body_writes(
                            target,
                            Some(&cell),
                            &step.writes,
                        ) {
                            Ok(Some(pumpkin_cluster::canonical_action::Cell {
                                value: Value::Entity(body),
                                ..
                            })) => {
                                property_bodies.insert(id, *body);
                            }
                            reason => {
                                view.failed = true;
                                tracing::error!(
                                    ?gid,
                                    generation,
                                    epoch,
                                    ?target,
                                    ?reason,
                                    "cluster entity delta projection failed"
                                );
                                player.client.try_kick(
                                    crate::net::DisconnectReason::Unknown,
                                    &pumpkin_util::text::TextComponent::text(
                                        "Entity state could not be delivered",
                                    ),
                                );
                                break;
                            }
                        }
                    }
                    for (id, body) in property_bodies {
                        candidate.replace(id, Some(&body), None, &mut events);
                    }
                }
                if view.failed {
                    continue;
                }
                let desired = visible
                    .chunks
                    .iter()
                    .filter(|_| visible.epoch == Some(epoch))
                    .filter_map(|position| {
                        self.bodies.get(&pumpkin_cluster::protocol::ChunkKey::new(
                            world_id, position.x, position.y,
                        ))
                    })
                    .flat_map(|bodies| bodies.iter().map(|(id, body)| (*id, body)))
                    .collect::<BTreeMap<_, _>>();
                for id in candidate.bodies.keys().copied().collect::<Vec<_>>() {
                    if !desired.contains_key(&id) {
                        candidate.replace(id, None, None, &mut events);
                    }
                }
                for (id, body) in desired {
                    candidate.replace(id, Some(body), None, &mut events);
                    let yaw = item_spatial.head_yaw(body).unwrap_or(body.spatial.yaw);
                    if matches!(
                        body.payload,
                        pumpkin_cluster::entity_body::EntityPayload::Living(_)
                    ) && candidate.head_yaws.get(&id) != Some(&yaw)
                    {
                        candidate.head_yaws.insert(id, yaw);
                        events.push(BodyPresentationEvent::Head {
                            body: body.clone(),
                            yaw,
                        });
                    }
                }
                candidate
                    .head_yaws
                    .retain(|id, _| candidate.bodies.contains_key(id));
                candidate.refresh_passengers(view, passenger_ids, &mut events);
                candidate.revision = self.revision;

                candidate.spatial_revision = self.item_spatial_revision;
                candidate.world = Some(world_id);
                let directory = server.cluster_canonical_entities.load();
                let frame_domain = FrameRenderDomain { health_refresh: view.health_policy != Some(health_policy), health: super::cluster_entity_health::HealthRenderContext { mode: health_policy.1.mode, viewer: Some(lease) }, ready: visible.as_ref(), winners: if player.client.bedrock().is_some() { candidate.bodies.values().filter_map(|body| {
                    let frame = item_frame(body)?;
                    (directory.frame_at(body.spatial.world, frame.attachment) == Some(body.id))
                        .then_some((frame.attachment, body.id))
                }).collect() } else { BTreeMap::new() } };
                if player.client.bedrock().is_some() {
                    let mut affected = std::collections::BTreeSet::new();
                    let emitted = events.iter().map(|event| {
                        let body = match event {
                            BodyPresentationEvent::Spawn(body) | BodyPresentationEvent::Remove(body)
                            | BodyPresentationEvent::Update { after: body, .. } | BodyPresentationEvent::Collect { body, .. }
                            | BodyPresentationEvent::Head { body, .. } => body,
                        };
                        if let Some(frame) = item_frame(body) { affected.insert(frame.attachment); }
                        if let BodyPresentationEvent::Update { before, .. } = event {
                            if let Some(frame) = item_frame(before) { affected.insert(frame.attachment); }
                        }
                        body.id
                    }).collect::<std::collections::BTreeSet<_>>();
                    let ready_changed = !view.ready.as_ref().is_some_and(|old| std::sync::Arc::ptr_eq(old, &visible));
                    for body in candidate.bodies.values() {
                        if let Some(frame) = item_frame(body)
                            && frame_domain.winners.get(&frame.attachment) == Some(&body.id)
                            && !emitted.contains(&body.id) && (ready_changed || affected.contains(&frame.attachment)) {
                            events.push(BodyPresentationEvent::Update { before: body.clone(), after: body.clone() });
                        }
                    }
                }
                if view.health_policy != Some(health_policy) {
                    let emitted = events.iter().filter_map(|event| match event {
                        BodyPresentationEvent::Spawn(body) | BodyPresentationEvent::Update { after: body, .. } => Some(body.id),
                        _ => None,
                    }).collect::<std::collections::BTreeSet<_>>();
                    for body in candidate.bodies.values() {
                        if matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Living(_)) && !emitted.contains(&body.id) {
                            events.push(BodyPresentationEvent::Update { before: body.clone(), after: body.clone() });
                        }
                    }
                }
                candidate.health_policy = Some(health_policy);
                candidate.ready = Some(visible.clone());
                match prepare_body_events(
                    &player.client,
                    ids,
                    &events,
                    blocks,
                    &self.frame_palette,
                    Some(&frame_domain),
                    &identity_id,
                ) {
                    Ok(frames) => {
                        if player
                            .chunk_send_epoch
                            .load(std::sync::atomic::Ordering::Acquire)
                            != epoch
                            || player
                                .cluster_owner_install_generation
                                .load(std::sync::atomic::Ordering::Acquire)
                                != generation
                        {
                            continue;
                        }
                        for frame in frames {
                            player.client.try_enqueue_packet_data(frame);
                        }
                        if player.client.closed() {
                            view.failed = true;
                        } else {
                            *view = candidate;
                        }
                        for retired in retired_keys {
                            self.views.remove(&retired);
                        }
                    }
                    Err(reason) => {
                        view.failed = true;
                        tracing::error!(?gid, generation, epoch, %reason, "cluster entity client projection failed");
                        player.client.try_kick(
                            crate::net::DisconnectReason::Unknown,
                            &pumpkin_util::text::TextComponent::text(
                                "Entity state could not be delivered",
                            ),
                        );
                    }
                }
            }
        }
        self.views.retain(|key, _| active.contains(key));
    }
}

fn pickup_collector(
    server: &super::Server,
    inventory: &super::cluster_world_inventory::InventoryOwner,
    actors: &super::cluster_world_actor::OwnedActors,
    graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    viewer: pumpkin_cluster::identity::GlobalPlayerId,
    viewer_uuid: uuid::Uuid,
    visible: impl Fn(pumpkin_cluster::protocol::ChunkKey) -> bool,
    reference: pumpkin_cluster::canonical_action::ActionRef,
    source: Target,
) -> Option<i32> {
    let node = graphs
        .ticks
        .get(&reference.tick)?
        .actions
        .get(&reference.id)?;
    let mut destinations = Vec::new();
    if let Some(pumpkin_cluster::canonical_action::SemanticIntent::CollectProjectile(intent)) =
        &node.action.semantic_intent
        && intent.contact.item.target() == source
        && let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
            &intent.contact.picker.identity
    {
        destinations.push(lease.key);
    }
    if let Some(pumpkin_cluster::canonical_action::SemanticIntent::CollectExperience(intent)) =
        &node.action.semantic_intent
        && intent.contact.item.target() == source
        && let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
            &intent.contact.picker.identity
    {
        destinations.push(lease.key);
    }
    for effect in &node.factual.effects {
        let pumpkin_cluster::canonical_action::Effect::MoveItems {
            source: from,
            destination,
            ..
        } = effect
        else {
            continue;
        };
        if *from != source {
            continue;
        }
        let Target::Player { key, .. } = destination else {
            continue;
        };
        destinations.push(*key);
    }
    for key in destinations {
        let lease = *node
            .action
            .player_leases
            .iter()
            .find(|lease| lease.key == key)?;
        if lease.gid != viewer {
            let chunk = actors
                .get(lease.gid)
                .map(|truth| truth.local.chunk)
                .or_else(|| {
                    node.action
                        .player_chunks
                        .iter()
                        .find_map(|(key, chunk)| (*key == lease.key).then_some(*chunk))
                })?;
            if !visible(chunk)
                || !server.cluster_presence.snapshot().is_visible_to(
                    &lease.gid,
                    lease.key.0.as_bytes(),
                    Some(viewer_uuid.as_bytes()),
                )
            {
                return None;
            }
        }
        if inventory.player_lease_for_gid(lease.gid) == Some(lease)
            && let Some(generation) = inventory.player_install_generation(lease.gid)
            && let Some(id) = actors.local_client_entity_id(lease, generation)
        {
            return Some(id);
        }
        let origin = lease.gid.host();
        let generation = *server.cluster_peer_incarnations.load().get(&origin)?;
        return super::cluster_ghost::remote_player_client_entity_id(
            server,
            lease,
            pumpkin_cluster::realm_completion::PeerGeneration {
                server: origin,
                generation,
            },
        );
    }
    None
}

fn prepare_body_events(
    client: &crate::net::ClientPlatform,
    ids: &OwnerEntityClientIds,
    events: &[BodyPresentationEvent],
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    frame_palette: &super::cluster_object_frame::BedrockFramePalette,
    frame_domain: Option<&FrameRenderDomain<'_>>,
    identity_id: &impl Fn(&pumpkin_cluster::item_contact::ContactIdentity) -> Option<i32>,
) -> Result<Vec<bytes::Bytes>, String> {
    use pumpkin_protocol::codec::{var_int::VarInt, var_long::VarLong, var_ulong::VarULong};
    let mut frames = Vec::new();
    let mut total_bytes = 0_usize;
    let encoder = client.bedrock().map(|client| client.encoder_snapshot());
    for event in events {
        let first_frame = frames.len();
        let body = match event {
            BodyPresentationEvent::Spawn(body)
            | BodyPresentationEvent::Update { after: body, .. }
            | BodyPresentationEvent::Remove(body)
            | BodyPresentationEvent::Collect { body, .. } => body,
            BodyPresentationEvent::Head { body, .. } => body,
        };
        let id = ids
            .client_id(body)
            .ok_or("entity client identity is absent")?;
        match client {
            crate::net::ClientPlatform::Java(_) => {
                let version = client.java_version();
                match event {
                    BodyPresentationEvent::Head { yaw, .. } => frames.push(
                        crate::net::java::JavaClient::serialize_packet_for_version(
                            &pumpkin_protocol::java::client::play::CHeadRot::new(
                                VarInt(id),
                                (f32::from_bits(*yaw) * 256.0 / 360.0).rem_euclid(256.0) as u8,
                            ),
                            version,
                        )
                        .map_err(|error| error.to_string())?,
                    ),
                    BodyPresentationEvent::Spawn(body) => frames.extend(
                        prepare_java_body_spawn(body, id, version)
                            .map_err(|error| error.to_string())?,
                    ),
                    BodyPresentationEvent::Update {
                        before,
                        after: body,
                    } => {
                        let position = body.spatial.position.map(f64::from_bits);
                        let velocity = body
                            .spatial
                            .velocity_micro
                            .map(|value| value as f64 / 1_000_000.0);
                        frames.push(
                            crate::net::java::JavaClient::serialize_packet_for_version(
                                &pumpkin_protocol::java::client::play::CEntityPositionSync::new(
                                    VarInt(id),
                                    pumpkin_util::math::vector3::Vector3::new(
                                        position[0],
                                        position[1],
                                        position[2],
                                    ),
                                    pumpkin_util::math::vector3::Vector3::new(
                                        velocity[0],
                                        velocity[1],
                                        velocity[2],
                                    ),
                                    f32::from_bits(body.spatial.yaw),
                                    f32::from_bits(body.spatial.pitch),
                                    body.spatial.on_ground,
                                ),
                                version,
                            )
                            .map_err(|error| error.to_string())?,
                        );
                        if let pumpkin_cluster::entity_body::EntityPayload::Living(living) =
                            &body.payload
                        {
                            let mut metadata = Vec::new();
                            super::cluster_living_metadata::write_java_living_presentation(
                                living,
                                version,
                                &mut metadata,
                            )
                            .map_err(|error| error.to_string())?;
                            super::cluster_owned_mob_metadata::write_java(
                                living,
                                body.kind,
                                version,
                                &mut metadata,
                            )
                            .map_err(|error| error.to_string())?;
                            super::cluster_object_metadata::write_java_stand(
                                living,
                                version,
                                &mut metadata,
                            )
                            .map_err(|error| error.to_string())?;
                            metadata.push(
                                if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 {
                                    127
                                } else {
                                    255
                                },
                            );
                            if let pumpkin_cluster::entity_body::EntityPayload::Living(previous) =
                                &before.payload
                            {
                                let status = if previous.combat.health_milli > 0
                                    && living.combat.health_milli == 0
                                {
                                    Some(3)
                                } else if previous.combat.health_milli > living.combat.health_milli
                                    || previous.combat.absorption_milli
                                        > living.combat.absorption_milli
                                {
                                    Some(2)
                                } else {
                                    None
                                };
                                if let Some(status) = status {
                                    frames.push(crate::net::java::JavaClient::serialize_packet_for_version(
                                        &pumpkin_protocol::java::client::play::CEntityStatus::new(id, status), version,
                                    ).map_err(|error| error.to_string())?);
                                }
                                if previous.stand.as_ref().zip(living.stand.as_ref()).is_some_and(|(before, after)| after.last_hit.is_some() && before.last_hit != after.last_hit) {
                                    frames.push(crate::net::java::JavaClient::serialize_packet_for_version(
                                        &pumpkin_protocol::java::client::play::CEntityStatus::new(id, 32), version,
                                    ).map_err(|error| error.to_string())?);
                                }
                                if super::cluster_owned_mob_metadata::started_grazing(
                                    previous, living,
                                ) {
                                    frames.push(crate::net::java::JavaClient::serialize_packet_for_version(
                                        &pumpkin_protocol::java::client::play::CEntityStatus::new(id, 10), version,
                                    ).map_err(|error| error.to_string())?);
                                }
                            }
                            frames.push(
                                crate::net::java::JavaClient::serialize_packet_for_version(
                                    &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(
                                        VarInt(id),
                                        metadata.into_boxed_slice(),
                                    ),
                                    version,
                                )
                                .map_err(|error| error.to_string())?,
                            );
                        }
                        if let pumpkin_cluster::entity_body::EntityPayload::Object(object) =
                            &body.payload
                        {
                            let mut metadata = Vec::new();
                            super::cluster_object_metadata::write_java(
                                object,
                                version,
                                &mut metadata,
                            )
                            .map_err(|error| error.to_string())?;
                            if !metadata.is_empty() {
                                metadata.push(
                                    if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9
                                    {
                                        127
                                    } else {
                                        255
                                    },
                                );
                                frames.push(crate::net::java::JavaClient::serialize_packet_for_version(
                                    &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(VarInt(id), metadata.into_boxed_slice()), version)
                                    .map_err(|error| error.to_string())?);
                            }
                        }
                        if let pumpkin_cluster::entity_body::EntityPayload::Vehicle(vehicle) =
                            &body.payload
                        {
                            let mut metadata = Vec::new();
                            super::cluster_vehicle_metadata::write_java(
                                vehicle,
                                version,
                                &mut metadata,
                            )
                            .map_err(|error| error.to_string())?;
                            metadata.push(
                                if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 {
                                    127
                                } else {
                                    255
                                },
                            );
                            frames.push(
                                crate::net::java::JavaClient::serialize_packet_for_version(
                                    &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(
                                        VarInt(id),
                                        metadata.into_boxed_slice(),
                                    ),
                                    version,
                                )
                                .map_err(|error| error.to_string())?,
                            );
                        }
                    }
                    BodyPresentationEvent::Remove(_) => frames.push(
                        crate::net::java::JavaClient::serialize_packet_for_version(
                            &pumpkin_protocol::java::client::play::CRemoveEntities::new(&[VarInt(
                                id,
                            )]),
                            version,
                        )
                        .map_err(|error| error.to_string())?,
                    ),
                    BodyPresentationEvent::Collect {
                        collector, amount, ..
                    } => frames.push(
                        crate::net::java::JavaClient::serialize_packet_for_version(
                            &pumpkin_protocol::java::client::play::CTakeItemEntity::new(
                                VarInt(id),
                                VarInt(*collector),
                                VarInt(*amount as i32),
                            ),
                            version,
                        )
                        .map_err(|error| error.to_string())?,
                    ),
                }
            }
            crate::net::ClientPlatform::Bedrock(_) => {
                let encoder = encoder.as_ref().ok_or("Bedrock encoder is absent")?;
                if let pumpkin_cluster::entity_body::EntityPayload::Object(object) = &body.payload
                    && let pumpkin_cluster::object_state::ObjectKind::ItemFrame(frame) =
                        &object.kind
                {
                    if let BodyPresentationEvent::Update { before, .. } = event
                        && let Some(previous) = item_frame(before)
                        && previous.attachment != frame.attachment
                        && frame_domain.is_none_or(|domain| domain.ready(previous.attachment) && !domain.winners.contains_key(&previous.attachment)) {
                        frames.extend(super::cluster_object_frame::project(before, previous, true, blocks, frame_palette, encoder)?);
                    }
                    let removed = matches!(event, BodyPresentationEvent::Remove(_));
                    if frame_domain.is_none_or(|domain| domain.ready(frame.attachment) && if removed {
                        !domain.winners.contains_key(&frame.attachment)
                    } else { domain.winners.get(&frame.attachment) == Some(&body.id) })
                        && !matches!(event, BodyPresentationEvent::Head { .. }) {
                        frames.extend(super::cluster_object_frame::project(
                            body,
                            frame,
                            matches!(event, BodyPresentationEvent::Remove(_)),
                            blocks,
                            frame_palette,
                            encoder,
                        )?);
                    }
                } else {
                    frames.push(
                    match event {
                        BodyPresentationEvent::Head { body, yaw } => {
                            let position = body.spatial.position.map(f64::from_bits);
                            let angle = |bits| (f32::from_bits(bits) * 256.0 / 360.0).rem_euclid(256.0) as u8;
                            encoder.serialize_packet(&pumpkin_protocol::bedrock::client::move_actor_absolute::CMoveActorAbsolute {
                                actor_runtime_id: VarULong(id as u64),
                                header: u8::from(body.spatial.on_ground),
                                position: pumpkin_util::math::vector3::Vector3::new(position[0] as f32, position[1] as f32, position[2] as f32),
                                rotation_x: angle(body.spatial.pitch),
                                rotation_y: angle(body.spatial.yaw),
                                rotation_y_head: angle(*yaw),
                            })
                        }
                        BodyPresentationEvent::Spawn(body) => {
                            prepare_bedrock_body_spawn(body, id, encoder)
                        }
                        BodyPresentationEvent::Update { after: body, .. } => {
                            let position = body.spatial.position.map(f64::from_bits);
                            let angle = |bits| (f32::from_bits(bits) * 256.0 / 360.0).rem_euclid(256.0) as u8;
                            encoder.serialize_packet(&pumpkin_protocol::bedrock::client::move_actor_absolute::CMoveActorAbsolute {
                                actor_runtime_id: VarULong(id as u64),
                                header: u8::from(body.spatial.on_ground),
                                position: pumpkin_util::math::vector3::Vector3::new(position[0] as f32, position[1] as f32, position[2] as f32),
                                rotation_x: angle(body.spatial.pitch),
                                rotation_y: angle(body.spatial.yaw),
                                rotation_y_head: angle(body.spatial.yaw),
                            })
                        }
                        BodyPresentationEvent::Remove(_) => encoder.serialize_packet(
                            &pumpkin_protocol::bedrock::client::remove_actor::CRemoveActor::new(
                                VarLong(i64::from(id)),
                            ),
                        ),
                        BodyPresentationEvent::Collect { collector, .. } => encoder
                            .serialize_packet(
                            &pumpkin_protocol::bedrock::client::take_item_actor::CTakeItemActor {
                                item_runtime_id: VarULong(id as u64),
                                actor_runtime_id: VarULong(*collector as u64),
                            },
                        ),
                    }
                    .map_err(|error| error.to_string())?,
                );
                }
            }
        }
        if let BodyPresentationEvent::Update { before, after } = event {
            if let pumpkin_cluster::entity_body::EntityPayload::Vehicle(vehicle) = &after.payload
                && let Some(encoder) = &encoder
            {
                frames.push(
                    encoder
                        .serialize_packet(&pumpkin_protocol::bedrock::client::CSetActorData {
                            target_runtime_id: VarULong(id as u64),
                            actor_data: super::cluster_vehicle_metadata::bedrock(vehicle),
                            synced_properties: Default::default(),
                            tick: VarULong(0),
                        })
                        .map_err(|error| error.to_string())?,
                );
            }
            if let pumpkin_cluster::entity_body::EntityPayload::Object(object) = &after.payload
                && !matches!(
                    object.kind,
                    pumpkin_cluster::object_state::ObjectKind::ItemFrame(_)
                )
                && let Some(encoder) = &encoder
            {
                frames.push(
                    encoder
                        .serialize_packet(&pumpkin_protocol::bedrock::client::CSetActorData {
                            target_runtime_id: VarULong(id as u64),
                            actor_data: super::cluster_object_metadata::bedrock(object)
                                .map_err(|error| error.to_string())?,
                            synced_properties: Default::default(),
                            tick: VarULong(0),
                        })
                        .map_err(|error| error.to_string())?,
                );
            }
            if let pumpkin_cluster::entity_body::EntityPayload::Living(living) = &after.payload {
                if let Some(encoder) = &encoder {
                    frames.push(
                        encoder
                            .serialize_packet(&pumpkin_protocol::bedrock::client::CSetActorData {
                                target_runtime_id: VarULong(id as u64),
                                actor_data: super::cluster_owned_mob_metadata::bedrock(
                                    living, after.kind,
                                ),
                                synced_properties: Default::default(),
                                tick: VarULong(0),
                            })
                            .map_err(|error| error.to_string())?,
                    );
                    if let pumpkin_cluster::entity_body::EntityPayload::Living(previous) =
                        &before.payload
                    {
                        let event = if previous.combat.health_milli != 0
                            && living.combat.health_milli == 0
                        {
                            Some(
                                pumpkin_protocol::bedrock::server::actor_event::ActorEventID::Death,
                            )
                        } else if living.combat.health_milli < previous.combat.health_milli
                            || living.combat.absorption_milli < previous.combat.absorption_milli
                        {
                            Some(pumpkin_protocol::bedrock::server::actor_event::ActorEventID::Hurt)
                        } else {
                            None
                        };
                        if let Some(event_id) = event {
                            frames.push(encoder.serialize_packet(&pumpkin_protocol::bedrock::server::actor_event::SActorEvent {
                                target_runtime_id: VarULong(id as u64), event_id, data: VarInt(0), fire_at_position: None,
                            }).map_err(|error| error.to_string())?);
                        }
                        if super::cluster_owned_mob_metadata::started_grazing(previous, living) {
                            frames.push(encoder.serialize_packet(&pumpkin_protocol::bedrock::server::actor_event::SActorEvent {
                                target_runtime_id: VarULong(id as u64), event_id: pumpkin_protocol::bedrock::server::actor_event::ActorEventID::EatGrass,
                                data: VarInt(0), fire_at_position: None,
                            }).map_err(|error| error.to_string())?);
                        }
                    }
                }
            }
        }
        let previous_passengers = match event {
            BodyPresentationEvent::Update { before, .. } => Some(&before.passengers),
            _ => None,
        };
        if matches!(
            event,
            BodyPresentationEvent::Spawn(_) | BodyPresentationEvent::Update { .. }
        ) && previous_passengers != Some(&body.passengers)
            && (previous_passengers.is_some() || !body.passengers.is_empty())
        {
            let passengers = body
                .passengers
                .iter()
                .filter_map(|(seat, identity)| identity_id(identity).map(|id| (*seat, id)))
                .collect::<Vec<_>>();
            match client {
                crate::net::ClientPlatform::Java(_) => {
                    if client.java_version() <= pumpkin_util::version::JavaMinecraftVersion::V_1_8 {
                        let mut links = Vec::new();
                        if let Some(previous) = previous_passengers {
                            for identity in previous.values().filter(|identity| {
                                !body.passengers.values().any(|next| next == *identity)
                            }) {
                                if let Some(passenger) = identity_id(identity) {
                                    links.push((passenger, -1));
                                }
                            }
                        }
                        links.extend(passengers.into_iter().map(|(_, passenger)| (passenger, id)));
                        for (passenger, vehicle) in links {
                            frames.push(
                                crate::net::java::JavaClient::serialize_packet_for_version(
                                    &pumpkin_protocol::java::client::play::CSetEntityLink::new(
                                        passenger, vehicle, false,
                                    ),
                                    client.java_version(),
                                )
                                .map_err(|error| error.to_string())?,
                            );
                        }
                    } else {
                        let passengers = passengers
                            .into_iter()
                            .map(|(_, passenger)| VarInt(passenger))
                            .collect::<Vec<_>>();
                        frames.push(
                            crate::net::java::JavaClient::serialize_packet_for_version(
                                &pumpkin_protocol::java::client::play::CSetPassengers::new(
                                    VarInt(id),
                                    &passengers,
                                ),
                                client.java_version(),
                            )
                            .map_err(|error| error.to_string())?,
                        );
                    }
                }
                crate::net::ClientPlatform::Bedrock(_) => {
                    let encoder = encoder.as_ref().ok_or("missing Bedrock packet encoder")?;
                    let mut links = Vec::new();
                    if let Some(previous) = previous_passengers {
                        for identity in previous.values().filter(|identity| {
                            !body.passengers.values().any(|next| next == *identity)
                        }) {
                            if let Some(passenger) = identity_id(identity) {
                                links.push((passenger, 0));
                            }
                        }
                    }
                    links.extend(
                        passengers
                            .into_iter()
                            .map(|(seat, passenger)| (passenger, if seat == 0 { 1 } else { 2 })),
                    );
                    for (passenger, link_type) in links {
                        frames.push(
                            encoder
                                .serialize_packet(
                                    &pumpkin_protocol::bedrock::client::CSetActorLink {
                                        link:
                                            pumpkin_protocol::bedrock::client::common::ActorLink {
                                                ridden_unique_id: VarLong(i64::from(id)),
                                                rider_unique_id: VarLong(i64::from(passenger)),
                                                link_type,
                                                immediate: true,
                                                rider_initiated: false,
                                                vehicle_angular_velocity: 0.0,
                                            },
                                    },
                                )
                                .map_err(|error| error.to_string())?,
                        );
                    }
                }
            }
        }
        let health_context = frame_domain.map(|domain| domain.health).unwrap_or_default();
        let health_changed = match event {
            BodyPresentationEvent::Spawn(_) => true,
            BodyPresentationEvent::Update { before, after } => frame_domain.is_some_and(|domain| domain.health_refresh)
                || super::cluster_entity_health::values(before, health_context)? != super::cluster_entity_health::values(after, health_context)?,
            _ => false,
        };
        if health_changed { frames.extend(super::cluster_entity_health::project(body, id, client, health_context)?); }
        total_bytes = total_bytes.saturating_add(
            frames[first_frame..]
                .iter()
                .map(bytes::Bytes::len)
                .sum::<usize>(),
        );
        if total_bytes > 2_097_152 {
            return Err("entity presentation exceeds turn budget".to_owned());
        }
    }
    Ok(frames)
}

pub(super) fn prepare_bedrock_body_spawn(
    body: &EntityBody,
    client_id: i32,
    encoder: &pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder,
) -> Result<bytes::Bytes, std::io::Error> {
    use pumpkin_cluster::entity_body::EntityPayload;
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        MetadataValue, PropertySyncData, SyncedActorDataList, entity_data_key,
    };
    use pumpkin_protocol::bedrock::client::{add_actor::CAddActor, add_item_actor::CAddItemActor};
    use pumpkin_protocol::bedrock::network_item::ItemStackWrapper;
    use pumpkin_protocol::codec::{var_long::VarLong, var_ulong::VarULong};
    use pumpkin_util::math::{vector2::Vector2, vector3::Vector3};

    let position = body.spatial.position.map(f64::from_bits);
    let key = pumpkin_cluster::protocol::ChunkKey::new(
        body.spatial.world,
        (position[0].floor() as i32).div_euclid(16),
        (position[2].floor() as i32).div_euclid(16),
    );
    if client_id <= 0 || !super::cluster_world_canonical::valid_entity_body(body, key, body.id) {
        return Err(std::io::Error::other("invalid entity presentation"));
    }
    let position = Vector3::new(position[0] as f32, position[1] as f32, position[2] as f32);
    let velocity = Vector3::new(
        body.spatial.velocity_micro[0] as f32 / 1_000_000.0,
        body.spatial.velocity_micro[1] as f32 / 1_000_000.0,
        body.spatial.velocity_micro[2] as f32 / 1_000_000.0,
    );
    match &body.payload {
        EntityPayload::Item(item) => {
            let stack = super::cluster_invsee::item_stack_from_inventory_cell(
                &pumpkin_cluster::inventory::InventoryCell {
                    loc: pumpkin_cluster::inventory::InvLoc { inv: 0, slot: 0 },
                    item: item.item,
                    count: item.count,
                    nbt: item.components.clone(),
                },
            )
            .map_err(|_| std::io::Error::other("invalid item component presentation"))?;
            let mut item = ItemStackWrapper::from(&stack);
            item.net_id = std::num::NonZero::new(1);
            if item.id == 0 || item.stack_size != u16::from(stack.item_count) {
                return Err(std::io::Error::other("Bedrock item mapping is unavailable"));
            }
            encoder.serialize_packet(&CAddItemActor {
                target_actor_id: VarLong(i64::from(client_id)),
                target_runtime_id: VarULong(client_id as u64),
                item,
                position,
                velocity,
                entity_data: SyncedActorDataList::new(),
                is_from_fishing: false,
            })
        }
        EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
            amount,
            ..
        }) => {
            let mut actor_data = SyncedActorDataList::new();
            actor_data.set(
                entity_data_key::VALUE,
                MetadataValue::Int(
                    i32::try_from(*amount).map_err(|_| {
                        std::io::Error::other("experience orb value exceeds protocol")
                    })?,
                ),
            );
            let yaw = f32::from_bits(body.spatial.yaw);
            encoder.serialize_packet(&CAddActor {
                target_actor_id: VarLong(i64::from(client_id)),
                target_runtime_id: VarULong(client_id as u64),
                actor_type: "minecraft:xp_orb".to_owned(),
                position,
                velocity,
                rotation: Vector2::new(f32::from_bits(body.spatial.pitch), yaw),
                y_head_rotation: yaw,
                y_body_rotation: yaw,
                attributes_list: Vec::new(),
                actor_data,
                synced_properties: PropertySyncData::default(),
                actor_links: Vec::new(),
            })
        }
        EntityPayload::Projectile(projectile) => {
            use pumpkin_cluster::entity_body::ProjectileKind;
            let actor_type = match projectile.kind {
                ProjectileKind::Arrow | ProjectileKind::SpectralArrow => "minecraft:arrow",
                ProjectileKind::Trident => "minecraft:thrown_trident",
                ProjectileKind::Snowball => "minecraft:snowball",
                ProjectileKind::Egg => "minecraft:egg",
                ProjectileKind::EnderPearl => "minecraft:ender_pearl",
                ProjectileKind::ExperienceBottle => "minecraft:xp_bottle",
                ProjectileKind::SplashPotion => "minecraft:splash_potion",
                ProjectileKind::LingeringPotion => "minecraft:lingering_potion",
                ProjectileKind::Firework => "minecraft:fireworks_rocket",
            };
            let yaw = f32::from_bits(body.spatial.yaw);
            encoder.serialize_packet(&CAddActor {
                target_actor_id: VarLong(i64::from(client_id)),
                target_runtime_id: VarULong(client_id as u64),
                actor_type: actor_type.to_owned(),
                position,
                velocity,
                rotation: Vector2::new(f32::from_bits(body.spatial.pitch), yaw),
                y_head_rotation: yaw,
                y_body_rotation: yaw,
                attributes_list: Vec::new(),
                actor_data: SyncedActorDataList::new(),
                synced_properties: PropertySyncData::default(),
                actor_links: Vec::new(),
            })
        }
        EntityPayload::Living(living) => {
            let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)
                .ok_or_else(|| std::io::Error::other("living entity type is unavailable"))?;
            let yaw = f32::from_bits(body.spatial.yaw);
            encoder.serialize_packet(&CAddActor {
                target_actor_id: VarLong(i64::from(client_id)),
                target_runtime_id: VarULong(client_id as u64),
                actor_type: format!("minecraft:{}", kind.resource_name),
                position,
                velocity,
                rotation: Vector2::new(f32::from_bits(body.spatial.pitch), yaw),
                y_head_rotation: yaw,
                y_body_rotation: yaw,
                attributes_list: Vec::new(),
                actor_data: super::cluster_owned_mob_metadata::bedrock(living, body.kind),
                synced_properties: PropertySyncData::default(),
                actor_links: Vec::new(),
            })
        }
        EntityPayload::Object(object) => {
            if let pumpkin_cluster::object_state::ObjectKind::Painting(painting) = &object.kind {
                let direction = match painting.facing {
                    3 => 0,
                    4 => 1,
                    2 => 2,
                    5 => 3,
                    _ => return Err(std::io::Error::other("invalid painting direction")),
                };
                let motif =
                    super::cluster_object_metadata::bedrock_painting_motif(painting.variant)
                        .ok_or_else(|| std::io::Error::other("painting variant is unavailable"))?;
                return encoder.serialize_packet(
                    &pumpkin_protocol::bedrock::client::add_painting::CAddPainting {
                        target_actor_id: VarLong(i64::from(client_id)),
                        target_runtime_id: VarULong(client_id as u64),
                        position,
                        direction: pumpkin_protocol::codec::var_int::VarInt(direction),
                        motif,
                    },
                );
            }
            let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)
                .ok_or_else(|| std::io::Error::other("object entity type is unavailable"))?;
            let yaw = f32::from_bits(body.spatial.yaw);
            encoder.serialize_packet(&CAddActor {
                target_actor_id: VarLong(i64::from(client_id)),
                target_runtime_id: VarULong(client_id as u64),
                actor_type: format!("minecraft:{}", kind.resource_name),
                position,
                velocity,
                rotation: Vector2::new(f32::from_bits(body.spatial.pitch), yaw),
                y_head_rotation: yaw,
                y_body_rotation: yaw,
                attributes_list: Vec::new(),
                actor_data: super::cluster_object_metadata::bedrock(object)?,
                synced_properties: PropertySyncData::default(),
                actor_links: Vec::new(),
            })
        }
        EntityPayload::Vehicle(vehicle) => {
            let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)
                .ok_or_else(|| std::io::Error::other("vehicle entity type is unavailable"))?;
            let yaw = f32::from_bits(body.spatial.yaw);
            encoder.serialize_packet(&CAddActor {
                target_actor_id: VarLong(i64::from(client_id)),
                target_runtime_id: VarULong(client_id as u64),
                actor_type: format!("minecraft:{}", kind.resource_name),
                position,
                velocity,
                rotation: Vector2::new(f32::from_bits(body.spatial.pitch), yaw),
                y_head_rotation: yaw,
                y_body_rotation: yaw,
                attributes_list: Vec::new(),
                actor_data: super::cluster_vehicle_metadata::bedrock(vehicle),
                synced_properties: PropertySyncData::default(),
                actor_links: Vec::new(),
            })
        }
    }
}

pub(super) fn prepare_java_body_spawn(
    body: &EntityBody,
    client_id: i32,
    version: pumpkin_util::version::JavaMinecraftVersion,
) -> Result<Vec<bytes::Bytes>, pumpkin_protocol::ser::WritingError> {
    use pumpkin_cluster::entity_body::EntityPayload;
    use pumpkin_protocol::codec::var_int::VarInt;
    use pumpkin_protocol::java::client::play::MetadataSerializer;
    use pumpkin_protocol::java::client::play::{
        CSetEntityMetadata, CSpawnEntity, CSpawnExperienceOrb, Metadata,
    };
    use pumpkin_protocol::ser::NetworkWriteExt;
    use pumpkin_protocol::ser::WritingError;
    use pumpkin_util::math::vector3::Vector3;
    use pumpkin_util::version::JavaMinecraftVersion;

    let position = body.spatial.position.map(f64::from_bits);
    let key = pumpkin_cluster::protocol::ChunkKey::new(
        body.spatial.world,
        (position[0].floor() as i32).div_euclid(16),
        (position[2].floor() as i32).div_euclid(16),
    );
    if client_id <= 0 || !super::cluster_world_canonical::valid_entity_body(body, key, body.id) {
        return Err(WritingError::Message("invalid entity presentation".into()));
    }
    let encode = |packet: &CSpawnEntity| {
        crate::net::java::JavaClient::serialize_packet_for_version(packet, version)
    };
    if let EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
        amount,
        ..
    }) = &body.payload
        && version < JavaMinecraftVersion::V_1_21_5
    {
        let packet = CSpawnExperienceOrb {
            entity_id: VarInt(client_id),
            position: Vector3::new(position[0], position[1], position[2]),
            amount: i16::try_from(*amount).map_err(|_| {
                WritingError::Message("experience orb value exceeds protocol".into())
            })?,
        };
        return Ok(vec![
            crate::net::java::JavaClient::serialize_packet_for_version(&packet, version)?,
        ]);
    }
    let spawn = CSpawnEntity::new(
        VarInt(client_id),
        body.birth.uuid,
        VarInt(i32::from(body.kind)),
        Vector3::new(position[0], position[1], position[2]),
        f32::from_bits(body.spatial.pitch),
        f32::from_bits(body.spatial.yaw),
        f32::from_bits(body.spatial.yaw),
        VarInt(match &body.payload {
            EntityPayload::Object(object) => super::cluster_object_metadata::spawn_data(object),
            _ => 0,
        }),
        Vector3::new(
            body.spatial.velocity_micro[0] as f64 / 1_000_000.0,
            body.spatial.velocity_micro[1] as f64 / 1_000_000.0,
            body.spatial.velocity_micro[2] as f64 / 1_000_000.0,
        ),
    );
    let mut metadata = Vec::new();
    match &body.payload {
        EntityPayload::Item(item) => {
            let stack = super::cluster_invsee::item_stack_from_inventory_cell(
                &pumpkin_cluster::inventory::InventoryCell {
                    loc: pumpkin_cluster::inventory::InvLoc { inv: 0, slot: 0 },
                    item: item.item,
                    count: item.count,
                    nbt: item.components.clone(),
                },
            )
            .map_err(|_| WritingError::Message("invalid item component presentation".into()))?;
            if version < JavaMinecraftVersion::V_1_20_5 && !stack.patch.is_empty() {
                return Err(WritingError::UnsupportedVersion(version));
            }
            let stack =
                pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer::from(stack);
            if version >= JavaMinecraftVersion::V_1_21 {
                Metadata::new(pumpkin_data::tracked_data::item::ITEM, stack)
                    .write(&mut metadata, &version)?;
            } else if version < JavaMinecraftVersion::V_1_9 {
                metadata.push((5 << 5) | 10);
                stack.write_metadata(&mut metadata, &version)?;
            } else {
                let index = if version < JavaMinecraftVersion::V_1_10 {
                    5
                } else if version < JavaMinecraftVersion::V_1_14 {
                    6
                } else if version < JavaMinecraftVersion::V_1_17 {
                    7
                } else {
                    8
                };
                let serializer = if version < JavaMinecraftVersion::V_1_13 {
                    5
                } else if version < JavaMinecraftVersion::V_1_19_3 {
                    6
                } else {
                    7
                };
                metadata.push(index);
                metadata.write_var_int(&VarInt(serializer))?;
                stack.write_metadata(&mut metadata, &version)?;
            }
        }
        EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
            amount,
            ..
        }) => {
            let tracked = if version >= JavaMinecraftVersion::V_26_1 {
                pumpkin_data::tracked_data::experience_orb::DATA_VALUE
            } else {
                pumpkin_data::tracked_data::experience_orb::VALUE
            };
            if tracked.id.get(&version) == 255 {
                return Err(WritingError::UnsupportedVersion(version));
            }
            let amount = i32::try_from(*amount).map_err(|_| {
                WritingError::Message("experience orb value exceeds protocol".into())
            })?;
            Metadata::new(tracked, VarInt(amount)).write(&mut metadata, &version)?;
        }
        EntityPayload::Projectile(projectile) => {
            let burning = pumpkin_data::tracked_data::entity::DATA_SHARED_FLAGS_ID;
            if burning.id.get(&version) != 255 {
                Metadata::new(burning, u8::from(projectile.fire_ticks != 0))
                    .write(&mut metadata, &version)?;
            }
            if matches!(
                projectile.kind,
                pumpkin_cluster::entity_body::ProjectileKind::Arrow
                    | pumpkin_cluster::entity_body::ProjectileKind::SpectralArrow
                    | pumpkin_cluster::entity_body::ProjectileKind::Trident
            ) {
                let flags = pumpkin_data::tracked_data::abstract_arrow::ID_FLAGS;
                if flags.id.get(&version) != 255 {
                    Metadata::new(flags, u8::from(projectile.critical))
                        .write(&mut metadata, &version)?;
                }
                let pierce = pumpkin_data::tracked_data::abstract_arrow::PIERCE_LEVEL;
                if pierce.id.get(&version) != 255 {
                    Metadata::new(pierce, projectile.pierce).write(&mut metadata, &version)?;
                }
            }
        }
        EntityPayload::Living(living) => {
            super::cluster_living_metadata::write_java_living_presentation(
                living,
                version,
                &mut metadata,
            )?;
            super::cluster_object_metadata::write_java_stand(living, version, &mut metadata)?;
            super::cluster_owned_mob_metadata::write_java(
                living,
                body.kind,
                version,
                &mut metadata,
            )?;
        }
        EntityPayload::Object(object) => {
            super::cluster_object_metadata::write_java(object, version, &mut metadata)?
        }
        EntityPayload::Vehicle(vehicle) => {
            super::cluster_vehicle_metadata::write_java(vehicle, version, &mut metadata)?
        }
    }
    metadata.push(if version < JavaMinecraftVersion::V_1_9 {
        127
    } else {
        255
    });
    let metadata = CSetEntityMetadata::new(VarInt(client_id), metadata.into_boxed_slice());
    Ok(vec![
        encode(&spawn)?,
        crate::net::java::JavaClient::serialize_packet_for_version(&metadata, version)?,
    ])
}

pub(super) struct OwnerEntityClientIds {
    next: i32,
    end: i32,
    spare: std::collections::VecDeque<(i32, i32)>,
    bindings: BTreeMap<GlobalEntityId, (EntityBirth, i32)>,
    uuids: BTreeMap<Uuid, GlobalEntityId>,
}

pub(super) struct PreparedEntityClientIds {
    next: i32,
    end: i32,
    spare_used: usize,
    additions: BTreeMap<GlobalEntityId, (EntityBirth, i32)>,
}

impl OwnerEntityClientIds {
    pub(super) fn resolve_contact(
        &self,
        contact: &pumpkin_cluster::item_contact::ContactObservation,
    ) -> Option<i32> {
        let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } =
            &contact.identity
        else {
            return None;
        };
        if !contact.valid() {
            return None;
        }
        self.bindings
            .get(id)
            .and_then(|(bound_birth, client_id)| (bound_birth == birth).then_some(*client_id))
    }

    pub(super) fn new(start: Option<i32>, count: i32) -> Self {
        let (next, end) = start
            .filter(|start| *start > 0 && count > 0)
            .and_then(|start| start.checked_add(count).map(|end| (start, end)))
            .unwrap_or((0, 0));
        Self {
            next,
            end,
            spare: std::collections::VecDeque::new(),
            bindings: BTreeMap::new(),
            uuids: BTreeMap::new(),
        }
    }

    pub(super) fn prepare_local(
        &mut self,
        steps: &[LocalDeltaStep],
    ) -> Result<PreparedEntityClientIds, ApplyFailure> {
        let unknown = steps
            .iter()
            .flat_map(|step| &step.writes)
            .flat_map(|write| [&write.before, &write.after])
            .filter_map(|cell| match cell.as_ref().map(|cell| &cell.value) {
                Some(Value::Entity(body)) if !self.bindings.contains_key(&body.id) => Some(body.id),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        self.ensure_capacity(unknown);
        self.prepare_bodies(steps.iter().flat_map(|step| {
            step.writes.iter().flat_map(|write| {
                let target = write.target;
                [&write.before, &write.after]
                    .into_iter()
                    .filter_map(move |cell| match cell.as_ref().map(|cell| &cell.value) {
                        Some(Value::Entity(body)) => Some((target, body.as_ref())),
                        _ => None,
                    })
            })
        }))
    }

    fn ensure_capacity(&mut self, count: usize) {
        let available = (self.end - self.next).max(0) as usize
            + self
                .spare
                .iter()
                .map(|(next, end)| (end - next) as usize)
                .sum::<usize>();
        if available >= count {
            return;
        }
        let Ok(count) = i32::try_from((count - available).max(65_536)) else {
            return;
        };
        if let Some(start) = crate::entity::Entity::try_reserve_ids(count)
            && let Some(end) = start.checked_add(count)
        {
            self.spare.push_back((start, end));
        }
    }

    fn prepare_bodies<'a>(
        &self,
        bodies: impl Iterator<Item = (Target, &'a EntityBody)>,
    ) -> Result<PreparedEntityClientIds, ApplyFailure> {
        let mut prepared = PreparedEntityClientIds {
            next: self.next,
            end: self.end,
            spare_used: 0,
            additions: BTreeMap::new(),
        };
        let mut added_uuids = BTreeMap::new();
        for (target, body) in bodies {
            let Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            } = target
            else {
                return Err(ApplyFailure::Type(target));
            };
            if !body.belongs_to(id, key) {
                return Err(ApplyFailure::Type(target));
            }
            if self
                .uuids
                .get(&body.birth.uuid)
                .or_else(|| added_uuids.get(&body.birth.uuid))
                .is_some_and(|existing| *existing != id)
            {
                return Err(ApplyFailure::InvalidAction);
            }
            if let Some((birth, _)) = self
                .bindings
                .get(&id)
                .or_else(|| prepared.additions.get(&id))
            {
                if *birth != body.birth {
                    return Err(ApplyFailure::InvalidAction);
                }
                continue;
            }
            if prepared.next <= 0 || prepared.next >= prepared.end {
                if let Some((next, end)) = self.spare.get(prepared.spare_used) {
                    prepared.next = *next;
                    prepared.end = *end;
                    prepared.spare_used += 1;
                }
            }
            if prepared.next <= 0 || prepared.next >= prepared.end {
                return Err(ApplyFailure::Overflow(target));
            }
            added_uuids.insert(body.birth.uuid, id);
            prepared
                .additions
                .insert(id, (body.birth.clone(), prepared.next));
            prepared.next += 1;
        }
        Ok(prepared)
    }

    pub(super) fn commit(&mut self, prepared: PreparedEntityClientIds) {
        self.next = prepared.next;
        self.end = prepared.end;
        for _ in 0..prepared.spare_used {
            self.spare.pop_front();
        }
        for (id, (birth, client_id)) in prepared.additions {
            self.uuids.insert(birth.uuid, id);
            self.bindings.insert(id, (birth, client_id));
        }
    }

    pub(super) fn client_id(&self, body: &EntityBody) -> Option<i32> {
        self.bindings
            .get(&body.id)
            .filter(|(birth, _)| *birth == body.birth)
            .map(|(_, client_id)| *client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::canonical_action::{ActionId, ActionRef};
    use pumpkin_cluster::entity_body::{EntityPayload, EntitySpatial};
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::{ChunkKey, WorldId};
    use pumpkin_cluster::realm_completion::PeerGeneration;
    use pumpkin_cluster::time::TickStamp;

    fn body(local_id: i32, generation: u64) -> (Target, EntityBody) {
        let id = GlobalEntityId {
            origin: ServerId(2),
            local_id,
            generation,
        };
        let source = PeerGeneration {
            server: id.origin,
            generation: 9,
        };
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let body = EntityBody {
            mount: None,
            tags: Default::default(),
            tags_revision: 0,
            relation_versions: Default::default(),
            passengers: Default::default(),
            id,
            spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(ActionRef {
                tick: TickStamp(10),
                id: ActionId {
                    actor: ActorId::Peer(id.origin),
                    incarnation: 9,
                    sequence: local_id as u16,
                    family: 8,
                },
            }),
            birth: EntityBirth {
                action: ActionRef {
                    tick: TickStamp(10),
                    id: ActionId {
                        actor: ActorId::Peer(id.origin),
                        incarnation: 9,
                        sequence: local_id as u16,
                        family: 8,
                    },
                },
                source,
                uuid: Uuid::from_u128((u128::from(generation) << 64) | local_id as u128),
            },
            owner: source,
            kind: pumpkin_data::entity::EntityType::EXPERIENCE_ORB.id,
            spatial: EntitySpatial {
                world: key.world,
                position: [0.5_f64.to_bits(), 80_f64.to_bits(), 0.5_f64.to_bits()],
                velocity_micro: [0; 3],
                yaw: 0,
                pitch: 0,
                on_ground: false,
            },
            payload: EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
                amount: 7,
                count: 1,
                health_milli: 5_000,
                age: 0,
                last_lifetime_tick: None,
            }),
        };
        (
            Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            },
            body,
        )
    }

    #[test]
    fn restored_entity_is_presented_before_a_later_collection() {
        let (_, mut restored) = body(1, 1);
        restored.kind = pumpkin_data::entity::EntityType::ITEM.id;
        restored.payload = EntityPayload::Item(pumpkin_cluster::entity_body::DroppedItem {
            item: pumpkin_data::item::Item::STONE.id,
            count: 2,
            components: Vec::new(),
            age: 0,
            last_lifetime_tick: None,
            pickup_delay: 0,
            health_milli: 5_000,
            never_despawn: false,
            never_pickup: false,
            fire_immune: false,
        });
        let mut view = BodyView::default();
        let mut events = Vec::new();
        view.replace(restored.id, Some(&restored), None, &mut events);
        view.replace(restored.id, None, Some(42), &mut events);
        assert!(matches!(
            &events[..],
            [
                BodyPresentationEvent::Spawn(_),
                BodyPresentationEvent::Collect {
                    collector: 42,
                    amount: 2,
                    ..
                },
                BodyPresentationEvent::Remove(_)
            ]
        ));
        assert!(view.bodies.is_empty());
    }

    #[test]
    fn entity_age_changes_do_not_respawn_the_visible_entity() {
        let (_, before) = body(1, 1);
        let mut after = before.clone();
        let EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
            age, ..
        }) = &mut after.payload
        else {
            unreachable!()
        };
        *age = 20;
        let mut view = BodyView::default();
        let mut events = Vec::new();
        view.replace(before.id, Some(&before), None, &mut events);
        events.clear();
        view.replace(after.id, Some(&after), None, &mut events);
        assert!(events.is_empty());
        assert_eq!(view.bodies.get(&after.id), Some(&after));
    }

    #[test]
    fn client_id_refill_is_consumed_only_when_preparation_commits() {
        let mut ids = OwnerEntityClientIds::new(Some(100), 1);
        ids.spare.push_back((200, 202));
        let (first_target, first) = body(1, 1);
        let (second_target, second) = body(2, 1);
        let prepare = |ids: &OwnerEntityClientIds| {
            ids.prepare_bodies([(first_target, &first), (second_target, &second)].into_iter())
                .unwrap()
        };
        let discarded = prepare(&ids);
        assert_eq!(discarded.next, 201);
        assert_eq!(ids.next, 100);
        assert_eq!(ids.spare.len(), 1);
        let retried = prepare(&ids);
        ids.commit(retried);
        assert_eq!(ids.client_id(&first), Some(100));
        assert_eq!(ids.client_id(&second), Some(200));
        assert!(ids.spare.is_empty());
        assert_eq!(ids.next, 201);
    }

    #[test]
    fn body_client_ids_preserve_birth_across_undo_and_generation_reuse() {
        let mut ids = OwnerEntityClientIds::new(Some(100), 2);
        let (target, first) = body(1, 1);
        let prepared = ids.prepare_bodies([(target, &first)].into_iter()).unwrap();
        assert_eq!(ids.client_id(&first), None);
        ids.commit(prepared);
        assert_eq!(ids.client_id(&first), Some(100));
        let restored = ids.prepare_bodies([(target, &first)].into_iter()).unwrap();
        ids.commit(restored);
        assert_eq!(ids.next, 101);
        let (next_target, next) = body(1, (1_u64 << 32) | 1);
        let prepared = ids
            .prepare_bodies([(next_target, &next)].into_iter())
            .unwrap();
        ids.commit(prepared);
        assert_eq!(ids.client_id(&next), Some(101));
        assert_eq!(ids.client_id(&first), Some(100));
        let mut forged = first.clone();
        forged.birth.source.generation += 1;
        assert_eq!(ids.client_id(&forged), None);
        assert!(ids.prepare_bodies([(target, &forged)].into_iter()).is_err());
        let (last_target, last) = body(2, 1);
        assert!(
            ids.prepare_bodies([(last_target, &last)].into_iter())
                .is_err()
        );
        assert_eq!(ids.next, 102);
    }

    #[test]
    fn owned_bedrock_orb_spawn_carries_actual_value_and_identity() {
        use pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder;
        use pumpkin_protocol::codec::{
            var_int::VarInt, var_long::VarLong, var_uint::VarUInt, var_ulong::VarULong,
        };
        use pumpkin_protocol::serial::PacketRead;
        use pumpkin_util::math::{vector2::Vector2, vector3::Vector3};
        use std::io::Cursor;

        let (_, body) = body(1, 1);
        let original = body.clone();
        let bytes = prepare_bedrock_body_spawn(&body, 101, &BedrockBatchEncoder::new()).unwrap();
        assert_eq!(bytes[0], 254);
        let mut cursor = Cursor::new(&bytes[1..]);
        let length = VarUInt::read(&mut cursor).unwrap().0;
        assert_eq!(
            length as usize,
            bytes.len() - 1 - cursor.position() as usize
        );
        assert_eq!(VarUInt::read(&mut cursor).unwrap().0, 13);
        assert_eq!(VarLong::read(&mut cursor).unwrap().0, 101);
        assert_eq!(VarULong::read(&mut cursor).unwrap().0, 101);
        assert_eq!(String::read(&mut cursor).unwrap(), "minecraft:xp_orb");
        assert_eq!(
            Vector3::<f32>::read(&mut cursor).unwrap(),
            Vector3::new(0.5, 80.0, 0.5)
        );
        assert_eq!(
            Vector3::<f32>::read(&mut cursor).unwrap(),
            Vector3::new(0.0, 0.0, 0.0)
        );
        assert_eq!(
            Vector2::<f32>::read(&mut cursor).unwrap(),
            Vector2::new(0.0, 0.0)
        );
        assert_eq!(f32::read(&mut cursor).unwrap(), 0.0);
        assert_eq!(f32::read(&mut cursor).unwrap(), 0.0);
        assert_eq!(VarUInt::read(&mut cursor).unwrap().0, 0);
        assert_eq!(VarUInt::read(&mut cursor).unwrap().0, 1);
        assert_eq!(VarUInt::read(&mut cursor).unwrap().0, 15);
        assert_eq!(VarUInt::read(&mut cursor).unwrap().0, 2);
        assert_eq!(VarInt::read(&mut cursor).unwrap().0, 7);
        for _ in 0..3 {
            assert_eq!(VarUInt::read(&mut cursor).unwrap().0, 0);
        }
        assert_eq!(cursor.position() as usize, bytes.len() - 1);
        assert_eq!(body, original);
        assert!(prepare_bedrock_body_spawn(&body, 0, &BedrockBatchEncoder::new()).is_err());
    }

    #[test]
    fn body_client_id_preparation_is_atomic_and_uuid_cannot_alias() {
        let mut ids = OwnerEntityClientIds::new(Some(100), 1);
        let (target, first) = body(1, 1);
        let (second_target, mut second) = body(2, 1);
        assert!(
            ids.prepare_bodies([(target, &first), (second_target, &second)].into_iter())
                .is_err()
        );
        assert_eq!(ids.next, 100);
        assert!(ids.bindings.is_empty());
        ids.commit(ids.prepare_bodies([(target, &first)].into_iter()).unwrap());
        second.birth.uuid = first.birth.uuid;
        assert!(
            ids.prepare_bodies([(second_target, &second)].into_iter())
                .is_err()
        );
        assert_eq!(ids.client_id(&first), Some(100));
    }

    #[test]
    fn owned_orb_spawn_preserves_uuid_value_and_packet_order() {
        use pumpkin_protocol::MultiVersionJavaPacket;
        use pumpkin_protocol::java::client::play::{CSetEntityMetadata, CSpawnEntity};
        use pumpkin_protocol::ser::NetworkReadExt;
        use pumpkin_util::version::JavaMinecraftVersion;

        let (_, body) = body(1, 1);
        for version in [JavaMinecraftVersion::V_1_21_5, JavaMinecraftVersion::V_26_1] {
            let packets = prepare_java_body_spawn(&body, 101, version).unwrap();
            assert_eq!(packets.len(), 2);
            let mut spawn = packets[0].as_ref();
            assert_eq!(spawn.get_var_int().unwrap().0, CSpawnEntity::to_id(version));
            let spawn = CSpawnEntity::read_packet_data(spawn, &version).unwrap();
            assert_eq!(spawn.entity_id.0, 101);
            assert_eq!(spawn.entity_uuid, body.birth.uuid);
            assert_eq!(spawn.position.y, 80.0);
            let mut metadata = packets[1].as_ref();
            assert_eq!(
                metadata.get_var_int().unwrap().0,
                CSetEntityMetadata::to_id(version)
            );
            let entity_id = if version < JavaMinecraftVersion::V_1_8 {
                metadata.get_i32_be().unwrap()
            } else {
                metadata.get_var_int().unwrap().0
            };
            assert_eq!(entity_id, 101);
            assert_eq!(metadata.get_u8().unwrap(), 8);
            assert_eq!(metadata.get_var_int().unwrap().0, 1);
            assert_eq!(metadata.get_var_int().unwrap().0, 7);
            assert_eq!(metadata, &[255]);
        }
        assert_eq!(
            prepare_java_body_spawn(&body, 101, JavaMinecraftVersion::V_1_8)
                .unwrap()
                .len(),
            1
        );
        assert!(prepare_java_body_spawn(&body, 0, JavaMinecraftVersion::V_26_1).is_err());
    }

    #[test]
    fn owned_item_spawn_keeps_exact_stack_components() {
        use pumpkin_cluster::entity_body::{DroppedItem, EntityPayload};
        use pumpkin_protocol::MultiVersionJavaPacket;
        use pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer;
        use pumpkin_protocol::java::client::play::CSetEntityMetadata;
        use pumpkin_protocol::ser::NetworkReadExt;
        use pumpkin_util::version::JavaMinecraftVersion;

        let (_, mut body) = body(1, 1);
        let mut stack = pumpkin_data::item_stack::ItemStack::static_new_java(
            2,
            &pumpkin_data::item::Item::STONE,
        );
        stack.set_custom_name("alpha".into());
        let cell = super::super::cluster_invsee::inventory_cell_from_item_stack(
            pumpkin_cluster::inventory::InvLoc { inv: 0, slot: 0 },
            &stack,
        )
        .unwrap();
        body.kind = pumpkin_data::entity::EntityType::ITEM.id;
        body.payload = EntityPayload::Item(DroppedItem {
            item: cell.item,
            count: cell.count,
            components: cell.nbt,
            age: 0,
            last_lifetime_tick: None,
            pickup_delay: 0,
            health_milli: 5_000,
            never_despawn: false,
            never_pickup: false,
            fire_immune: false,
        });
        let original = body.clone();
        assert!(
            prepare_bedrock_body_spawn(
                &body,
                101,
                &pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder::new(),
            )
            .is_ok()
        );
        for (version, header, serializer, terminator) in [
            (JavaMinecraftVersion::V_1_7_6, 170, None, 127),
            (JavaMinecraftVersion::V_1_8, 170, None, 127),
            (JavaMinecraftVersion::V_1_9, 5, Some(5), 255),
            (JavaMinecraftVersion::V_1_10, 6, Some(5), 255),
            (JavaMinecraftVersion::V_1_12_2, 6, Some(5), 255),
            (JavaMinecraftVersion::V_1_13, 6, Some(6), 255),
            (JavaMinecraftVersion::V_1_13_2, 6, Some(6), 255),
            (JavaMinecraftVersion::V_1_14, 7, Some(6), 255),
            (JavaMinecraftVersion::V_1_16_4, 7, Some(6), 255),
            (JavaMinecraftVersion::V_1_17, 8, Some(6), 255),
            (JavaMinecraftVersion::V_1_19_1, 8, Some(6), 255),
            (JavaMinecraftVersion::V_1_19_3, 8, Some(7), 255),
            (JavaMinecraftVersion::V_1_20_5, 8, Some(7), 255),
            (JavaMinecraftVersion::V_1_21, 8, Some(7), 255),
            (JavaMinecraftVersion::V_1_21_5, 8, Some(7), 255),
            (JavaMinecraftVersion::V_26_1, 8, Some(7), 255),
            (JavaMinecraftVersion::V_26_2, 8, Some(7), 255),
        ] {
            let presented = if version < JavaMinecraftVersion::V_1_20_5 {
                assert!(prepare_java_body_spawn(&body, 101, version).is_err());
                let mut plain = body.clone();
                let plain_stack = pumpkin_data::item_stack::ItemStack::static_new_java(
                    2,
                    &pumpkin_data::item::Item::STONE,
                );
                let plain_cell = super::super::cluster_invsee::inventory_cell_from_item_stack(
                    pumpkin_cluster::inventory::InvLoc { inv: 0, slot: 0 },
                    &plain_stack,
                )
                .unwrap();
                let EntityPayload::Item(item) = &mut plain.payload else {
                    unreachable!()
                };
                item.components = plain_cell.nbt;
                plain
            } else {
                body.clone()
            };
            let packets = prepare_java_body_spawn(&presented, 101, version).unwrap();
            assert_eq!(packets.len(), 2);
            let mut metadata = packets[1].as_ref();
            assert_eq!(
                metadata.get_var_int().unwrap().0,
                CSetEntityMetadata::to_id(version)
            );
            let entity_id = if version < JavaMinecraftVersion::V_1_8 {
                metadata.get_i32_be().unwrap()
            } else {
                metadata.get_var_int().unwrap().0
            };
            assert_eq!(entity_id, 101);
            assert_eq!(metadata.get_u8().unwrap(), header);
            if let Some(serializer) = serializer {
                assert_eq!(metadata.get_var_int().unwrap().0, serializer);
            }
            let restored = ItemStackSerializer::read_with_version(&mut metadata, &version).unwrap();
            assert_eq!(restored.0.item.id, stack.item.id);
            assert_eq!(restored.0.item_count, 2);
            if version >= JavaMinecraftVersion::V_1_20_5 {
                assert_eq!(
                    restored.0.get_custom_name().unwrap().clone().get_text(),
                    "alpha"
                );
            } else {
                assert!(restored.0.get_custom_name().is_none());
            }
            assert_eq!(metadata, &[terminator]);
        }
        assert_eq!(body, original);
    }
}

#[cfg(test)]
#[path = "cluster_object_projection_tests.rs"]
mod object_tests;

#[cfg(test)]
#[path = "cluster_vehicle_projection_tests.rs"]
mod vehicle_tests;
