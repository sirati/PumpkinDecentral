use std::sync::Arc;

#[path = "cluster_vehicle_terminal.rs"]
mod terminal;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::{ActorActionDraft, FrozenEntityIssueAuthority};
use pumpkin_cluster::actor_relation::VehicleControlInput;
use pumpkin_cluster::canonical_action::{
    Action, ActionId, ActionState, ApplyFailure, ChunkPart, Target, TruthView, Value,
};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::vehicle_control::VehicleControllerFrame;

pub(super) struct FrozenVehicleControl {
    pub input: Option<VehicleControlInput>,
    pub body: EntityBody,
    pub chunk: ChunkKey,
    pub tick: pumpkin_cluster::time::TickStamp,
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    frame: VehicleControllerFrame,
    draft: Option<ActorActionDraft>,
    pin: Option<super::cluster_actor_chunk_pin::ActorChunkPin>,
    ready: bool,
    facts_ready: bool,
    terminal: Option<terminal::FrozenVehicleTerminal>,
    hopper: Option<Action>,
    activation: Option<pumpkin_cluster::vehicle_tick::VehicleActivation>,
}

pub(super) fn required_chunks(body: &EntityBody) -> Result<Vec<ChunkKey>, ApplyFailure> {
    let position = body.spatial.position.map(f64::from_bits);
    let reach = |axis: usize| (body.spatial.velocity_micro[axis] as f64 / 1_000_000.0).abs() + 1.6;
    let min_x = ((position[0] - reach(0)).floor() as i32).div_euclid(16);
    let max_x = ((position[0] + reach(0)).ceil() as i32).div_euclid(16);
    let min_z = ((position[2] - reach(2)).floor() as i32).div_euclid(16);
    let max_z = ((position[2] + reach(2)).ceil() as i32).div_euclid(16);
    let width = i64::from(max_x) - i64::from(min_x) + 1;
    let depth = i64::from(max_z) - i64::from(min_z) + 1;
    if width <= 0 || depth <= 0 || width.checked_mul(depth).is_none_or(|area| area > 8192) {
        return Err(ApplyFailure::Capacity);
    }
    Ok((min_x..=max_x)
        .flat_map(|x| (min_z..=max_z).map(move |z| ChunkKey::new(body.spatial.world, x, z)))
        .collect())
}

impl FrozenVehicleControl {
    pub(super) fn idle(
        snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        mut body: EntityBody,
        chunk: ChunkKey,
        tick: pumpkin_cluster::time::TickStamp,
        draft: Option<ActorActionDraft>,
        pin: super::cluster_actor_chunk_pin::ActorChunkPin,
    ) -> Result<Option<Self>, ApplyFailure> {
        if snapshot.original_tick() != tick || due_tnt(&body, tick) {
            return Ok(None);
        }
        if let Some(draft) = &draft {
            let target = Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            };
            let Value::Entity(current) = draft
                .view(snapshot.as_ref())
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?
                .value
            else {
                return Err(ApplyFailure::Type(target));
            };
            let spatial = body.spatial.clone();
            if current.birth != body.birth || current.spatial_epoch != body.spatial_epoch {
                return Err(ApplyFailure::InvalidAction);
            }
            body = *current;
            body.spatial = spatial;
        }
        let terminal = terminal::FrozenVehicleTerminal::capture(
            snapshot.as_ref(),
            draft.as_ref(),
            &body,
            chunk,
            tick,
        )?;
        let Some(frame) = VehicleControllerFrame::idle(&body, tick) else {
            return Ok(None);
        };
        Ok(Some(Self {
            input: None,
            body,
            chunk,
            tick,
            snapshot,
            frame,
            draft,
            pin: Some(pin),
            ready: false,
            facts_ready: false,
            terminal,
            hopper: None,
            activation: None,
        }))
    }

    pub(super) fn capture(
        snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        input: VehicleControlInput,
        mut body: EntityBody,
        chunk: ChunkKey,
        driver_chunk: ChunkKey,
        tick: pumpkin_cluster::time::TickStamp,
        draft: Option<ActorActionDraft>,
    ) -> Result<Self, ApplyFailure> {
        if snapshot.original_tick() != tick || due_tnt(&body, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(draft) = &draft {
            let target = Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            };
            let Value::Entity(current) = draft
                .view(snapshot.as_ref())
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?
                .value
            else {
                return Err(ApplyFailure::Type(target));
            };
            let spatial = body.spatial.clone();
            if current.birth != body.birth || current.spatial_epoch != body.spatial_epoch {
                return Err(ApplyFailure::InvalidAction);
            }
            body = *current;
            body.spatial = spatial;
        }
        if !snapshot.validate_live_player_lease(input.driver) {
            return Err(ApplyFailure::InvalidAction);
        }
        let target = Target::Chunk {
            key: driver_chunk,
            part: ChunkPart::Player(input.driver.gid),
            inventory: None,
        };
        let Value::Living(driver) = snapshot
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?
            .value
        else {
            return Err(ApplyFailure::Type(target));
        };
        if driver.combat.health_milli == 0
            || driver
                .avatar
                .as_ref()
                .is_none_or(|avatar| avatar.game_mode == 3)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mount = driver
            .avatar
            .as_ref()
            .and_then(|avatar| avatar.mount.as_ref());
        let mut accepted = input.clone();
        accepted.simulation_tick = tick;
        if matches!(&body.payload, EntityPayload::Vehicle(state)
            if state.simulation_tick.is_some_and(|previous| !tick.is_after(previous)))
        {
            return Err(ApplyFailure::Capacity);
        }
        let mut frame =
            pumpkin_cluster::vehicle_control::capture(&body, body.owner, &accepted, mount)
                .map_err(|_| ApplyFailure::InvalidAction)?;
        frame.claimed = Some(input.clone());
        frame.accepted_tick = tick;
        if !matches!(body.payload, EntityPayload::Vehicle(_)) {
            return Err(ApplyFailure::InvalidAction);
        }
        let terminal = terminal::FrozenVehicleTerminal::capture(
            snapshot.as_ref(),
            draft.as_ref(),
            &body,
            chunk,
            tick,
        )?;
        Ok(Self {
            input: Some(input),
            body,
            chunk,
            tick,
            snapshot,
            frame,
            draft,
            pin: None,
            ready: false,
            facts_ready: false,
            terminal,
            hopper: None,
            activation: None,
        })
    }

    pub(super) fn prepare(
        &mut self,
        server: &super::Server,
        pins: &mut super::cluster_actor_chunk_pin::ActorChunkPins,
        chunks: &super::cluster_chunk_state::OwnedChunkStore,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    ) -> Result<(), ApplyFailure> {
        self.prepare_facts(server, pins, chunks, graphs)?;
        self.finish_prepare()
    }

    pub(super) fn prepare_facts(
        &mut self,
        server: &super::Server,
        pins: &mut super::cluster_actor_chunk_pin::ActorChunkPins,
        chunks: &super::cluster_chunk_state::OwnedChunkStore,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    ) -> Result<(), ApplyFailure> {
        if self.terminal.is_some() {
            self.facts_ready = true;
            return Ok(());
        }
        if self.facts_ready {
            return Ok(());
        }
        if self.ready {
            return Ok(());
        }
        if self.pin.is_none() {
            let ring = required_chunks(&self.body)?;
            self.pin = Some(pins.acquire(server, &ring)?);
        }
        let pin = self.pin.as_ref().ok_or(ApplyFailure::Capacity)?;
        if !pin.available(chunks) {
            return Err(ApplyFailure::Capacity);
        }
        let palette = self
            .snapshot
            .original_store()
            .snapshot(self.chunk)
            .ok_or(ApplyFailure::Capacity)?;
        let minimum = palette.min_y();
        let maximum = minimum + (palette.block_palettes().len() as i32) * 16;
        let y = f64::from_bits(self.body.spatial.position[1]);
        let vertical = (self.body.spatial.velocity_micro[1] as f64 / 1_000_000.0).abs();
        let low = ((y - vertical - 2.0).floor() as i32).max(minimum);
        let high = ((y + vertical + 4.0).ceil() as i32).min(maximum);
        let targets = if high > low {
            super::cluster_actor_chunk_pin::section_targets(pin.chunks(), low, high)?
        } else {
            Vec::new()
        };
        self.snapshot = self.snapshot.expand_history(graphs, self.tick, &targets)?;
        self.facts_ready = true;
        Ok(())
    }

    pub(super) fn finish_prepare(&mut self) -> Result<(), ApplyFailure> {
        if self.terminal.is_some() {
            self.ready = true;
            return Ok(());
        }
        if self.ready {
            return Ok(());
        }
        if !self.facts_ready {
            return Err(ApplyFailure::Capacity);
        }
        let pin = self.pin.as_ref().ok_or(ApplyFailure::Capacity)?;
        let mut loaded = self
            .snapshot
            .original_store()
            .owned_chunks()
            .collect::<std::collections::BTreeSet<_>>();
        loaded.extend(pin.chunks().iter().copied());
        let empty = ActorActionDraft::new(
            ActorId::Entity(self.body.id),
            self.body.id.generation,
            self.tick,
            4096,
            16384,
        );
        let world = super::cluster_vehicle_world::VehicleWorldView::new(
            self.snapshot.as_ref(),
            self.draft.as_ref().unwrap_or(&empty),
            &self.body.spatial,
            &loaded,
        )
        .with_rules(
            self.snapshot
                .mob_rules(self.body.spatial.world)
                .ok_or(ApplyFailure::InvalidAction)?,
        );
        self.activation = if self.frame.state.kind
            == pumpkin_cluster::entity_body::VehicleKind::Minecart
            && (self.body.passengers.is_empty() || self.body.mount.is_some())
        {
            None
        } else {
            world.activation(&self.body.spatial, self.frame.state.kind)?
        };
        if let Some(activation) = &mut self.activation
            && activation.kind == pumpkin_cluster::entity_body::VehicleKind::Minecart
        {
            activation.detach = Some(terminal::capture_detach(
                self.snapshot.as_ref(),
                &self
                    .draft
                    .as_ref()
                    .unwrap_or(&empty)
                    .view(self.snapshot.as_ref()),
                &self.body,
                self.chunk,
                self.tick,
            )?);
        }
        let mut frame = self.frame.clone();
        frame.simulate(&world).map_err(|_| ApplyFailure::Capacity)?;
        self.frame = frame;
        self.hopper = self.prepare_hopper()?;
        self.ready = true;
        Ok(())
    }

    pub(super) fn ready(&self) -> bool {
        self.ready
    }

    pub(super) fn materialize(
        &mut self,
        id: ActionId,
        lifecycle: Option<ActionId>,
        hopper: Option<ActionId>,
        transition: Option<ActionId>,
        entities: Option<&mut pumpkin_cluster::entity_origin::EntityOriginAllocator>,
    ) -> Result<(Vec<Action>, ActorActionDraft, VehicleControllerFrame), ApplyFailure> {
        if !self.ready {
            return Err(ApplyFailure::Capacity);
        }
        let mut draft = if let Some(draft) = &self.draft {
            draft.clone()
        } else {
            ActorActionDraft::new_entity(
                self.snapshot.as_ref(),
                FrozenEntityIssueAuthority {
                    target: Target::Chunk {
                        key: self.chunk,
                        part: ChunkPart::Entity(self.body.id),
                        inventory: None,
                    },
                    birth: self.body.birth.clone(),
                    producer: self.body.owner,
                    controlled_owner: self.body.owner,
                    effective_tick: self.tick,
                    next_sequence: id.sequence,
                },
                self.tick,
                4096,
                16384,
            )
            .map_err(|_| ApplyFailure::InvalidAction)?
        };
        let frame = self.frame.clone();
        if let Some(terminal) = &self.terminal {
            let action = terminal.materialize(&mut self.snapshot, &draft, id, entities)?;
            draft
                .issue(self.snapshot.as_ref(), action)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            return Ok((
                vec![
                    draft
                        .actions()
                        .last()
                        .cloned()
                        .ok_or(ApplyFailure::InvalidAction)?,
                ],
                draft,
                frame,
            ));
        }
        let action = frame
            .control_action(id, &self.body, self.chunk)
            .ok_or(ApplyFailure::InvalidAction)?;
        draft
            .issue(self.snapshot.as_ref(), action)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let mut actions = vec![
            draft
                .actions()
                .last()
                .cloned()
                .ok_or(ApplyFailure::InvalidAction)?,
        ];
        let lifecycle = pumpkin_cluster::vehicle_tick::VehicleTickIntent::capture_activated(
            &draft.view(self.snapshot.as_ref()),
            &self.body,
            self.chunk,
            lifecycle.ok_or(ApplyFailure::InvalidAction)?,
            self.tick,
            self.activation.clone(),
        )?;
        draft
            .issue(self.snapshot.as_ref(), lifecycle)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        actions.push(
            draft
                .actions()
                .last()
                .cloned()
                .ok_or(ApplyFailure::InvalidAction)?,
        );
        if let Some(mut action) = self.hopper.clone() {
            action.id = hopper.ok_or(ApplyFailure::InvalidAction)?;
            draft
                .issue(self.snapshot.as_ref(), action)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            actions.push(
                draft
                    .actions()
                    .last()
                    .cloned()
                    .ok_or(ApplyFailure::InvalidAction)?,
            );
        }
        if let Some(id) = transition {
            let action = pumpkin_cluster::entity_transition::capture(
                &self.body,
                self.chunk,
                frame.effective_spatial.clone(),
                self.tick,
                id,
            )?;
            draft
                .issue(self.snapshot.as_ref(), action)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            actions.push(
                draft
                    .actions()
                    .last()
                    .cloned()
                    .ok_or(ApplyFailure::InvalidAction)?,
            );
        }
        Ok((actions, draft, frame))
    }

    pub(super) fn actor(&self) -> (ActorId, u64) {
        (
            ActorId::Entity(self.body.id),
            u64::from(self.body.id.generation),
        )
    }

    pub(super) fn action_count(&self) -> u32 {
        if self.terminal.is_some() {
            return 1;
        }
        2 + u32::from(self.has_hopper()) + u32::from(self.transitions())
    }

    pub(super) fn sample(
        &self,
        effective: &VehicleControllerFrame,
        cache: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
    ) -> pumpkin_cluster::item_spatial::ItemSpatialSample {
        pumpkin_cluster::item_spatial::ItemSpatialSample {
            id: self.body.id,
            birth: self.body.birth.clone(),
            epoch: self.body.spatial_epoch,
            issuer: self.body.owner,
            tick: self.tick,
            sequence: cache
                .get(self.body.id)
                .map_or(pumpkin_cluster::identity::PlayerSeq(0), |prior| {
                    pumpkin_cluster::identity::PlayerSeq(prior.sequence.0.wrapping_add(1))
                }),
            spatial: effective.effective_spatial.clone(),
            head_yaw: effective.effective_spatial.yaw,
        }
    }
}

pub(super) fn emit_frame(
    frame: &mut pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<FrozenVehicleControl>,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    capacity: usize,
    mut entities: Option<&mut pumpkin_cluster::entity_origin::EntityOriginAllocator>,
) -> Result<Vec<(Vec<Action>, ActorActionDraft, VehicleControllerFrame)>, ApplyFailure> {
    let mut output = Vec::new();
    let mut issued = 0usize;
    let tick = frame.tick;
    for job in frame.remaining_mut().iter_mut().take(capacity) {
        if !job.ready() {
            break;
        }
        let (actor, incarnation) = job.actor();
        let count = job.action_count();
        if issued.saturating_add(count as usize) > capacity {
            break;
        }
        let Some(reservation) = clock.reserve(actor, incarnation, tick, count) else {
            break;
        };
        match job.materialize(
            reservation.id(0, 14).unwrap(),
            (count >= 2).then(|| reservation.id(1, 15).unwrap()),
            job.has_hopper().then(|| reservation.id(2, 16).unwrap()),
            job.transitions()
                .then(|| reservation.id(2 + u32::from(job.has_hopper()), 8).unwrap()),
            entities.as_deref_mut(),
        ) {
            Ok(page) => {
                issued += count as usize;
                output.push(page);
            }
            Err(reason) => {
                clock.release_unused(reservation);
                if output.is_empty() {
                    return Err(reason);
                }
                break;
            }
        }
    }
    Ok(output)
}

impl FrozenVehicleControl {
    fn hopper_items(
        &self,
        view: &impl ActionState,
        position: [f64; 3],
    ) -> Result<Vec<pumpkin_cluster::item_contact::ContactObservation>, ApplyFailure> {
        use pumpkin_cluster::actor_occupancy::OccupancyIdentity;
        let minimum = [position[0] - 1.6, position[2] - 1.6]
            .map(|value| (value.floor() as i32).div_euclid(16));
        let maximum = [position[0] + 1.6, position[2] + 1.6]
            .map(|value| (value.floor() as i32).div_euclid(16));
        let mut items = std::collections::BTreeMap::new();
        for x in minimum[0]..=maximum[0] {
            for z in minimum[1]..=maximum[1] {
                let witness = self
                    .snapshot
                    .capture_actor_occupancy(ChunkKey::new(self.chunk.world, x, z), Vec::new())?;
                for pose in witness.poses {
                    let cut = pumpkin_cluster::actor_occupancy::ActorOccupancyCut {
                        position: pose.position,
                        sample_tick: pose.sample_tick,
                        simulation_sequence: pumpkin_cluster::identity::PlayerSeq(
                            pose.simulation_sequence,
                        ),
                    };
                    let entry = pose.entry;
                    let (ActorId::Entity(id), OccupancyIdentity::Entity(birth)) =
                        (entry.actor, &entry.identity)
                    else {
                        continue;
                    };
                    let target = entry.body_target();
                    let Some(pumpkin_cluster::canonical_action::Cell {
                        value: Value::Entity(body),
                        ..
                    }) = view.read(TruthView::Local, target)
                    else {
                        return Err(ApplyFailure::Condition(target));
                    };
                    if !matches!(body.payload, EntityPayload::Item(_)) {
                        continue;
                    }
                    if body.birth != *birth
                        || body.owner != entry.owner
                        || body.spatial_epoch != entry.spatial_epoch
                        || cut.sample_tick.is_after(self.tick)
                        || !cut.valid()
                    {
                        return Err(ApplyFailure::Condition(target));
                    }
                    if !matches!(&body.payload, EntityPayload::Item(item) if item.health_milli > 0 && item.count > 0)
                    {
                        continue;
                    }
                    let mut spatial = body.spatial.clone();
                    spatial.position = cut.position;
                    let half_width = [
                        entry.shape.offsets[0][0],
                        entry.shape.offsets[0][2],
                        entry.shape.offsets[1][0],
                        entry.shape.offsets[1][2],
                    ]
                    .into_iter()
                    .map(f64::from_bits)
                    .map(f64::abs)
                    .fold(0.0_f64, f64::max);
                    let height = f64::from_bits(entry.shape.offsets[1][1])
                        - f64::from_bits(entry.shape.offsets[0][1]);
                    let contact = pumpkin_cluster::item_contact::ContactObservation::entity(
                        &body,
                        entry.association,
                        &spatial,
                        half_width,
                        height,
                        self.tick,
                        cut.simulation_sequence,
                    )
                    .ok_or(ApplyFailure::InvalidAction)?;
                    if items
                        .insert(id, contact.clone())
                        .is_some_and(|previous| previous != contact)
                    {
                        return Err(ApplyFailure::Condition(target));
                    }
                }
            }
        }
        Ok(items.into_values().collect())
    }

    fn prepare_hopper(&self) -> Result<Option<Action>, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{
            InventoryDestinationPolicy, InventoryId, InventoryReplayCandidate,
        };
        use pumpkin_cluster::entity_body::VehicleKind;
        use pumpkin_cluster::item_contact::{ContactObservation, PickupContact};
        if self.frame.state.kind != VehicleKind::HopperMinecart || !self.frame.state.hopper_enabled
        {
            return Ok(None);
        }
        if let Some(activation) = &self.activation {
            if let Some(cell) = self.snapshot.read(TruthView::Local, activation.block) {
                if let Value::Block(block) = cell.value {
                    if super::cluster_vehicle_world::activator(block)? == Some(true) {
                        return Ok(None);
                    }
                }
            }
        }
        let picker = ContactObservation::entity(
            &self.body,
            self.chunk,
            &self.frame.effective_spatial,
            0.49,
            0.7,
            self.tick,
            pumpkin_cluster::identity::PlayerSeq(0),
        )
        .ok_or(ApplyFailure::InvalidAction)?;
        let empty = ActorActionDraft::new(
            ActorId::Entity(self.body.id),
            self.body.id.generation,
            self.tick,
            4096,
            16384,
        );
        let view = self
            .draft
            .as_ref()
            .unwrap_or(&empty)
            .view(self.snapshot.as_ref());
        let position = self.frame.effective_spatial.position.map(f64::from_bits);
        let above = pumpkin_cluster::protocol::BlockPos {
            x: position[0].floor() as i32,
            y: (position[1] + 1.5).floor() as i32,
            z: position[2].floor() as i32,
        };
        let above_chunk = ChunkKey::new(
            self.chunk.world,
            above.x.div_euclid(16),
            above.z.div_euclid(16),
        );
        let above_target = Target::Chunk {
            key: above_chunk,
            part: ChunkPart::Block(above),
            inventory: None,
        };
        let above_cell = view
            .read(TruthView::Local, above_target)
            .ok_or(ApplyFailure::Capacity)?;
        let Value::Block(state) = &above_cell.value else {
            return Err(ApplyFailure::Type(above_target));
        };
        let state = pumpkin_data::BlockStateId::new(*state).ok_or(ApplyFailure::InvalidAction)?;
        if super::cluster_inventory_menu::block_inventory_policy(state).is_some() {
            return Ok(None);
        }
        let items = self.hopper_items(&view, position)?;
        for item in &items {
            let contact = PickupContact {
                picker: picker.clone(),
                item: item.clone(),
                producer: self.body.owner,
                tick: self.tick,
            };
            if !contact.valid_hopper() {
                continue;
            }
            let Some(cell) = view.read(TruthView::Local, item.target()) else {
                continue;
            };
            let Value::Entity(body) = &cell.value else {
                continue;
            };
            let EntityPayload::Item(source) = &body.payload else {
                continue;
            };
            let source = pumpkin_cluster::inventory::InventoryCell {
                loc: pumpkin_cluster::inventory::InvLoc::new(0, 0),
                item: source.item,
                count: source.count,
                nbt: source.components.clone(),
            };
            let destinations = (0..5)
                .map(|slot| {
                    let target = Target::Chunk {
                        key: self.chunk,
                        part: ChunkPart::Entity(self.body.id),
                        inventory: Some(InventoryId::Entity { inv: 0, slot }),
                    };
                    Ok(InventoryReplayCandidate {
                        target,
                        max_stack: view.inventory_limit(&source, target)?,
                    })
                })
                .collect::<Result<Vec<_>, ApplyFailure>>()?;
            let id = ActionId {
                actor: ActorId::Entity(self.body.id),
                incarnation: self.body.id.generation,
                sequence: 0,
                family: 16,
            };
            if let Ok(mut action) = pumpkin_cluster::item_lifecycle::capture_pickup(
                &view,
                id,
                contact,
                destinations,
                0,
                InventoryDestinationPolicy::MergeThenEmpty {
                    merge_only: Vec::new(),
                },
                None,
                Vec::new(),
            ) && !action.effects.is_empty()
            {
                let guard = pumpkin_cluster::canonical_action::Condition {
                    target: above_target,
                    expected: pumpkin_cluster::canonical_action::Expected::Exact(
                        above_cell.clone(),
                    ),
                };
                action.conditions.push(guard.clone());
                action.counterfactual_conditions.push(above_target);
                let stakeholder =
                    pumpkin_cluster::canonical_action::Stakeholder::Chunk(above_chunk);
                if !action.stakeholders.contains(&stakeholder) {
                    action.stakeholders.push(stakeholder);
                }
                action.validate().map_err(|_| ApplyFailure::InvalidAction)?;
                return Ok(Some(action));
            }
        }
        Ok(None)
    }

    pub(super) fn has_hopper(&self) -> bool {
        self.hopper.is_some()
    }
    pub(super) fn transitions(&self) -> bool {
        self.terminal.is_none() && !self.frame.effective_spatial.belongs_to(self.chunk)
    }
    pub(super) fn publishes_sample(&self) -> bool {
        self.terminal.is_none() && !self.transitions()
    }
}

pub(super) fn capture_dismount_proposal(
    server: &super::Server,
    player: &crate::entity::player::Player,
    tick: pumpkin_cluster::time::TickStamp,
    sequence: &mut u16,
) -> Option<pumpkin_cluster::canonical_action::ActionProposal> {
    use pumpkin_cluster::canonical_action::{ActionProposal, ClaimedAction, ClientOrigin};
    use pumpkin_cluster::item_contact::ContactIdentity;
    use std::sync::atomic::Ordering;
    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
    let publication = server.cluster_actor_living.load();
    let avatar = publication
        .get(&ActorId::Player(lease.gid))?
        .avatar
        .as_ref()?;
    let mount = avatar.mount.as_ref()?;
    let ContactIdentity::Entity { id, birth } = mount else {
        return None;
    };
    let vehicles = server.cluster_vehicle_views.load();
    let directory = server.cluster_canonical_entities.load();
    let (vehicle_chunk, seat) = if let Some(vehicle) = vehicles
        .vehicles
        .get(id)
        .filter(|vehicle| &vehicle.birth == birth)
    {
        let seat = vehicle.passengers.iter().find_map(|(seat, passenger)| {
            (*passenger == ContactIdentity::Player(lease)).then_some(*seat)
        })?;
        (vehicle.chunk, seat)
    } else {
        let entity = directory.entity_by_id(*id)?;
        let shell = entity
            .cast_any()
            .downcast_ref::<super::cluster_entity_registry::CanonicalEntityShell>()?;
        let body = &shell.body;
        if &body.birth != birth
            || !matches!(&body.payload, EntityPayload::Living(living)
            if living.mob.as_ref().is_some_and(|mob| matches!(mob.species, pumpkin_cluster::mob_continuation::MobSpecies::Horse { .. })))
        {
            return None;
        }
        let position = body.spatial.position.map(f64::from_bits);
        let chunk = ChunkKey::new(
            body.spatial.world,
            (position[0].floor() as i32).div_euclid(16),
            (position[2].floor() as i32).div_euclid(16),
        );
        let seat = body.passengers.iter().find_map(|(seat, passenger)| {
            (*passenger == ContactIdentity::Player(lease)).then_some(*seat)
        })?;
        (chunk, seat)
    };
    let spatial = server.cluster_actor_spatial.load();
    let source = spatial.get(&lease.gid)?;
    let chunk = source.truth.local.chunk;
    if chunk.world != vehicle_chunk.world {
        return None;
    }
    *sequence = sequence.wrapping_add(1);
    Some(ActionProposal {
        command: None,
        id: ActionId {
            actor: ActorId::Player(lease.gid),
            incarnation: lease.incarnation,
            sequence: *sequence,
            family: 7,
        },
        tick,
        lease: Some(lease),
        client_origin: ClientOrigin::None,
        screen_generation: 0,
        claimed_targets: vec![
            Target::Chunk {
                key: chunk,
                part: ChunkPart::Player(lease.gid),
                inventory: None,
            },
            Target::Chunk {
                key: vehicle_chunk,
                part: ChunkPart::Entity(*id),
                inventory: None,
            },
        ],
        claim: ClaimedAction::Dismount(pumpkin_cluster::vehicle_dismount::DismountClaim {
            owner_install_generation: player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
            source_epoch: avatar.spatial_epoch,
            mount: mount.clone(),
            vehicle_chunk,
            seat,
        }),
    })
}

pub(super) fn due_tnt(body: &EntityBody, tick: pumpkin_cluster::time::TickStamp) -> bool {
    matches!(&body.payload, EntityPayload::Vehicle(vehicle)
        if vehicle.kind == pumpkin_cluster::entity_body::VehicleKind::TntMinecart
            && vehicle.fuse_ticks.is_some_and(|fuse| fuse <= 1)
            && vehicle.lifecycle_tick.is_none_or(|previous| tick.is_after(previous)))
}

pub(super) fn simulate_uncontrolled(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    draft: &ActorActionDraft,
    body: &EntityBody,
    tick: pumpkin_cluster::time::TickStamp,
    loaded: &std::collections::BTreeSet<ChunkKey>,
) -> Result<VehicleControllerFrame, ApplyFailure> {
    let mut frame = VehicleControllerFrame::idle(body, tick).ok_or(ApplyFailure::InvalidAction)?;
    let world =
        super::cluster_vehicle_world::VehicleWorldView::new(snapshot, draft, &body.spatial, loaded)
            .with_rules(
                snapshot
                    .mob_rules(body.spatial.world)
                    .ok_or(ApplyFailure::InvalidAction)?,
            );
    frame.simulate(&world).map_err(|_| ApplyFailure::Capacity)?;
    Ok(frame)
}

pub(super) fn passenger_attachment(
    parent: &EntityBody,
    spatial: &pumpkin_cluster::entity_body::EntitySpatial,
    seat: u8,
) -> Option<[f64; 3]> {
    let EntityPayload::Vehicle(vehicle) = &parent.payload else {
        return None;
    };
    let mut position =
        pumpkin_cluster::vehicle_control::passenger_position(vehicle.kind, seat, spatial)?;
    let raft = pumpkin_data::entity::EntityType::from_raw(parent.kind)?
        .resource_name
        .ends_with("_raft");
    if raft {
        position[1] += 0.35;
    }
    Some(position)
}
