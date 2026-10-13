use crate::canonical_action::*;
use crate::canonical_inventory::InventorySlotBinding;
use crate::entity_body::{EntityBody, EntityPayload, VehicleKind};
use crate::inventory::InventoryCell;
use crate::protocol::ChunkKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleCargoDrop {
    pub binding: InventorySlotBinding,
    pub before: InventoryCell,
    pub drop: Option<EntityBody>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleTerminalIntent {
    pub chunk: ChunkKey,
    pub rules: crate::world_rules::WorldRulesWitness,
    pub do_entity_drops: bool,
    pub body: EntityBody,
    pub cargo: Vec<VehicleCargoDrop>,
    pub vehicle_drop: Option<EntityBody>,
    pub detach: crate::actor_relation::OwnerDetachPlan,
}

impl VehicleTerminalIntent {
    pub fn target(&self) -> Target {
        Target::Chunk {
            key: self.chunk,
            part: ChunkPart::Entity(self.body.id),
            inventory: None,
        }
    }

    pub fn matches(&self, cell: Option<&Cell>) -> bool {
        matches!(cell, Some(Cell { value: Value::Entity(body), .. }) if body.as_ref() == &self.body)
            && matches!(&self.body.payload, EntityPayload::Vehicle(vehicle) if vehicle.damage_milli > 40_000)
    }

    pub fn valid(&self, id: ActionId, tick: crate::time::TickStamp) -> bool {
        let EntityPayload::Vehicle(vehicle) = &self.body.payload else {
            return false;
        };
        let slots = match vehicle.kind {
            VehicleKind::ChestBoat | VehicleKind::ChestMinecart => 27,
            VehicleKind::HopperMinecart => 5,
            _ => 0,
        };
        if id.actor != crate::actor::ActorId::Entity(self.body.id)
            || id.incarnation != self.body.id.generation
            || !self.body.belongs_to(self.body.id, self.chunk)
            || !self.body.valid_at(tick)
            || vehicle.damage_milli <= 40_000
            || self.cargo.len() != slots
        {
            return false;
        }
        let mut targets = std::collections::BTreeSet::new();
        for (slot, cargo) in self.cargo.iter().enumerate() {
            if !cargo.binding.is_valid()
                || !matches!(cargo.binding.target, Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: Some(InventoryId::Entity { inv: 0, slot: index }) }
                    if key == self.chunk && entity == self.body.id && usize::from(index) == slot)
                || cargo.before.loc != cargo.binding.slot.loc
                || !targets.insert(cargo.binding.target)
                || cargo.drop.is_some()
                    != (!cargo.before.is_empty() && self.do_entity_drops && vehicle.drop_loot)
            {
                return false;
            }
        }
        for body in self
            .vehicle_drop
            .iter()
            .chain(self.cargo.iter().filter_map(|cargo| cargo.drop.as_ref()))
        {
            let target = Target::Chunk {
                key: self.chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            };
            if !body.belongs_to(body.id, self.chunk)
                || !body.valid_at(tick)
                || body.birth.action != (ActionRef { id, tick })
                || body.birth.source.server != body.id.origin
                || body.birth.source.generation != body.id.generation
                || !matches!(&body.payload, EntityPayload::Item(item) if item.age == 0 && item.pickup_delay <= 40)
                || !targets.insert(target)
                || body.id == self.body.id
            {
                return false;
            }
        }
        self.valid_detach(id, tick)
    }

    pub fn outputs(&self) -> Vec<Effect> {
        let mut effects = Vec::new();
        for cargo in &self.cargo {
            effects.push(Effect::Put {
                target: cargo.binding.target,
                value: Value::Inventory(InventoryCell::empty(cargo.before.loc)),
            });
        }
        for body in self
            .vehicle_drop
            .iter()
            .chain(self.cargo.iter().filter_map(|cargo| cargo.drop.as_ref()))
        {
            effects.push(Effect::Put {
                target: Target::Chunk {
                    key: self.chunk,
                    part: ChunkPart::Entity(body.id),
                    inventory: None,
                },
                value: Value::Entity((body.clone()).into()),
            });
        }
        effects.extend(self.detach.effects.iter().cloned());
        effects.push(Effect::RetireEntity {
            intent: self.retirement(),
        });
        effects
    }
}

impl VehicleTerminalIntent {
    fn valid_detach(&self, id: ActionId, tick: crate::time::TickStamp) -> bool {
        use crate::actor_relation::ActorRelation;
        let exact = |relation, identity| {
            let target = Target::Chunk {
                key: self.chunk,
                part: ChunkPart::ActorRelation {
                    actor: crate::actor::ActorId::Entity(self.body.id),
                    relation,
                },
                inventory: None,
            };
            self.detach.conditions.iter().any(|condition| condition.target == target
                && matches!(&condition.expected, Expected::Exact(Cell { value: Value::ActorRelation(actual), .. }) if *actual == identity))
        };
        if !exact(ActorRelation::Mount, self.body.mount.clone())
            || !(0..32).all(|seat| {
                exact(
                    ActorRelation::Passenger(seat),
                    self.body.passengers.get(&seat).cloned(),
                )
            })
            || !self.detach.conditions.contains(&Condition {
                target: self.target(),
                expected: Expected::EntityBirthOwned {
                    birth: self.body.birth.clone(),
                    owner: self.body.owner,
                },
            })
            || !self.detach.conditions.contains(&Condition {
                target: self.target(),
                expected: Expected::SpatialEpochEquals(self.body.spatial_epoch),
            })
        {
            return false;
        }
        if self.body.mount.is_none() && self.body.passengers.is_empty() {
            return self.detach.effects.is_empty()
                && self.detach.leases.is_empty()
                && self.detach.chunks == vec![self.chunk];
        }
        let action = Action {
            edit_authority: None,
            id,
            tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(self.chunk),
            player_leases: self.detach.leases.clone(),
            player_chunks: Vec::new(),
            stakeholders: self
                .detach
                .chunks
                .iter()
                .copied()
                .map(Stakeholder::Chunk)
                .collect(),
            dependencies: Vec::new(),
            conditions: self.detach.conditions.clone(),
            effects: self.detach.effects.clone(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        crate::actor_relation::owner_detach_all(&action)
    }

    pub fn retirement(&self) -> crate::canonical_entity_retirement::EntityRetirementIntent {
        crate::canonical_entity_retirement::EntityRetirementIntent {
            target: self.target(),
            birth: self.body.birth.clone(),
            owner: self.body.owner,
            kind: self.body.kind,
            inventory: self
                .cargo
                .iter()
                .map(|cargo| cargo.binding.clone())
                .collect(),
            disposition: crate::canonical_entity_retirement::EntityInventoryDisposition::Destroy,
            living_death: None,
        }
    }

    pub fn targets(&self) -> Vec<Target> {
        let mut targets = vec![self.target()];
        targets.extend(self.cargo.iter().map(|cargo| cargo.binding.target));
        targets.extend(self.outputs().iter().flat_map(Effect::targets));
        targets.extend(
            self.detach
                .conditions
                .iter()
                .map(|condition| condition.target),
        );
        targets.sort_unstable();
        targets.dedup();
        targets
    }
}

impl VehicleTerminalIntent {
    pub fn guards_valid(&self, action: &Action) -> bool {
        if !self.valid(action.id, action.tick)
            || action.class != ActionClass::Undoable
            || action.kind != ActionKind::Actor
            || action.chunk_transition
            || action.actor_chunk != Some(self.chunk)
            || action.semantic_intent.is_some()
            || action.effects != self.outputs()
            || !action.counterfactual_conditions.is_empty()
            || !action.counterfactual_effects.is_empty()
        {
            return false;
        }
        let has = |target, expected: &Expected| {
            action
                .conditions
                .iter()
                .any(|condition| condition.target == target && &condition.expected == expected)
        };
        if !has(
            self.target(),
            &Expected::ActorControlled {
                birth: self.body.birth.clone(),
                issuer: self.body.owner,
            },
        ) || !has(
            self.target(),
            &Expected::SpatialEpochEquals(self.body.spatial_epoch),
        ) {
            return false;
        }
        for cargo in &self.cargo {
            if !action.conditions.iter().any(|condition| condition.target == cargo.binding.target
                && matches!(&condition.expected, Expected::Exact(Cell { value: Value::Inventory(cell), .. }) if cell == &cargo.before))
            { return false; }
        }
        for body in self
            .vehicle_drop
            .iter()
            .chain(self.cargo.iter().filter_map(|cargo| cargo.drop.as_ref()))
        {
            if !has(
                Target::Chunk {
                    key: self.chunk,
                    part: ChunkPart::Entity(body.id),
                    inventory: None,
                },
                &Expected::Missing,
            ) {
                return false;
            }
        }
        if !self
            .detach
            .conditions
            .iter()
            .all(|condition| action.conditions.contains(condition))
        {
            return false;
        }
        action.player_leases == self.detach.leases
    }

    pub fn capture<S: ActionState>(
        &self,
        state: &S,
        id: ActionId,
        tick: crate::time::TickStamp,
    ) -> Result<Action, ApplyFailure> {
        if !self.valid(id, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut conditions = vec![
            Condition {
                target: self.target(),
                expected: Expected::VehicleTerminal(Box::new(self.clone())),
            },
            Condition {
                target: self.target(),
                expected: Expected::ActorControlled {
                    birth: self.body.birth.clone(),
                    issuer: self.body.owner,
                },
            },
            Condition {
                target: self.target(),
                expected: Expected::SpatialEpochEquals(self.body.spatial_epoch),
            },
        ];
        for cargo in &self.cargo {
            let before = state
                .read(TruthView::Local, cargo.binding.target)
                .ok_or(ApplyFailure::Condition(cargo.binding.target))?;
            if before.value != Value::Inventory(cargo.before.clone()) {
                return Err(ApplyFailure::Condition(cargo.binding.target));
            }
            conditions.push(Condition {
                target: cargo.binding.target,
                expected: Expected::Exact(before),
            });
        }
        for body in self
            .vehicle_drop
            .iter()
            .chain(self.cargo.iter().filter_map(|cargo| cargo.drop.as_ref()))
        {
            conditions.push(Condition {
                target: Target::Chunk {
                    key: self.chunk,
                    part: ChunkPart::Entity(body.id),
                    inventory: None,
                },
                expected: Expected::Missing,
            });
        }
        for condition in &self.detach.conditions {
            if !conditions.contains(condition) {
                conditions.push(condition.clone());
            }
        }
        let mut dependencies = conditions
            .iter()
            .filter_map(|condition| match condition.expected {
                Expected::SpatialEpochEquals(SpatialEpoch::Action(reference)) => Some(reference),
                _ => None,
            })
            .collect::<Vec<_>>();
        dependencies.sort_unstable();
        dependencies.dedup();
        let player_leases = self.detach.leases.clone();
        let mut stakeholders = self
            .targets()
            .iter()
            .map(|target| target.stakeholder())
            .collect::<Vec<_>>();
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let action = Action {
            edit_authority: None,
            id,
            tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(self.chunk),
            player_leases,
            player_chunks: Vec::new(),
            stakeholders,
            dependencies,
            conditions,
            effects: self.outputs(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate()?;
        Ok(action)
    }
}

impl VehicleTerminalIntent {
    pub fn validate_context<S: ActionState>(
        &self,
        state: &S,
        tick: crate::time::TickStamp,
    ) -> Result<std::collections::BTreeMap<(u16, Vec<u8>), i64>, ApplyFailure> {
        let rules = state.world_rules(self.rules, tick)?;
        if rules.rules.world != self.chunk.world || rules.do_entity_drops != self.do_entity_drops {
            return Err(ApplyFailure::InvalidAction);
        }
        let EntityPayload::Vehicle(vehicle) = &self.body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        let drops = self.do_entity_drops && vehicle.drop_loot;
        let mut balance = std::collections::BTreeMap::new();
        if drops != self.vehicle_drop.is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(template) = &self.vehicle_drop {
            let item = state.vehicle_drop_item(&self.body)?;
            if state.dropped_item_body(template, &item)? != *template {
                return Err(ApplyFailure::InvalidAction);
            }
            *balance.entry((item.item, item.nbt)).or_default() += i64::from(item.count);
        }
        for cargo in &self.cargo {
            if let Some(template) = &cargo.drop {
                if state.dropped_item_body(template, &cargo.before)? != *template {
                    return Err(ApplyFailure::InvalidAction);
                }
            } else if !cargo.before.is_empty() {
                *balance
                    .entry((cargo.before.item, cargo.before.nbt.clone()))
                    .or_default() -= i64::from(cargo.before.count);
            }
        }
        let primary = state.scheduled_executor(self.chunk)?;
        for body in self
            .vehicle_drop
            .iter()
            .chain(self.cargo.iter().filter_map(|cargo| cargo.drop.as_ref()))
        {
            if body.owner != primary || body.birth.source != self.body.owner {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        Ok(balance)
    }
}

pub fn witness(action: &Action) -> Option<&VehicleTerminalIntent> {
    action
        .conditions
        .iter()
        .find_map(|condition| match &condition.expected {
            Expected::VehicleTerminal(intent) => Some(intent.as_ref()),
            _ => None,
        })
}

pub fn valid_terminal_action(action: &Action) -> bool {
    let witnesses = action
        .conditions
        .iter()
        .filter_map(|condition| match &condition.expected {
            Expected::VehicleTerminal(intent) => Some((condition.target, intent.as_ref())),
            _ => None,
        })
        .collect::<Vec<_>>();
    matches!(witnesses.as_slice(), [(target, intent)] if *target == intent.target() && intent.guards_valid(action))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::actor_relation::{ActorRelation, capture_owner_detach_from_participants};
    use crate::entity_body::{EntityBirth, EntitySpatial, VehicleState};
    use crate::identity::ServerId;
    use crate::protocol::{GlobalEntityId, WorldId};
    use crate::realm_completion::PeerGeneration;
    use crate::time::TickStamp;
    use crate::world_rules::{WorldRulesId, WorldRulesSnapshot, WorldRulesWitness};

    struct State {
        cells: OwnedStakeholderSet,
        rules: WorldRulesSnapshot,
    }
    fn cart_shape() -> crate::actor_occupancy::CollisionShape {
        crate::actor_occupancy::CollisionShape {
            offsets: [
                [-0.49_f64, 0.0, -0.49].map(f64::to_bits),
                [0.49_f64, 0.7, 0.49].map(f64::to_bits),
            ],
            blocks_building: true,
            hanging: false,
        }
    }
    impl ActionState for State {
        fn actor_occupancy_cut(
            &self,
            target: Target,
            value: &Value,
        ) -> Result<
            (
                crate::realm_completion::PeerGeneration,
                crate::actor_occupancy::ActorOccupancyCut,
            ),
            ApplyFailure,
        > {
            let Value::Entity(body) = value else {
                return Err(ApplyFailure::Type(target));
            };
            Ok((
                body.owner,
                crate::actor_occupancy::ActorOccupancyCut {
                    position: body.spatial.position,
                    sample_tick: TickStamp(1),
                    simulation_sequence: crate::identity::PlayerSeq(0),
                },
            ))
        }
        fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
            Ok(Some(state == 1))
        }
        fn actor_collision_shape(
            &self,
            _: Target,
            _: &Value,
        ) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
            Ok(cart_shape())
        }
        fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
            if let Some(body) = crate::derived_living::actor_body_target(target) {
                return self
                    .cells
                    .read(view, body)
                    .and_then(|body| crate::derived_living::actor_facet_cell(&body, target));
            }
            self.cells.read(view, target)
        }
        fn commit(
            &mut self,
            context: ActionContext,
            view: TruthView,
            writes: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            let mut physical = writes
                .iter()
                .filter(|write| crate::derived_living::actor_body_target(write.target).is_none())
                .cloned()
                .collect::<Vec<_>>();
            let bodies = writes
                .iter()
                .filter_map(|write| crate::derived_living::actor_body_target(write.target))
                .collect::<std::collections::BTreeSet<_>>();
            for target in bodies {
                let before = self.read(view, target);
                let after =
                    crate::derived_living::project_body_writes(target, before.as_ref(), writes)?;
                if let Some(existing) = physical.iter_mut().find(|write| write.target == target) {
                    existing.after = after;
                } else {
                    physical.push(AppliedWrite {
                        target,
                        before,
                        after,
                    });
                }
            }
            self.cells.commit(context, view, &physical)
        }
        fn entity_inventory_layout(
            &self,
            _: &EntityBody,
        ) -> Result<Vec<crate::inventory::InvLoc>, ApplyFailure> {
            Ok(Vec::new())
        }
        fn scheduled_executor(&self, _: ChunkKey) -> Result<PeerGeneration, ApplyFailure> {
            Ok(self.rules.rules.issuer)
        }
        fn world_rules(
            &self,
            witness: WorldRulesWitness,
            tick: TickStamp,
        ) -> Result<WorldRulesSnapshot, ApplyFailure> {
            if witness.rules != self.rules.rules || witness.slot as u16 != tick.0 {
                return Err(ApplyFailure::InvalidAction);
            }
            Ok(self.rules.clone())
        }
    }

    fn fixture() -> (State, VehicleTerminalIntent, ActionId) {
        let peer = PeerGeneration {
            server: ServerId(1),
            generation: 1,
        };
        let world = WorldId::new(0, 1);
        let chunk = ChunkKey::new(world, 0, 0);
        let entity = GlobalEntityId {
            origin: peer.server,
            local_id: 1,
            generation: 1,
        };
        let id = ActionId {
            actor: ActorId::Entity(entity),
            incarnation: 1,
            sequence: 2,
            family: 15,
        };
        let birth = ActionRef {
            id: ActionId {
                sequence: 1,
                family: 1,
                ..id
            },
            tick: TickStamp(1),
        };
        let mut vehicle = VehicleState::new(VehicleKind::Minecart);
        vehicle.damage_milli = 40_001;
        vehicle.drop_loot = false;
        let body = EntityBody {
            mount: None,
            tags: Default::default(),
            tags_revision: 0,
            relation_versions: Default::default(),
            passengers: Default::default(),
            id: entity,
            birth: EntityBirth {
                action: birth,
                source: peer,
                uuid: uuid::Uuid::from_u128(1),
            },
            owner: peer,
            kind: 1,
            spatial: EntitySpatial {
                world,
                position: [1.0_f64, 64.0, 1.0].map(f64::to_bits),
                velocity_micro: [0; 3],
                yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                on_ground: true,
            },
            spatial_epoch: SpatialEpoch::Action(birth),
            payload: EntityPayload::Vehicle(vehicle),
        };
        let rules = WorldRulesSnapshot {
            advance_time: true,
            advance_weather: true,
            players_sleeping_percentage: 100,
            rules: WorldRulesId {
                world,
                issuer: peer,
                epoch: 1,
            },
            configuration: crate::world_rules::WorldConfigurationVersion { issuer: peer, revision: 1, from_slot: 0 },
            from_slot: 0,
            keep_inventory: false,
            do_entity_drops: true,
            do_mob_loot: true,
            mob_griefing: true,
            tnt_explodes: true,
            random_tick_speed: 3,
            natural_regeneration: true,
            do_mob_spawning: true,
            water_source_conversion: true,
            lava_source_conversion: false,
            fire_spread_radius_around_player: 128,
            min_y: -64,
            height: 384,
            fire_damage: true,
            drowning_damage: true,
            freeze_damage: true,
            fall_damage: true,
            difficulty: 2,
        };
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(entity),
            inventory: None,
        };
        let mut cells = std::collections::BTreeMap::from([(
            target,
            Cell {
                revision: 1,
                value: Value::Entity((body.clone()).into()),
            },
        )]);
        for relation in
            std::iter::once(ActorRelation::Mount).chain((0..32).map(ActorRelation::Passenger))
        {
            cells.insert(
                Target::Chunk {
                    key: chunk,
                    part: ChunkPart::ActorRelation {
                        actor: ActorId::Entity(entity),
                        relation,
                    },
                    inventory: None,
                },
                Cell {
                    revision: 1,
                    value: Value::ActorRelation(None),
                },
            );
        }
        cells.insert(
            Target::Chunk {
                key: chunk,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            },
            Cell {
                revision: 1,
                value: Value::ActorOccupancy(
                    crate::actor_occupancy::ActorOccupancyRoot::from_entries(vec![
                        crate::actor_occupancy::ActorOccupancyEntry {
                            association: chunk,
                            owner: body.owner,
                            cut: crate::actor_occupancy::ActorOccupancyCut {
                                position: body.spatial.position,
                                sample_tick: TickStamp(1),
                                simulation_sequence: crate::identity::PlayerSeq(0),
                            },
                            actor: ActorId::Entity(entity),
                            identity: crate::actor_occupancy::OccupancyIdentity::Entity(
                                body.birth.clone(),
                            ),
                            spatial_epoch: body.spatial_epoch,
                            shape: cart_shape(),
                        },
                    ])
                    .unwrap(),
                ),
            },
        );
        let state = State {
            cells: OwnedStakeholderSet {
                local: cells.clone(),
                ground: cells,
            },
            rules,
        };
        let detach = capture_owner_detach_from_participants(&state, &body, chunk, &[]).unwrap();
        let terminal = VehicleTerminalIntent {
            chunk,
            rules: WorldRulesWitness {
                rules: state.rules.rules,
                slot: 2,
            },
            do_entity_drops: true,
            body,
            cargo: Vec::new(),
            vehicle_drop: None,
            detach,
        };
        (state, terminal, id)
    }

    #[test]
    fn creative_terminal_retires_all_relations_in_local_and_ground_and_undo_restores() {
        let (mut state, terminal, id) = fixture();
        let action = terminal.capture(&state, id, TickStamp(2)).unwrap();
        assert!(matches!(
            action.effects.last(),
            Some(Effect::RetireEntity { .. })
        ));
        assert_eq!(terminal.retirement().relation_targets().len(), 33);
        for view in [TruthView::Local, TruthView::Ground] {
            let applied = apply_action(&mut state, &action, view).unwrap();
            assert!(state.read(view, terminal.target()).is_none());
            assert!(
                terminal
                    .retirement()
                    .relation_targets()
                    .iter()
                    .all(|target| state.read(view, *target).is_none())
            );
            undo_action(&mut state, &applied).unwrap();
            assert_eq!(
                state.read(view, terminal.target()).unwrap().value,
                Value::Entity(terminal.body.clone().into())
            );
            assert!(
                terminal
                    .retirement()
                    .relation_targets()
                    .iter()
                    .all(|target| state.read(view, *target).is_some())
            );
            apply_action(&mut state, &action, view).unwrap();
            assert!(state.read(view, terminal.target()).is_none());
        }
    }

    #[test]
    fn terminal_rejects_changed_cause_or_incomplete_relation_manifest() {
        let (state, terminal, id) = fixture();
        let mut action = terminal.capture(&state, id, TickStamp(2)).unwrap();
        let own_mount = terminal.retirement().relation_targets()[0];
        action
            .conditions
            .retain(|condition| condition.target != own_mount);
        assert!(action.validate().is_err());
        let mut changed = terminal.clone();
        let EntityPayload::Vehicle(vehicle) = &mut changed.body.payload else {
            unreachable!()
        };
        vehicle.drop_loot = true;
        assert!(changed.validate_context(&state, TickStamp(2)).is_err());
    }
    #[test]
    fn powered_activator_detaches_exact_cross_chunk_entity_rider_and_undo_restores() {
        use crate::actor_relation::ActorRelationParticipant;
        use crate::item_contact::ContactIdentity;
        use crate::vehicle_tick::{VehicleActivation, VehicleTickIntent};
        let (mut state, terminal, id) = fixture();
        let chunk = terminal.chunk;
        let mut parent = terminal.body;
        let EntityPayload::Vehicle(vehicle) = &mut parent.payload else {
            unreachable!()
        };
        vehicle.damage_milli = 0;
        let child_chunk = ChunkKey::new(chunk.world, 1, 0);
        let mut child = parent.clone();
        child.id = GlobalEntityId {
            origin: ServerId(2),
            local_id: 2,
            generation: 1,
        };
        child.owner = PeerGeneration {
            server: ServerId(2),
            generation: 1,
        };
        child.birth.source = child.owner;
        child.birth.uuid = uuid::Uuid::from_u128(2);
        child.birth.action.id.actor = ActorId::Entity(child.id);
        child.spatial_epoch = SpatialEpoch::Action(child.birth.action);
        child.spatial.position[0] = 17.0_f64.to_bits();
        child.mount = Some(ContactIdentity::Entity {
            id: parent.id,
            birth: parent.birth.clone(),
        });
        let child_identity = ContactIdentity::Entity {
            id: child.id,
            birth: child.birth.clone(),
        };
        parent.passengers.insert(0, child_identity.clone());
        let parent_target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(parent.id),
            inventory: None,
        };
        let child_target = Target::Chunk {
            key: child_chunk,
            part: ChunkPart::Entity(child.id),
            inventory: None,
        };
        state.cells.local.insert(
            parent_target,
            Cell {
                revision: 1,
                value: Value::Entity((parent.clone()).into()),
            },
        );
        state.cells.local.insert(
            child_target,
            Cell {
                revision: 1,
                value: Value::Entity((child.clone()).into()),
            },
        );
        for (body, key) in [(&parent, chunk), (&child, child_chunk)] {
            for relation in
                std::iter::once(ActorRelation::Mount).chain((0..32).map(ActorRelation::Passenger))
            {
                let identity = match relation {
                    ActorRelation::Mount => body.mount.clone(),
                    ActorRelation::Passenger(seat) => body.passengers.get(&seat).cloned(),
                };
                state.cells.local.insert(
                    Target::Chunk {
                        key,
                        part: ChunkPart::ActorRelation {
                            actor: ActorId::Entity(body.id),
                            relation,
                        },
                        inventory: None,
                    },
                    Cell {
                        revision: 1,
                        value: Value::ActorRelation(identity),
                    },
                );
            }
        }
        for property in crate::vehicle_property::VehicleProperty::ALL {
            let value =
                crate::vehicle_property::read(&Value::Entity((parent.clone()).into()), property)
                    .unwrap();
            state.cells.local.insert(
                Target::Chunk {
                    key: chunk,
                    part: ChunkPart::VehicleProperty {
                        actor: parent.id,
                        property,
                    },
                    inventory: None,
                },
                Cell {
                    revision: 1,
                    value: Value::VehicleProperty(value),
                },
            );
        }
        let block = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(crate::protocol::BlockPos { x: 1, y: 64, z: 1 }),
            inventory: None,
        };
        state.cells.local.insert(
            block,
            Cell {
                revision: 1,
                value: Value::Block(1),
            },
        );
        state.cells.local.insert(
            Target::Chunk {
                key: child_chunk,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            },
            Cell {
                revision: 1,
                value: Value::ActorOccupancy(
                    crate::actor_occupancy::ActorOccupancyRoot::from_entries(vec![
                        crate::actor_occupancy::ActorOccupancyEntry {
                            association: child_chunk,
                            owner: child.owner,
                            cut: crate::actor_occupancy::ActorOccupancyCut {
                                position: child.spatial.position,
                                sample_tick: TickStamp(1),
                                simulation_sequence: crate::identity::PlayerSeq(0),
                            },
                            actor: ActorId::Entity(child.id),
                            identity: crate::actor_occupancy::OccupancyIdentity::Entity(
                                child.birth.clone(),
                            ),
                            spatial_epoch: child.spatial_epoch,
                            shape: cart_shape(),
                        },
                    ])
                    .unwrap(),
                ),
            },
        );
        state.cells.ground = state.cells.local.clone();
        let plan = capture_owner_detach_from_participants(
            &state,
            &parent,
            chunk,
            &[ActorRelationParticipant::from_entity(&child, child_chunk).unwrap()],
        )
        .unwrap();
        let activation = VehicleActivation {
            detach: Some(plan),
            block,
            kind: VehicleKind::Minecart,
            position: parent.spatial.position,
        };
        let action = VehicleTickIntent::capture_activated(
            &state,
            &parent,
            chunk,
            id,
            TickStamp(2),
            Some(activation),
        )
        .unwrap();
        assert!(crate::vehicle_tick::permits_activator_detach(&action));
        let mount = Target::Chunk {
            key: child_chunk,
            part: ChunkPart::ActorRelation {
                actor: ActorId::Entity(child.id),
                relation: ActorRelation::Mount,
            },
            inventory: None,
        };
        let seat = Target::Chunk {
            key: chunk,
            part: ChunkPart::ActorRelation {
                actor: ActorId::Entity(parent.id),
                relation: ActorRelation::Passenger(0),
            },
            inventory: None,
        };
        for view in [TruthView::Local, TruthView::Ground] {
            let applied = apply_action(&mut state, &action, view).unwrap();
            assert_eq!(
                state.read(view, mount).unwrap().value,
                Value::ActorRelation(None)
            );
            assert_eq!(
                state.read(view, seat).unwrap().value,
                Value::ActorRelation(None)
            );
            assert!(
                matches!(state.read(view,parent_target).unwrap().value,Value::Entity(body) if body.passengers.is_empty())
            );
            assert!(
                matches!(state.read(view,child_target).unwrap().value,Value::Entity(body) if body.mount.is_none())
            );
            undo_action(&mut state, &applied).unwrap();
            assert_eq!(
                state.read(view, seat).unwrap().value,
                Value::ActorRelation(Some(child_identity.clone()))
            );
        }
        let Some(SemanticIntent::VehicleTick(intent)) = &action.semantic_intent else {
            unreachable!()
        };
        let mut values = intent
            .targets()
            .into_iter()
            .map(|target| (target, state.read(TruthView::Local, target).unwrap()))
            .collect::<std::collections::BTreeMap<_, _>>();
        values.get_mut(&block).unwrap().value = Value::Block(0);
        assert!(
            !intent
                .derive_with(&state, TickStamp(2), &values)
                .unwrap()
                .iter()
                .any(|effect| matches!(
                    effect,
                    Effect::Put {
                        value: Value::ActorRelation(_),
                        ..
                    }
                ))
        );
        let mut graph = crate::action_graph::TickActionGraph::default();
        graph.insert(action.clone()).unwrap();
        let mut anchors = action
            .targets()
            .into_iter()
            .chain(action.counterfactual_conditions.iter().copied())
            .chain(action.counterfactual_effects.iter().copied())
            .map(|target| (target, state.read(TruthView::Local, target)))
            .collect::<std::collections::BTreeMap<_, _>>();
        for stakeholder in &action.stakeholders {
            if let Stakeholder::Chunk(key) = stakeholder {
                let target = Target::Chunk {
                    key: *key,
                    part: ChunkPart::ActorOccupancy,
                    inventory: None,
                };
                anchors.insert(target, state.read(TruthView::Local, target));
            }
        }
        anchors.insert(
            block,
            Some(Cell {
                revision: 1,
                value: Value::Block(0),
            }),
        );
        let solved = crate::action_solver::solve_component_with_context(
            &graph,
            &std::collections::BTreeSet::from([action.id]),
            &anchors,
            29,
            |_| Some(true),
            &state,
        )
        .unwrap();
        assert!(
            matches!(&solved.variants[&action.id],crate::action_solver::SolvedVariant::Applied {replayed:true,effects,..}
            if !effects.iter().any(|effect| matches!(effect,Effect::Put {value:Value::ActorRelation(_),..})))
        );
        let mut broken = action.clone();
        broken
            .conditions
            .retain(|condition| condition.target != mount);
        assert!(broken.validate().is_err());
    }
}

#[cfg(test)]
mod tnt_cart_tests {
    use crate::actor::ActorId;
    use crate::canonical_action::{ActionId, ActionRef, SpatialEpoch};
    use crate::entity_body::{
        EntityBirth, EntityBody, EntityPayload, EntitySpatial, VehicleKind, VehicleState,
    };
    use crate::identity::ServerId;
    use crate::protocol::{GlobalEntityId, WorldId};
    use crate::realm_completion::PeerGeneration;
    use crate::time::TickStamp;

    #[test]
    fn due_cart_uses_original_wrapped_tick_and_effective_velocity_for_terminal_power() {
        let peer = PeerGeneration {
            server: ServerId(1),
            generation: 1,
        };
        let id = GlobalEntityId {
            origin: peer.server,
            local_id: 2,
            generation: 1,
        };
        let birth = ActionRef {
            tick: TickStamp(u16::MAX - 2),
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: 1,
                sequence: 1,
                family: 1,
            },
        };
        let mut state = VehicleState::new(VehicleKind::TntMinecart);
        state.lifecycle_tick = Some(TickStamp(u16::MAX));
        state.fuse_ticks = Some(1);
        let mut body = EntityBody {
            mount: None,
            tags: Default::default(),
            tags_revision: 0,
            relation_versions: Default::default(),
            passengers: Default::default(),
            id,
            birth: EntityBirth {
                action: birth,
                source: peer,
                uuid: uuid::Uuid::from_u128(2),
            },
            owner: peer,
            kind: 1,
            spatial: EntitySpatial {
                world: WorldId::new(0, 1),
                position: [1.0_f64, 64.0, 1.0].map(f64::to_bits),
                velocity_micro: [0; 3],
                yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                on_ground: true,
            },
            spatial_epoch: SpatialEpoch::Action(birth),
            payload: EntityPayload::Vehicle(state),
        };
        let tick = TickStamp(0);
        let plan = crate::object_tick::capture_body(&body, tick, &body.spatial, -64, 320).unwrap();
        assert_eq!(plan.elapsed, 1);
        assert!(
            matches!(plan.terminal,Some(crate::object_tick::ObjectTerminal::Explosion { power, igniter:None }) if f32::from_bits(power)==4.0)
        );
        let mut effective = body.spatial.clone();
        effective.velocity_micro = [400_000, 0, 0];
        let moving = crate::object_tick::capture_body(&body, tick, &effective, -64, 320).unwrap();
        assert_eq!(
            moving,
            crate::object_tick::capture_body(&body, tick, &effective, -64, 320).unwrap()
        );
        assert!(
            matches!(moving.terminal,Some(crate::object_tick::ObjectTerminal::Explosion { power, .. }) if f32::from_bits(power)>=4.0 && f32::from_bits(power)<=4.6)
        );
        let EntityPayload::Vehicle(state) = &mut body.payload else {
            unreachable!()
        };
        state.lifecycle_tick = Some(tick);
        assert!(crate::object_tick::capture_body(&body, tick, &effective, -64, 320).is_none());
        let EntityPayload::Vehicle(state) = &mut body.payload else {
            unreachable!()
        };
        state.lifecycle_tick = Some(TickStamp(u16::MAX));
        state.fuse_ticks = Some(2);
        assert!(crate::object_tick::capture_body(&body, tick, &effective, -64, 320).is_none());
    }
}
