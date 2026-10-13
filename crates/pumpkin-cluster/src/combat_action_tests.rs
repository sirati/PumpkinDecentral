use super::*;
use crate::canonical_action::{
    ActionRef, AppliedAction, AppliedWrite, ChunkPart, LivePlayerLease, PlayerKey, SpatialEpoch,
    TruthView,
};
use crate::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
use crate::item_action::{ActorCombatState, ActorFoodState, PlayerAvatarState};
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::protocol::{ChunkKey, WorldId};
use crate::realm_completion::PeerGeneration;
use std::collections::BTreeMap;

pub(crate) fn fixture(slot: u16) -> (LivePlayerLease, ActorLivingState, ContactObservation) {
    let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(slot));
    let lease = LivePlayerLease {
        key: PlayerKey(uuid::Uuid::from_u128(u128::from(slot))),
        gid: actor,
        incarnation: 9,
    };
    let world = WorldId::new(0, 0);
    let living = ActorLivingState {
        permanent_attribute_modifiers: Default::default(),
        menu: Default::default(),
        environment: Default::default(),
        tags: Default::default(),
        tags_revision: 0,
        advancements: Default::default(),
        stand: None,
        mob: None,
        merchant: None,
        simulation_tick: None,
        effects: Vec::new(),
        hidden_effects: BTreeMap::new(),
        property_versions: std::collections::BTreeMap::new(),
        food: ActorFoodState {
            exhaustion_milli: 0,
            tick_timer: 0,
            actor: ActorId::Player(actor),
            food: 20,
            saturation_milli: 5_000,
            revision: 0,
        },
        combat: ActorCombatState {
            actor: ActorId::Player(actor),
            revision: 0,
            health_milli: 20_000,
            absorption_milli: 0,
            velocity_micro: [0; 3],
            timing: Default::default(),
            effective_attributes: Default::default(),
            equipment: Default::default(),
            status: Default::default(),
        },
        use_state: None,
        cooldowns: BTreeMap::new(),
        avatar: Some(PlayerAvatarState {
            stance: Default::default(),
            mount: None,
            relation_versions: Default::default(),
            experience: Default::default(),
            statistics: Default::default(),
            presentation: Default::default(),
            spatial_epoch: SpatialEpoch::Admission {
                lease: LivePlayerLease {
                    key: lease.key,
                    gid: actor,
                    incarnation: lease.incarnation,
                },
                admitted_at: TickStamp(0),
            },
            world,
            position: [1.0_f64.to_bits(), 80.0_f64.to_bits(), 1.0_f64.to_bits()],
            yaw: 0.0_f32.to_bits(),
            head_yaw: 0.0_f32.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
            movement_tick: TickStamp(0),
            movement_sequence: PlayerSeq(0),
            game_mode: 0,
            fall_distance: 0.0_f32.to_bits(),
            on_fire: false,
        }),
    };
    let contact = ContactObservation {
        identity: ContactIdentity::Player(lease),
        owner: PeerGeneration {
            server: ServerId(1),
            generation: 1,
        },
        epoch: living.avatar.as_ref().unwrap().spatial_epoch,
        world,
        chunk: ChunkKey::new(world, 0, 0),
        position: [
            f64::from(slot).to_bits(),
            80.0_f64.to_bits(),
            1.0_f64.to_bits(),
        ],
        half_width: 0.3_f64.to_bits(),
        height: 1.8_f64.to_bits(),
        simulation_tick: TickStamp(100),
        observed_sequence: PlayerSeq(0),
    };
    (lease, living, contact)
}

fn attack(
    attacker: ContactObservation,
    victim: ContactObservation,
    sweeping: bool,
) -> AttackIntent {
    AttackIntent {
        actor: attacker.actor(),
        source: attacker.target(),
        target: victim.target(),
        tick: attacker.simulation_tick,
        contact: AttackContact {
            producer: attacker.owner,
            tick: attacker.simulation_tick,
            attacker,
            victim,
            reach: 8.0_f64.to_bits(),
            eye_height: 1.6_f64.to_bits(),
            victim_yaw: 0,
            victim_on_ground: true,
        },
        pose: AttackPose {
            sprinting: false,
            on_ground: true,
            fall_distance: 0,
            bedrock: false,
            yaw: 0,
            knockback_enabled: false,
        },
        attributes: CombatAttributeIds {
            attack_damage: 1,
            attack_speed: 2,
            armor: 3,
            toughness: 4,
            knockback_resistance: 5,
        },
        target_undead: false,
        target_arthropod: false,
        sweeping,
    }
}

#[test]
fn sweeping_damage_uses_resolved_parent_charge_after_attack_clock_reset() {
    let (_, mut source, attacker) = fixture(1);
    let (_, mut victim, contact) = fixture(2);
    for body in [&mut source, &mut victim] {
        body.combat.effective_attributes = BTreeMap::from([
            (1, 8.0_f64.to_bits()),
            (2, 4.0_f64.to_bits()),
            (3, 0.0_f64.to_bits()),
            (4, 0.0_f64.to_bits()),
            (5, 0.0_f64.to_bits()),
        ]);
    }
    source.combat.equipment.sword = true;
    source.combat.equipment.sweeping_edge = 3;
    source.combat.timing.last_attack_tick = Some(TickStamp(100));
    let sweep = attack(attacker, contact, true);
    let values = BTreeMap::from([
        (
            sweep.source,
            Cell {
                revision: 0,
                value: Value::Living((source).into()),
            },
        ),
        (
            sweep.target,
            Cell {
                revision: 0,
                value: Value::Living((victim).into()),
            },
        ),
    ]);
    let charged = sweep
        .damage_with_protection(&values, true, Some(1.0_f64.to_bits()))
        .unwrap();
    let discharged = sweep.damage(&values).unwrap();
    assert_eq!(charged.damage_milli, 7000);
    assert!(discharged.damage_milli < charged.damage_milli);
    assert_eq!(
        sweep
            .damage_with_protection(&values, false, Some(1.0_f64.to_bits()))
            .unwrap()
            .damage_milli,
        charged.damage_milli
    );
}

#[test]
fn damaging_parent_requires_exact_victim_health_or_absorption_loss() {
    let (_, _, attacker) = fixture(1);
    let (_, _, victim) = fixture(2);
    let intent = CombatIntent {
        attacks: vec![attack(attacker.clone(), victim.clone(), false)],
        ..Default::default()
    };
    let id = ActionId {
        actor: attacker.actor(),
        incarnation: 9,
        sequence: 1,
        family: 7,
    };
    let action = Action {
        id,
        tick: TickStamp(100),
        class: ActionClass::Replayable,
        chunk_transition: false,
        edit_authority: None,
        actor_chunk: Some(attacker.chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: Vec::new(),
        dependencies: Vec::new(),
        conditions: Vec::new(),
        effects: Vec::new(),
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: Some(SemanticIntent::Combat(intent)),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::Combat,
        presentation: Vec::new(),
        client_origin: ClientOrigin::None,
    };
    let mut applied = AppliedAction {
        id,
        tick: action.tick,
        actor_chunk: action.actor_chunk,
        view: TruthView::Local,
        writes: Vec::new(),
    };
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let write = |property, before, after| AppliedWrite {
        target: Target::Chunk {
            key: victim.chunk,
            part: ChunkPart::LivingProperty {
                actor: victim.actor(),
                property,
            },
            inventory: None,
        },
        before: Some(Cell {
            revision: 0,
            value: Value::LivingProperty(V::Unsigned(before)),
        }),
        after: Some(Cell {
            revision: 1,
            value: Value::LivingProperty(V::Unsigned(after)),
        }),
    };
    applied.writes = vec![write(P::Health, 20000, 20000)];
    assert!(!damaging_parent(&action, &applied, &victim.identity));
    applied.writes = vec![write(P::Health, 20000, 19000)];
    assert!(damaging_parent(&action, &applied, &victim.identity));
    applied.writes = vec![write(P::Absorption, 2000, 1000)];
    assert!(damaging_parent(&action, &applied, &victim.identity));
    applied.writes = vec![write(P::FireTicks, 0, 1000)];
    assert!(!damaging_parent(&action, &applied, &victim.identity));
    assert!(!damaging_parent(&action, &applied, &attacker.identity));
    applied.tick = TickStamp(101);
    assert!(!damaging_parent(&action, &applied, &victim.identity));
}

#[test]
fn inline_sweep_requires_health_or_absorption_damage_from_primary() {
    let (_, mut source, attacker) = fixture(1);
    let (_, mut victim, primary_contact) = fixture(2);
    let (_, mut secondary, sweep_contact) = fixture(3);
    for body in [&mut source, &mut victim, &mut secondary] {
        body.combat.effective_attributes = BTreeMap::from([
            (1, 8.0_f64.to_bits()),
            (2, 4.0_f64.to_bits()),
            (3, 0.0_f64.to_bits()),
            (4, 0.0_f64.to_bits()),
            (5, 0.0_f64.to_bits()),
        ]);
    }
    source.combat.equipment.sword = true;
    source.combat.equipment.sweeping_edge = 3;
    victim.combat.timing.last_damage_tick = Some(TickStamp(100));
    victim.combat.timing.last_damage_milli = 10000;
    let primary = attack(attacker.clone(), primary_contact, false);
    let sweep = attack(attacker, sweep_contact, true);
    let intent = CombatIntent {
        attacks: vec![primary.clone(), sweep.clone()],
        ..Default::default()
    };
    let mut values = BTreeMap::from([
        (
            primary.source,
            Cell {
                revision: 0,
                value: Value::Living((source).into()),
            },
        ),
        (
            primary.target,
            Cell {
                revision: 0,
                value: Value::Living((victim).into()),
            },
        ),
        (
            sweep.target,
            Cell {
                revision: 0,
                value: Value::Living((secondary).into()),
            },
        ),
    ]);
    let has_sweep_damage = |effects: Vec<Effect>| {
        effects.into_iter().any(|effect| matches!(effect,
        Effect::Put {target:Target::Chunk {part:ChunkPart::LivingProperty {actor,property:crate::living_property::LivingProperty::Health},..},value:Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(after))} if actor == sweep.contact.victim.actor() && after < 20000))
    };
    assert!(!has_sweep_damage(intent.derive_effects(&values).unwrap()));
    let Value::Living(victim) = &mut values.get_mut(&primary.target).unwrap().value else {
        unreachable!()
    };
    victim.combat.timing.last_damage_tick = None;
    victim.combat.absorption_milli = 20000;
    assert!(has_sweep_damage(intent.derive_effects(&values).unwrap()));
}

pub(crate) fn respawn_shape(living: &ActorLivingState) -> crate::actor_occupancy::CollisionShape {
    crate::actor_occupancy::CollisionShape {
        hanging: false,
        offsets: [
            [-0.3_f64, 0.0, -0.3].map(f64::to_bits),
            [0.3_f64, 1.8, 0.3].map(f64::to_bits),
        ],
        blocks_building: living.combat.health_milli != 0,
    }
}

pub(crate) struct RespawnState {
    pub(crate) truth: crate::canonical_action::OwnedStakeholderSet,
    pub(crate) removed_modifier: CombatAttributeModifier,
}

impl crate::canonical_action::ActionState for RespawnState {
    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        self.truth
            .read(view, target)
            .or_else(|| {
                let (body, property) = crate::living_property::body_target(target)?;
                crate::derived_living::facet_cell(&self.truth.read(view, body)?, property)
            })
            .or_else(|| {
                let (body, relation) = crate::actor_relation::body_target(target)?;
                crate::actor_relation::cell(&self.truth.read(view, body)?, relation)
            })
    }
    fn actor_occupancy_cut(
        &self, target: Target, value: &Value,
    ) -> Result<(PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        let Value::Living(living) = value else { return Err(ApplyFailure::Type(target)); };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
        Ok((PeerGeneration { server: ServerId(1), generation: 1 },
            crate::actor_occupancy::ActorOccupancyCut { position: avatar.position,
                sample_tick: avatar.movement_tick, simulation_sequence: avatar.movement_sequence }))
    }
    fn actor_collision_shape(
        &self,
        target: Target,
        value: &Value,
    ) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
        let Value::Living(living) = value else {
            return Err(ApplyFailure::Type(target));
        };
        Ok(respawn_shape(living))
    }
    fn status_effect_modifiers(
        &self,
        _: &crate::active_effects::ActiveEffectSet,
    ) -> Result<Vec<CombatAttributeModifier>, ApplyFailure> {
        Ok(vec![self.removed_modifier.clone()])
    }
    fn commit(
        &mut self,
        context: crate::canonical_action::ActionContext,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        let bodies = writes
            .iter()
            .filter_map(|write| crate::derived_living::actor_body_target(write.target))
            .collect::<std::collections::BTreeSet<_>>();
        let mut physical = writes.iter().filter(|write| crate::derived_living::actor_body_target(write.target).is_none()).cloned().collect::<Vec<_>>();
        for target in bodies {
            let before = self.truth.read(view, target);
            let after =
                crate::derived_living::project_body_writes(target, before.as_ref(), writes)?;
            physical.push(AppliedWrite {
                target,
                before,
                after,
            });
        }
        self.truth.commit(context, view, &physical)
    }
}

#[test]
fn respawn_uses_symbolic_maximum_after_removing_status_modifiers_in_both_truths() {
    use crate::canonical_action::{ActionState, OwnedStakeholderSet, apply_action, undo_action};
    let (lease, mut dead, contact) = fixture(1);
    let death = ActionRef {
        tick: TickStamp(99),
        id: ActionId {
            actor: contact.actor(),
            incarnation: 9,
            sequence: 1,
            family: 7,
        },
    };
    dead.combat.health_milli = 0;
    dead.combat.timing.death_cause = Some(death);
    dead.combat.timing.death_inventory_processed = true;
    dead.combat
        .effective_attributes
        .insert(6, 30.0_f64.to_bits());
    let removed_modifier = CombatAttributeModifier {
        attribute: 6,
        slot: 0,
        id: "health_boost".into(),
        amount: 10.0_f64.to_bits(),
        operation: CombatModifierOperation::Add,
    };
    dead.combat.equipment.modifiers = vec![
        removed_modifier.clone(),
        CombatAttributeModifier {
            attribute: 6,
            slot: 1,
            id: "equipment".into(),
            amount: 5.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        },
    ];
    dead.permanent_attribute_modifiers.insert(
        6,
        vec![CombatAttributeModifier {
            attribute: 6,
            slot: 0,
            id: "permanent_health".into(),
            amount: 7.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        }],
    );
    dead.environment.air_ticks = -10;
    dead.environment.frozen_ticks = 140;
    let original = Cell {
        revision: 4,
        value: Value::Living((dead.clone()).into()),
    };
    let mut truth = OwnedStakeholderSet::default();
    truth.local.insert(contact.target(), original.clone());
    truth.ground.insert(contact.target(), original.clone());
    let root_target = Target::Chunk {
        key: contact.chunk,
        part: ChunkPart::ActorOccupancy,
        inventory: None,
    };
    let root = Cell {
        revision: 8,
        value: Value::ActorOccupancy(
            crate::actor_occupancy::ActorOccupancyRoot::from_entries(vec![
                crate::actor_occupancy::ActorOccupancyEntry {
                    association: contact.chunk,
                    owner: contact.owner,
                    cut: crate::actor_occupancy::ActorOccupancyCut {
                        position: contact.position,
                        sample_tick: TickStamp(0),
                        simulation_sequence: contact.observed_sequence,
                    },
                    actor: contact.actor(),
                    identity: crate::actor_occupancy::OccupancyIdentity::Player(lease),
                    spatial_epoch: contact.epoch,
                    shape: respawn_shape(&dead),
                },
            ])
            .unwrap(),
        ),
    };
    truth.local.insert(root_target, root.clone());
    truth.ground.insert(root_target, root.clone());
    let mut state = RespawnState {
        truth,
        removed_modifier,
    };
    let action = Action::from_player_respawn(
        ActionId {
            actor: contact.actor(),
            incarnation: 9,
            sequence: 2,
            family: 7,
        },
        lease,
        TickStamp(100),
        contact.chunk,
        contact.chunk,
        &dead,
        dead.avatar.clone().unwrap(),
        6,
    )
    .unwrap();
    for view in [TruthView::Local, TruthView::Ground] {
        let applied = apply_action(&mut state, &action, view).unwrap();
        let Value::Living(living) = state.read(view, contact.target()).unwrap().value else {
            unreachable!()
        };
        assert_eq!(living.combat.health_milli, 42000);
        let Value::ActorOccupancy(occupancy) = state.read(view, root_target).unwrap().value else {
            unreachable!()
        };
        assert!(occupancy.entries[0].shape.blocks_building);
        assert_eq!(
            occupancy.entries[0].spatial_epoch,
            SpatialEpoch::Action(ActionRef {
                tick: action.tick,
                id: action.id
            })
        );
        assert_eq!(living.environment, Default::default());
        assert_eq!(living.combat.equipment.modifiers.len(), 1);
        assert_eq!(living.combat.timing.death_cause, None);
        undo_action(&mut state, &applied).unwrap();
        assert_eq!(state.read(view, contact.target()), Some(original.clone()));
        assert_eq!(state.read(view, root_target), Some(root.clone()));
    }
}

#[test]
fn blocked_primary_keeps_reserved_sweep_as_noop_in_private_draft() {
    use crate::canonical_action::{ActionState, OwnedStakeholderSet};
    let (_, mut source, attacker) = fixture(1);
    let (_, mut victim, primary_contact) = fixture(2);
    let (_, mut secondary, sweep_contact) = fixture(3);
    for body in [&mut source, &mut victim, &mut secondary] {
        body.combat.effective_attributes = BTreeMap::from([
            (1, 8.0_f64.to_bits()),
            (2, 4.0_f64.to_bits()),
            (3, 0.0_f64.to_bits()),
            (4, 0.0_f64.to_bits()),
            (5, 0.0_f64.to_bits()),
        ]);
    }
    source.combat.equipment.sword = true;
    source.combat.equipment.sweeping_edge = 3;
    victim.combat.timing.last_damage_tick = Some(TickStamp(100));
    victim.combat.timing.last_damage_milli = 10000;
    let primary = attack(attacker.clone(), primary_contact.clone(), false);
    let sweep = attack(attacker.clone(), sweep_contact.clone(), true);
    let mut truth = OwnedStakeholderSet::default();
    for (target, body) in [
        (primary.source, source),
        (primary.target, victim),
        (sweep.target, secondary),
    ] {
        truth.local.insert(
            target,
            Cell {
                revision: 0,
                value: Value::Living((body).into()),
            },
        );
    }
    let state = RespawnState {
        truth,
        removed_modifier: CombatAttributeModifier {
            attribute: 6,
            slot: 0,
            id: "unused".into(),
            amount: 0,
            operation: CombatModifierOperation::Add,
        },
    };
    let id = ActionId {
        actor: attacker.actor(),
        incarnation: 9,
        sequence: 1,
        family: 7,
    };
    let intent = CombatIntent {
        attacks: vec![primary],
        ..Default::default()
    };
    let values = intent
        .targets()
        .filter_map(|target| {
            state
                .read(TruthView::Local, target)
                .map(|cell| (target, cell))
        })
        .collect();
    let parent = intent
        .capture_with(&state, id, attacker.chunk, &values, Vec::new())
        .unwrap();
    let mut draft = crate::actor_action_scope::ActorActionDraft::new(
        id.actor,
        id.incarnation,
        attacker.simulation_tick,
        10,
        256,
    );
    draft.issue(&state, parent).unwrap();
    let view = draft.view(&state);
    assert_eq!(
        view.combat_parent_damaged(
            ActionRef {
                id,
                tick: TickStamp(100)
            },
            &primary_contact.identity
        ),
        Some(false)
    );
    let dependent = CombatIntent {
        attacks: vec![sweep],
        damage_parent: Some(DamagingDependency {
            parent: ActionRef {
                id,
                tick: TickStamp(100),
            },
            victim: primary_contact.identity,
        }),
        ..Default::default()
    };
    let values = dependent
        .targets()
        .filter_map(|target| {
            view.read(TruthView::Local, target)
                .map(|cell| (target, cell))
        })
        .collect();
    let action = dependent
        .capture_with(
            &view,
            ActionId { sequence: 2, ..id },
            attacker.chunk,
            &values,
            Vec::new(),
        )
        .unwrap();
    assert!(action.effects.is_empty());
    assert_eq!(
        action.dependencies,
        vec![ActionRef {
            id,
            tick: TickStamp(100)
        }]
    );
    draft.issue(&state, action).unwrap();
    assert_eq!(draft.actions().len(), 2);
    assert_eq!(
        draft
            .view(&state)
            .read(TruthView::Local, sweep_contact.target()),
        state.read(TruthView::Local, sweep_contact.target())
    );
}

#[test]
fn mob_melee_has_full_damage_without_player_attack_speed_attribute() {
    let (_, mut source, mut attacker) = fixture(1);
    let (_, mut victim, contact) = fixture(2);
    let id = crate::protocol::GlobalEntityId {
        origin: ServerId(1),
        local_id: 1,
        generation: 1,
    };
    let actor = ActorId::Entity(id);
    let reference = ActionRef {
        tick: TickStamp(0),
        id: ActionId {
            actor,
            incarnation: 1,
            sequence: 1,
            family: 8,
        },
    };
    let birth = crate::entity_body::EntityBirth {
        action: reference,
        source: attacker.owner,
        uuid: uuid::Uuid::from_u128(100),
    };
    attacker.identity = ContactIdentity::Entity {
        id,
        birth: birth.clone(),
    };
    attacker.epoch = SpatialEpoch::Action(reference);
    source.avatar = None;
    source.food.actor = actor;
    source.combat.actor = actor;
    source.combat.effective_attributes = BTreeMap::from([(1, 8.0_f64.to_bits())]);
    victim.combat.effective_attributes = BTreeMap::from([
        (3, 0.0_f64.to_bits()),
        (4, 0.0_f64.to_bits()),
        (5, 0.0_f64.to_bits()),
    ]);
    let body = crate::entity_body::EntityBody {
        mount: None,
        tags: Default::default(),
        tags_revision: 0,
        relation_versions: Default::default(),
        passengers: Default::default(),
        id,
        birth,
        owner: attacker.owner,
        kind: 1,
        spatial: crate::entity_body::EntitySpatial {
            world: attacker.world,
            position: attacker.position,
            velocity_micro: [0; 3],
            yaw: 0,
            pitch: 0,
            on_ground: true,
        },
        spatial_epoch: attacker.epoch,
        payload: EntityPayload::Living(source),
    };
    let attack = attack(attacker, contact, false);
    let values = BTreeMap::from([
        (
            attack.source,
            Cell {
                revision: 0,
                value: Value::Entity((body).into()),
            },
        ),
        (
            attack.target,
            Cell {
                revision: 0,
                value: Value::Living((victim).into()),
            },
        ),
    ]);
    let damage = attack.damage(&values).unwrap();
    assert_eq!(damage.damage_milli, 8000);
    assert_eq!(damage.kind, DamageKind::MobAttack);
}

#[test]
fn shield_blocks_only_front_after_delay_and_counts_blocked_damage() {
    use crate::inventory::{INV_OFFHAND, InvLoc, InventoryCell, InventorySlotRef, InventoryTarget};
    use crate::item_action::{ItemUseMode, ItemUseState};
    let (_, mut source, attacker) = fixture(1);
    let (lease, mut victim, contact) = fixture(2);
    for body in [&mut source, &mut victim] {
        body.combat.effective_attributes = BTreeMap::from([
            (1, 8.0_f64.to_bits()),
            (2, 4.0_f64.to_bits()),
            (3, 0.0_f64.to_bits()),
            (4, 0.0_f64.to_bits()),
            (5, 0.0_f64.to_bits()),
        ]);
    }
    victim.combat.equipment.shield = true;
    let loc = InvLoc::new(INV_OFFHAND, 0);
    victim.use_state = Some(ItemUseState {
        actor: contact.actor(),
        source: InventorySlotRef::new(InventoryTarget::Player(lease.gid), loc),
        item: InventoryCell {
            loc,
            item: 1,
            count: 1,
            nbt: Vec::new(),
        },
        started: TickStamp(90),
        mode: ItemUseMode::Block,
        duration_ticks: u32::MAX,
        cooldown_ticks: 0,
        has_remainder: false,
        blocking_delay_ticks: 5,
    });
    let mut attack = attack(attacker, contact, false);
    attack.contact.victim_yaw = 90.0_f32.to_bits();
    let values = BTreeMap::from([
        (
            attack.source,
            Cell {
                revision: 0,
                value: Value::Living((source).into()),
            },
        ),
        (
            attack.target,
            Cell {
                revision: 0,
                value: Value::Living((victim).into()),
            },
        ),
    ]);
    assert_eq!(attack.damage(&values).unwrap().damage_milli, 0);
    assert_eq!(attack.blocked_damage(&values, None).unwrap(), 8000);
    let statistic = Target::Chunk {
        key: attack.contact.victim.chunk,
        part: ChunkPart::LivingProperty {
            actor: attack.contact.victim.actor(),
            property: crate::living_property::LivingProperty::Statistic {
                category: 0,
                key: 29,
            },
        },
        inventory: None,
    };
    let intent = CombatIntent {
        attacks: vec![attack.clone()],
        statistics: vec![CombatStatisticIntent {
            target: statistic,
            damage_targets: vec![attack.target],
            kind: CombatStatisticKind::Blocked,
        }],
        ..Default::default()
    };
    assert!(intent.derive_effects(&values).unwrap().iter().any(|effect| matches!(effect,Effect::Put {target,value:Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(80))} if *target == statistic)));
    attack.contact.victim_yaw = (-90.0_f32).to_bits();
    assert_eq!(attack.blocked_damage(&values, None).unwrap(), 0);
    assert_eq!(attack.damage(&values).unwrap().damage_milli, 8000);
    attack.contact.victim_yaw = 90.0_f32.to_bits();
    attack.tick = TickStamp(92);
    attack.contact.tick = attack.tick;
    attack.contact.attacker.simulation_tick = attack.tick;
    attack.contact.victim.simulation_tick = attack.tick;
    assert_eq!(attack.blocked_damage(&values, None).unwrap(), 0);
    assert_eq!(attack.damage(&values).unwrap().damage_milli, 8000);
}

#[test]
fn owner_produced_hazard_keeps_genuine_last_attacker_for_death_credit() {
    let (_, mut living, contact) = fixture(1);
    let (_, _, previous) = fixture(2);
    living.combat.timing.last_attacker = Some(previous.actor());
    let mut damage = DamageIntent {
        kind: DamageKind::Fall,
        source: contact.actor(),
        target: contact.target(),
        tick: TickStamp(100),
        damage_milli: 1000,
        knockback_micro: [0; 3],
        bypass_hurt_cooldown: true,
        fire_ticks: 0,
        reduction: None,
        bypass_invulnerability: false,
    };
    assert!(damage.apply(&mut living).unwrap());
    assert_eq!(living.combat.timing.last_attacker, Some(previous.actor()));
    let (_, _, replacement) = fixture(3);
    damage.kind = DamageKind::PlayerAttack;
    damage.source = replacement.actor();
    assert!(damage.apply(&mut living).unwrap());
    assert_eq!(
        living.combat.timing.last_attacker,
        Some(replacement.actor())
    );
}

#[test]
fn bundled_sweep_keeps_every_frozen_victim_beyond_old_action_cap() {
    let (_, mut source, attacker) = fixture(1);
    source.combat.effective_attributes = BTreeMap::from([
        (1, 8.0_f64.to_bits()),
        (2, 4.0_f64.to_bits()),
        (3, 0.0_f64.to_bits()),
        (4, 0.0_f64.to_bits()),
        (5, 0.0_f64.to_bits()),
    ]);
    source.combat.equipment.sword = true;
    source.combat.equipment.sweeping_edge = 3;
    let mut values = BTreeMap::from([(
        attacker.target(),
        Cell {
            revision: 0,
            value: Value::Living((source).into()),
        },
    )]);
    let mut attacks = Vec::new();
    for slot in 2..=4098 {
        let (_, mut victim, mut contact) = fixture(slot);
        victim.combat.effective_attributes = BTreeMap::from([
            (3, 0.0_f64.to_bits()),
            (4, 0.0_f64.to_bits()),
            (5, 0.0_f64.to_bits()),
        ]);
        contact.position[0] = 2.0_f64.to_bits();
        values.insert(
            contact.target(),
            Cell {
                revision: 0,
                value: Value::Living((victim).into()),
            },
        );
        attacks.push(attack(attacker.clone(), contact, slot != 2));
    }
    let intent = CombatIntent {
        attacks,
        ..Default::default()
    };
    let id = ActionId {
        actor: attacker.actor(),
        incarnation: 9,
        sequence: 1,
        family: 7,
    };
    let started = std::time::Instant::now();
    let derived = intent
        .derive_with(
            &crate::canonical_action::OwnedStakeholderSet::default(),
            id,
            TickStamp(100),
            &values,
        )
        .unwrap();
    eprintln!(
        "dense derivation completed in {:?}, {} effects",
        started.elapsed(),
        derived.len()
    );
    drop(derived);
    let action = intent
        .capture_with(
            &crate::canonical_action::OwnedStakeholderSet::default(),
            id,
            attacker.chunk,
            &values,
            Vec::new(),
        )
        .unwrap();
    eprintln!("dense capture completed in {:?}", started.elapsed());
    action.validate_resource_budget().unwrap();
    eprintln!(
        "dense resource validation completed in {:?}",
        started.elapsed()
    );
    action.validate().unwrap();
    eprintln!(
        "dense static validation completed in {:?}",
        started.elapsed()
    );
    let Some(crate::canonical_action::SemanticIntent::Combat(bundled)) = &action.semantic_intent
    else {
        unreachable!()
    };
    assert_eq!(bundled.attacks.len(), 4097);
    assert_eq!(action.id, id);
    assert!(action.dependencies.is_empty());
    let effects = action.effects;

    let damaged =
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Put {
                    target:
                        Target::Chunk {
                            part:
                                ChunkPart::LivingProperty {
                                    actor,
                                    property: crate::living_property::LivingProperty::Health,
                                },
                            ..
                        },
                    value:
                        Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(
                            health,
                        )),
                } if *health < 20_000 => Some(*actor),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(damaged.len(), 4097);
    assert!(intent.resolution_targets().len() > 4096);
}

#[test]
fn fixed_respawn_requires_completed_inventory_fate_and_no_open_relation() {
    let (lease, mut dead, contact) = fixture(1);
    let id = ActionId {
        actor: contact.actor(),
        incarnation: 9,
        sequence: 2,
        family: 7,
    };
    let death = ActionRef {
        tick: TickStamp(99),
        id: ActionId { sequence: 1, ..id },
    };
    dead.combat.health_milli = 0;
    let construct = |body: &ActorLivingState| {
        Action::from_player_respawn(
            id,
            lease,
            TickStamp(100),
            contact.chunk,
            contact.chunk,
            body,
            body.avatar.clone().unwrap(),
            6,
        )
    };
    assert!(construct(&dead).is_none());
    dead.combat.timing.death_cause = Some(death);
    assert!(construct(&dead).is_none());
    dead.combat.timing.death_inventory_processed = true;
    let action = construct(&dead).unwrap();
    assert_eq!(action.dependencies, vec![death]);
    let mut unsafe_action = action.clone();
    unsafe_action.conditions.retain(|condition| {
        !matches!(
            condition.target,
            Target::Chunk {
                part: ChunkPart::LivingProperty {
                    property: crate::living_property::LivingProperty::DeathInventoryProcessed,
                    ..
                },
                ..
            }
        )
    });
    assert!(unsafe_action.validate().is_err());
    let mut mounted = dead.clone();
    mounted.avatar.as_mut().unwrap().mount = Some(contact.identity.clone());
    assert!(construct(&mounted).is_none());
    let mut open = dead;
    open.menu.session = 7;
    assert!(construct(&open).is_none());
}

#[test]
fn permanent_modifier_removal_preserves_shadowed_gear_in_both_truths_and_undo() {
    use crate::canonical_action::{ActionState, OwnedStakeholderSet, apply_action, undo_action};
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let (lease, mut body, contact) = fixture(1);
    body.combat
        .effective_attributes
        .insert(6, 30.0_f64.to_bits());
    let gear = CombatAttributeModifier {
        attribute: 6,
        slot: 1,
        id: "shared_modifier".into(),
        amount: 5.0_f64.to_bits(),
        operation: CombatModifierOperation::Add,
    };
    let permanent = CombatAttributeModifier {
        amount: 7.0_f64.to_bits(),
        ..gear.clone()
    };
    body.combat.equipment.modifiers.push(gear.clone());
    body.permanent_attribute_modifiers
        .insert(6, vec![permanent.clone()]);
    assert_eq!(body.attribute(6, 30.0), 37.0);
    let original = Cell {
        revision: 4,
        value: Value::Living((body.clone()).into()),
    };
    let mut truth = OwnedStakeholderSet::default();
    let root_target = Target::Chunk {
        key: contact.chunk,
        part: ChunkPart::ActorOccupancy,
        inventory: None,
    };
    let root = Cell {
        revision: 8,
        value: Value::ActorOccupancy(
            crate::actor_occupancy::ActorOccupancyRoot::from_entries(vec![
                crate::actor_occupancy::ActorOccupancyEntry {
                    association: contact.chunk,
                    owner: contact.owner,
                    cut: crate::actor_occupancy::ActorOccupancyCut {
                        position: contact.position,
                        sample_tick: TickStamp(0),
                        simulation_sequence: contact.observed_sequence,
                    },
                    actor: contact.actor(),
                    identity: crate::actor_occupancy::OccupancyIdentity::Player(lease),
                    spatial_epoch: contact.epoch,
                    shape: respawn_shape(&body),
                },
            ])
            .unwrap(),
        ),
    };
    for view in [&mut truth.local, &mut truth.ground] {
        view.insert(contact.target(), original.clone());
        view.insert(root_target, root.clone());
    }
    let mut state = RespawnState {
        truth,
        removed_modifier: permanent,
    };
    let id = ActionId {
        actor: contact.actor(),
        incarnation: lease.incarnation,
        sequence: 1,
        family: 7,
    };
    let mut action = Action::from_player_stance(
        id,
        lease,
        TickStamp(100),
        contact.chunk,
        body.avatar.as_ref().unwrap().stance,
         0.0_f32,
        contact.epoch,
    )
    .unwrap();
    action.class = crate::canonical_action::ActionClass::Undoable;
    action
        .conditions
        .retain(|guard| guard.target == contact.target());
    action.effects.clear();
    for (property, value) in [
        (
            P::PermanentAttributeModifiers(6),
            V::PermanentAttributeModifiers(Vec::new()),
        ),
        (
            P::Attribute(6),
            V::Attribute {
                base: Some(30.0_f64.to_bits()),
                modifiers: vec![gear.clone()],
                transient_modifiers: vec![gear.clone()],
            },
        ),
    ] {
        let target = Target::Chunk {
            key: contact.chunk,
            part: ChunkPart::LivingProperty {
                actor: contact.actor(),
                property,
            },
            inventory: None,
        };
        action.conditions.push(Condition {
            target,
            expected: Expected::Exact(
                crate::derived_living::facet_cell(&original, property).unwrap(),
            ),
        });
        action.effects.push(Effect::Put {
            target,
            value: Value::LivingProperty(value),
        });
    }
    action.validate().unwrap();
    for view in [TruthView::Local, TruthView::Ground] {
        let applied = apply_action(&mut state, &action, view).unwrap();
        let Value::Living(after) = state.read(view, contact.target()).unwrap().value else {
            unreachable!()
        };
        assert_eq!(after.attribute(6, 30.0), 35.0);
        assert_eq!(after.combat.equipment.modifiers, vec![gear.clone()]);
        assert!(!after.permanent_attribute_modifiers.contains_key(&6));
        undo_action(&mut state, &applied).unwrap();
        assert_eq!(
            state.read(view, contact.target()).unwrap().value,
            original.value
        );
        assert_eq!(state.read(view, root_target), Some(root.clone()));
    }
}

#[test]
fn melee_resolves_effective_motion_and_preserves_winning_reliable_impulses() {
    use crate::combat_command::{VelocityCut, VelocityValues};
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let (_, mut source, attacker) = fixture(1);
    let (_, mut victim, contact) = fixture(2);
    source.combat.effective_attributes = BTreeMap::from([
        (1, 8.0_f64.to_bits()), (2, 4.0_f64.to_bits()),
    ]);
    source.combat.equipment.knockback = 1;
    victim.combat.effective_attributes = BTreeMap::from([
        (3, 0.0_f64.to_bits()), (4, 0.0_f64.to_bits()), (5, 0.0_f64.to_bits()),
    ]);
    let original = VelocityValues { velocity: [0; 3], revisions: [0; 3] };
    let motion = BTreeMap::from([
        (attacker.target(), VelocityCut { reliable: original, effective: [1_000_000, 200_000, 0] }),
        (contact.target(), VelocityCut { reliable: original, effective: [1_000_000, 0, 0] }),
    ]);
    let mut hit = attack(attacker.clone(), contact.clone(), false);
    hit.pose.knockback_enabled = true;
    let intent = CombatIntent {
        attacks: vec![hit],
        attack_clock: Some(AttackClockIntent { actor: attacker.actor(), target: attacker.target(), tick: TickStamp(100) }),
        ..Default::default()
    };
    let id = ActionId { actor: attacker.actor(), incarnation: 9, sequence: 1, family: 7 };
    let property = |actor, property| Target::Chunk {
        key: attacker.chunk,
        part: ChunkPart::LivingProperty { actor, property },
        inventory: None,
    };
    let velocity = |effects: &[Effect], actor, axis| effects.iter().find_map(|effect| match effect {
        Effect::Put { target, value: Value::LivingProperty(V::Signed(value)) }
            if *target == property(actor, axis) => Some(*value),
        _ => None,
    });
    for (winning, expected) in [(false, 450_000), (true, 262_500)] {
        let mut current_victim = victim.clone();
        if winning {
            current_victim.combat.velocity_micro[0] = 250_000;
            current_victim.property_versions.insert(P::VelocityX, 1);
        }
        let values = BTreeMap::from([
            (attacker.target(), Cell { revision: 0, value: Value::Living((source.clone()).into()) }),
            (contact.target(), Cell { revision: 0, value: Value::Living((current_victim).into()) }),
        ]);
        let effects = intent.derive_with_motion(&crate::canonical_action::OwnedStakeholderSet::default(), id, TickStamp(100), &values, &motion).unwrap();
        assert_eq!(velocity(&effects, attacker.actor(), P::VelocityX), Some(600_000));
        assert_eq!(velocity(&effects, attacker.actor(), P::VelocityY), Some(200_000));
        assert_eq!(velocity(&effects, contact.actor(), P::VelocityX), Some(expected));
        assert_eq!(velocity(&effects, contact.actor(), P::VelocityY), Some(400_000));
        assert_eq!(velocity(&effects, contact.actor(), P::VelocityZ), Some(500_000));
    }
}

#[test]
fn health_only_melee_does_not_publish_an_unrelated_effective_velocity() {
    use crate::combat_command::{VelocityCut, VelocityValues};
    use crate::living_property::LivingProperty as P;
    let (_, mut source, attacker) = fixture(1);
    let (_, mut victim, contact) = fixture(2);
    source.combat.effective_attributes = BTreeMap::from([(1, 8.0_f64.to_bits()), (2, 4.0_f64.to_bits())]);
    victim.combat.effective_attributes = BTreeMap::from([(3, 0.0_f64.to_bits()), (4, 0.0_f64.to_bits()), (5, 0.0_f64.to_bits())]);
    let cut = VelocityCut { reliable: VelocityValues { velocity: [0; 3], revisions: [0; 3] }, effective: [1_000_000, 200_000, 0] };
    let motion = BTreeMap::from([(attacker.target(), cut), (contact.target(), cut)]);
    let values = BTreeMap::from([
        (attacker.target(), Cell { revision: 0, value: Value::Living((source).into()) }),
        (contact.target(), Cell { revision: 0, value: Value::Living((victim).into()) }),
    ]);
    let mut hit = attack(attacker.clone(), contact, false);
    hit.pose.knockback_enabled = false;
    let intent = CombatIntent { attacks: vec![hit], ..Default::default() };
    let id = ActionId { actor: attacker.actor(), incarnation: 9, sequence: 1, family: 7 };
    let effects = intent.derive_with_motion(&crate::canonical_action::OwnedStakeholderSet::default(), id, TickStamp(100), &values, &motion).unwrap();
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Put { target: Target::Chunk { part: ChunkPart::LivingProperty { property: P::Health, .. }, .. }, .. })));
    assert!(!effects.iter().any(|effect| matches!(effect, Effect::Put { target: Target::Chunk { part: ChunkPart::LivingProperty { property: P::VelocityX | P::VelocityY | P::VelocityZ, .. }, .. }, .. })));
}

#[test]
fn scale_sixteen_player_contact_and_eye_height_remain_attackable() {
    let (_, _, mut attacker) = fixture(1);
    let (_, _, mut victim) = fixture(2);
    for contact in [&mut attacker, &mut victim] {
        contact.half_width = (0.6_f64 * 16.0 / 2.0).to_bits();
        contact.height = (1.8_f64 * 16.0).to_bits();
    }
    victim.position[0] = 8.0_f64.to_bits();
    let contact = AttackContact { attacker, victim, reach: 3.0_f64.to_bits(),
        eye_height: (1.62_f64 * 16.0).to_bits(), victim_yaw: 0.0_f32.to_bits(),
        victim_on_ground: true, producer: PeerGeneration { server: ServerId(1), generation: 1 }, tick: TickStamp(100) };
    assert!(contact.attacker.valid());
    assert!(contact.victim.valid());
    assert!(contact.valid());
    let mut outside = contact;
    outside.victim.position[0] = 9.0_f64.to_bits();
    assert!(!outside.valid());
}

#[test]
fn trusted_operator_reach_supports_far_chunk_targets_and_large_finite_attributes() {
    let (_, _, attacker) = fixture(1);
    let (_, _, mut victim) = fixture(2);
    victim.position[0] = 1000.0_f64.to_bits();
    victim.chunk = ChunkKey::new(victim.world, 62, 0);
    let mut contact = AttackContact { attacker, victim, reach: 1000.0_f64.to_bits(),
        eye_height: 1.62_f64.to_bits(), victim_yaw: 0.0_f32.to_bits(), victim_on_ground: true,
        producer: PeerGeneration { server: ServerId(1), generation: 1 }, tick: TickStamp(100) };
    assert!(contact.valid());
    contact.reach = 3.0_f64.to_bits();
    assert!(!contact.valid());
    contact.reach = f64::MAX.to_bits();
    assert!(contact.valid());
    contact.reach = (-0.0_f64).to_bits();
    assert!(!contact.valid());
    contact.reach = f64::INFINITY.to_bits();
    assert!(!contact.valid());
}

#[test]
fn rejected_motion_overflow_preserves_numeric_damage_state() {
    let (_, mut living, contact) = fixture(1);
    living.combat.absorption_milli = 2000;
    living.combat.velocity_micro = [i64::MAX, 0, 0];
    let before = living.clone();
    let damage = DamageIntent { kind: DamageKind::PlayerAttack, source: contact.actor(),
        target: contact.target(), tick: TickStamp(100), damage_milli: 4000,
        knockback_micro: [1, 0, 0], bypass_hurt_cooldown: false, fire_ticks: 20,
        reduction: None, bypass_invulnerability: false };
    assert_eq!(damage.apply(&mut living), Err(ApplyFailure::InvalidAction));
    assert_eq!(living, before);
}
