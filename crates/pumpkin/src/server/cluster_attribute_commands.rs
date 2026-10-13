use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::living_property::{LivingProperty, LivingPropertyValue};
use pumpkin_cluster::player_command_control::AttributeMutation;

pub(crate) fn living(
    server: &super::Server,
    target: &dyn crate::entity::EntityBase,
) -> Option<pumpkin_cluster::item_action::ActorLivingState> {
    if let Some(player) = target.get_player() {
        let actor = pumpkin_cluster::actor::ActorId::Player(player.cluster_gid()?);
        return server.cluster_actor_living.load().get(&actor).cloned();
    }
    let shell = target
        .cast_any()
        .downcast_ref::<super::cluster_entity_registry::CanonicalEntityShell>()?;
    pumpkin_cluster::living_property::state(&Value::Entity((shell.body.clone()).into())).cloned()
}

pub(super) fn materialize(
    view: &impl ActionState,
    id: ActionId,
    tick: pumpkin_cluster::time::TickStamp,
    body: Target,
    lease: Option<LivePlayerLease>,
    epoch: SpatialEpoch,
    authority: Option<(
        pumpkin_cluster::entity_body::EntityBirth,
        pumpkin_cluster::realm_completion::PeerGeneration,
    )>,
    mutation: &AttributeMutation,
) -> Result<Action, ApplyFailure> {
    let attribute = mutation.attribute();
    if !mutation.valid()
        || !pumpkin_data::attributes::Attributes::ALL
            .iter()
            .any(|candidate| u16::from(candidate.id) == attribute)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let Target::Chunk { key, .. } = body else {
        return Err(ApplyFailure::Type(body));
    };
    let attribute_target = Target::Chunk {
        key,
        part: ChunkPart::LivingProperty {
            actor: id.actor,
            property: LivingProperty::Attribute(attribute),
        },
        inventory: None,
    };
    let original = view
        .read(TruthView::Local, attribute_target)
        .ok_or(ApplyFailure::Condition(attribute_target))?;
    let Value::LivingProperty(LivingPropertyValue::Attribute {
        base: Some(base),
        modifiers,
        transient_modifiers,
    }) = &original.value
    else {
        return Err(ApplyFailure::Type(attribute_target));
    };
    let mut effective_base = f64::from_bits(*base);
    let mut effective_modifiers = modifiers.clone();
    match mutation {
        AttributeMutation::SetBase { value, .. } => effective_base = f64::from_bits(*value),
        AttributeMutation::AddModifier(modifier) => {
            if effective_modifiers
                .iter()
                .any(|current| current.id == modifier.id)
            {
                return Err(ApplyFailure::Condition(attribute_target));
            }
            effective_modifiers.push(modifier.clone());
        }
        AttributeMutation::RemoveModifier { id, .. } => {
            effective_modifiers.retain(|modifier| modifier.id != *id);
            effective_modifiers.extend(
                transient_modifiers
                    .iter()
                    .filter(|modifier| modifier.id == *id)
                    .cloned(),
            );
        }
    }
    let effective = pumpkin_cluster::combat_action::CombatEquipmentFacts {
        modifiers: effective_modifiers,
        ..Default::default()
    }
    .attribute(attribute, effective_base);
    if !effective.is_finite() {
        return Err(ApplyFailure::InvalidAction);
    }
    let property = match mutation {
        AttributeMutation::SetBase { .. } => LivingProperty::Attribute(attribute),
        _ => LivingProperty::PermanentAttributeModifiers(attribute),
    };
    let target = Target::Chunk {
        key,
        part: ChunkPart::LivingProperty {
            actor: id.actor,
            property,
        },
        inventory: None,
    };
    let before = view
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?;
    let mut after = before.value.clone();
    match (mutation, &mut after) {
        (
            AttributeMutation::SetBase { value, .. },
            Value::LivingProperty(LivingPropertyValue::Attribute { base, .. }),
        ) => *base = Some(*value),
        (
            AttributeMutation::AddModifier(modifier),
            Value::LivingProperty(LivingPropertyValue::PermanentAttributeModifiers(modifiers)),
        ) => {
            if modifiers.len() >= 256 || modifiers.iter().any(|current| current.id == modifier.id) {
                return Err(ApplyFailure::Condition(target));
            }
            modifiers.push(modifier.clone());
            modifiers.sort_by(|left, right| left.id.cmp(&right.id));
        }
        (
            AttributeMutation::RemoveModifier { id, .. },
            Value::LivingProperty(LivingPropertyValue::PermanentAttributeModifiers(modifiers)),
        ) => {
            let Some(index) = modifiers.iter().position(|modifier| modifier.id == *id) else {
                return Err(ApplyFailure::Condition(target));
            };
            modifiers.remove(index);
        }
        _ => return Err(ApplyFailure::Type(target)),
    }
    let mut conditions = vec![
        Condition {
            target,
            expected: Expected::Exact(before),
        },
        Condition {
            target: body,
            expected: Expected::SpatialEpochEquals(epoch),
        },
    ];
    if target != attribute_target {
        conditions.push(Condition {
            target: attribute_target,
            expected: Expected::Exact(original),
        });
    }
    if let Some((birth, owner)) = authority {
        conditions.push(Condition {
            target: body,
            expected: Expected::EntityBirthOwned { birth, owner },
        });
    }
    let action = Action {
        id,
        tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        edit_authority: None,
        actor_chunk: Some(key),
        player_leases: lease.into_iter().collect(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(key)],
        dependencies: match epoch {
            SpatialEpoch::Action(reference) => vec![reference],
            _ => Vec::new(),
        },
        conditions,
        effects: vec![Effect::Put {
            target,
            value: after,
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::PlayerAdministration,
        presentation: Vec::new(),
        client_origin: ClientOrigin::None,
    };
    action.validate()?;
    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::item_action::PlayerAvatarState;
    use pumpkin_cluster::protocol::{ChunkKey, WorldId};
    use pumpkin_cluster::time::TickStamp;

    #[derive(Default)]
    struct RegistryState(OwnedStakeholderSet);
    impl std::ops::Deref for RegistryState {
        type Target = OwnedStakeholderSet;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl std::ops::DerefMut for RegistryState {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
    impl ActionState for RegistryState {
        fn actor_occupancy_cut(
            &self,
            target: Target,
            value: &Value,
        ) -> Result<
            (
                pumpkin_cluster::realm_completion::PeerGeneration,
                pumpkin_cluster::actor_occupancy::ActorOccupancyCut,
            ),
            ApplyFailure,
        > {
            let Value::Living(living) = value else {
                return Err(ApplyFailure::Type(target));
            };
            let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
            let pumpkin_cluster::actor::ActorId::Player(gid) = living.combat.actor else {
                return Err(ApplyFailure::Type(target));
            };
            Ok((
                pumpkin_cluster::realm_completion::PeerGeneration {
                    server: gid.server,
                    generation: 1,
                },
                pumpkin_cluster::actor_occupancy::ActorOccupancyCut {
                    position: avatar.position,
                    sample_tick: avatar.movement_tick,
                    simulation_sequence: avatar.movement_sequence,
                },
            ))
        }
        fn actor_collision_shape(
            &self,
            target: Target,
            value: &Value,
        ) -> Result<pumpkin_cluster::actor_occupancy::CollisionShape, ApplyFailure> {
            super::super::cluster_world_canonical::actor_collision_shape(target, value)
        }
        fn wake_rest_stat(&self) -> Result<(u16, u32), ApplyFailure> {
            Ok(super::super::cluster_sleep_validation::rest_stat())
        }
        fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
            self.0.read(view, target)
        }
        fn commit(
            &mut self,
            context: ActionContext,
            view: TruthView,
            writes: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            self.0.commit(context, view, writes)
        }
        fn validate_wake_beds(
            &self,
            world: WorldId,
            sleeping: pumpkin_cluster::item_action::PlayerSleepingState,
            beds: &[pumpkin_cluster::sleep_action::BedRelease],
        ) -> Result<(), ApplyFailure> {
            super::super::cluster_sleep_validation::validate_beds(world, sleeping, beds)
        }
    }

    fn fixture() -> (RegistryState, LivePlayerLease, Target, SpatialEpoch) {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(1));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(1)),
            gid,
            incarnation: 1,
        };
        let epoch = SpatialEpoch::Admission {
            lease,
            admitted_at: TickStamp(1),
        };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let body = Target::Chunk {
            key: chunk,
            part: ChunkPart::Player(gid),
            inventory: None,
        };
        let mut living = super::super::cluster_actor_import::from_player_nbt(gid, None)
            .unwrap()
            .state;
        living.avatar = Some(PlayerAvatarState {
            stance: Default::default(),
            mount: None,
            relation_versions: Default::default(),
            experience: Default::default(),
            statistics: Default::default(),
            presentation: Default::default(),
            spatial_epoch: epoch,
            world: chunk.world,
            position: [8.0_f64.to_bits(), 80.0_f64.to_bits(), 8.0_f64.to_bits()],
            yaw: 0.0_f32.to_bits(),
            head_yaw: 0.0_f32.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
            movement_tick: TickStamp(1),
            movement_sequence: PlayerSeq(1),
            game_mode: 0,
            fall_distance: 0.0_f32.to_bits(),
            on_fire: false,
        });
        let attribute = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
        living
            .combat
            .effective_attributes
            .insert(attribute, 20.0_f64.to_bits());
        let (category, key) = super::super::cluster_sleep_validation::rest_stat();
        living
            .avatar
            .as_mut()
            .unwrap()
            .statistics
            .insert((category, key), 77);
        let mut state = RegistryState::default();
        state.local.insert(
            body,
            Cell {
                revision: 0,
                value: Value::Living((living.clone()).into()),
            },
        );
        let shape = super::super::cluster_world_canonical::actor_collision_shape(
            body,
            &Value::Living((living.clone()).into()),
        )
        .unwrap();
        let root = pumpkin_cluster::actor_occupancy::ActorOccupancyRoot::from_entries(vec![
            pumpkin_cluster::actor_occupancy::ActorOccupancyEntry {
                association: chunk,
                owner: pumpkin_cluster::realm_completion::PeerGeneration {
                    server: lease.gid.server,
                    generation: 1,
                },
                cut: pumpkin_cluster::actor_occupancy::ActorOccupancyCut {
                    position: living.avatar.as_ref().unwrap().position,
                    sample_tick: living.avatar.as_ref().unwrap().movement_tick,
                    simulation_sequence: living.avatar.as_ref().unwrap().movement_sequence,
                },
                actor: lease.gid.into(),
                identity: pumpkin_cluster::actor_occupancy::OccupancyIdentity::Player(lease),
                spatial_epoch: epoch,
                shape,
            },
        ])
        .unwrap();
        state.local.insert(
            Target::Chunk {
                key: chunk,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            },
            Cell {
                revision: 0,
                value: Value::ActorOccupancy(root),
            },
        );
        for property in [
            LivingProperty::Attribute(attribute),
            LivingProperty::PermanentAttributeModifiers(attribute),
            LivingProperty::Statistic { category, key },
        ] {
            state.local.insert(
                Target::Chunk {
                    key: chunk,
                    part: ChunkPart::LivingProperty {
                        actor: lease.gid.into(),
                        property,
                    },
                    inventory: None,
                },
                Cell {
                    revision: 0,
                    value: Value::LivingProperty(property.read(&living)),
                },
            );
        }
        state.ground = state.local.clone();
        (state, lease, body, epoch)
    }

    #[test]
    fn attribute_base_action_applies_independently_to_local_ground_and_undo() {
        let (mut state, lease, body, epoch) = fixture();
        let attribute = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
        let action = materialize(
            &state,
            ActionId {
                actor: lease.gid.into(),
                incarnation: lease.incarnation,
                sequence: 1,
                family: 7,
            },
            TickStamp(2),
            body,
            Some(lease),
            epoch,
            None,
            &AttributeMutation::SetBase {
                attribute,
                value: 40.0_f64.to_bits(),
            },
        )
        .unwrap();
        let original_ground = state.ground.clone();
        let applied = apply_action(&mut state, &action, TruthView::Local).unwrap();
        assert_eq!(state.ground, original_ground);
        assert_ne!(state.local, state.ground);
        apply_action(&mut state, &action, TruthView::Ground).unwrap();
        assert_eq!(state.local, state.ground);
        undo_action(&mut state, &applied).unwrap();
        assert_eq!(state.local, original_ground);
        assert_ne!(state.local, state.ground);
    }

    #[test]
    fn permanent_attribute_commands_reject_transient_collision_and_overflow() {
        use pumpkin_cluster::combat_action::{CombatAttributeModifier, CombatModifierOperation};
        let (mut state, lease, body, epoch) = fixture();
        let Target::Chunk { key, .. } = body else {
            panic!();
        };
        let attribute = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
        let target = Target::Chunk {
            key,
            part: ChunkPart::LivingProperty {
                actor: lease.gid.into(),
                property: LivingProperty::Attribute(attribute),
            },
            inventory: None,
        };
        let modifier = CombatAttributeModifier {
            attribute,
            slot: 1,
            id: "test:gear".to_owned(),
            amount: 2.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        };
        state.local.get_mut(&target).unwrap().value =
            Value::LivingProperty(LivingPropertyValue::Attribute {
                base: Some(20.0_f64.to_bits()),
                modifiers: vec![modifier.clone()],
                transient_modifiers: vec![modifier.clone()],
            });
        let id = ActionId {
            actor: lease.gid.into(),
            incarnation: lease.incarnation,
            sequence: 1,
            family: 7,
        };
        let mut permanent = modifier.clone();
        permanent.slot = 0;
        assert!(
            materialize(
                &state,
                id,
                TickStamp(2),
                body,
                Some(lease),
                epoch,
                None,
                &AttributeMutation::AddModifier(permanent.clone())
            )
            .is_err()
        );
        permanent.id = "test:overflow".to_owned();
        permanent.operation = CombatModifierOperation::MultiplyTotal;
        permanent.amount = f64::MAX.to_bits();
        assert!(
            materialize(
                &state,
                id,
                TickStamp(2),
                body,
                Some(lease),
                epoch,
                None,
                &AttributeMutation::AddModifier(permanent)
            )
            .is_err()
        );
        assert!(
            materialize(
                &state,
                id,
                TickStamp(2),
                body,
                Some(lease),
                epoch,
                None,
                &AttributeMutation::RemoveModifier {
                    attribute,
                    id: modifier.id
                }
            )
            .is_err()
        );
        let durable = CombatAttributeModifier {
            attribute,
            slot: 0,
            id: "test:durable".to_owned(),
            amount: 3.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        };
        let action = materialize(
            &state,
            id,
            TickStamp(2),
            body,
            Some(lease),
            epoch,
            None,
            &AttributeMutation::AddModifier(durable.clone()),
        )
        .unwrap();
        let original = state.local.clone();
        let applied = apply_action(&mut state, &action, TruthView::Local).unwrap();
        let remove = materialize(
            &state,
            ActionId { sequence: 2, ..id },
            TickStamp(3),
            body,
            Some(lease),
            epoch,
            None,
            &AttributeMutation::RemoveModifier {
                attribute,
                id: durable.id,
            },
        )
        .unwrap();
        let removed = apply_action(&mut state, &remove, TruthView::Local).unwrap();
        undo_action(&mut state, &removed).unwrap();
        undo_action(&mut state, &applied).unwrap();
        assert_eq!(state.local, original);
    }

    #[test]
    fn permanent_modifier_survives_equipment_attribute_replacement() {
        use pumpkin_cluster::combat_action::{CombatAttributeModifier, CombatModifierOperation};
        let (state, lease, body, _) = fixture();
        let Value::Living(mut living) = state.local[&body].value.clone() else {
            panic!();
        };
        let attribute = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
        let modifier = CombatAttributeModifier {
            attribute,
            slot: 0,
            id: "test:durable".to_owned(),
            amount: 5.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        };
        assert!(
            LivingProperty::PermanentAttributeModifiers(attribute).write(
                &mut living,
                &LivingPropertyValue::PermanentAttributeModifiers(vec![modifier.clone()])
            )
        );
        assert!(LivingProperty::Attribute(attribute).write(
            &mut living,
            &LivingPropertyValue::Attribute {
                base: Some(20.0_f64.to_bits()),
                modifiers: Vec::new(),
                transient_modifiers: Vec::new()
            }
        ));
        assert_eq!(living.attribute(attribute, 20.0), 25.0);
        assert!(living.belongs_to(lease.gid.into()));
        let mut transient = modifier.clone();
        transient.slot = 1;
        transient.amount = 2.0_f64.to_bits();
        let before_gear = LivingProperty::Attribute(attribute).read(&living);
        assert!(LivingProperty::Attribute(attribute).write(
            &mut living,
            &LivingPropertyValue::Attribute {
                base: Some(20.0_f64.to_bits()),
                modifiers: vec![modifier.clone()],
                transient_modifiers: vec![transient.clone()]
            }
        ));
        let before_remove = living.clone();
        assert_eq!(living.attribute(attribute, 20.0), 25.0);
        assert!(
            LivingProperty::PermanentAttributeModifiers(attribute).write(
                &mut living,
                &LivingPropertyValue::PermanentAttributeModifiers(Vec::new())
            )
        );
        assert_eq!(living.attribute(attribute, 20.0), 22.0);
        assert_eq!(living.combat.equipment.modifiers, vec![transient]);
        assert!(
            LivingProperty::PermanentAttributeModifiers(attribute).write(
                &mut living,
                &LivingPropertyValue::PermanentAttributeModifiers(vec![modifier])
            )
        );
        assert_eq!(living, before_remove);
        assert!(LivingProperty::Attribute(attribute).write(&mut living, &before_gear));
        assert!(living.combat.equipment.modifiers.is_empty());
        assert_eq!(living.attribute(attribute, 20.0), 25.0);
    }
    #[test]
    fn canonical_wake_releases_both_bed_halves_and_block_entity_with_avatar_and_undo() {
        use pumpkin_cluster::item_action::PlayerSleepingState;
        use pumpkin_cluster::protocol::BlockPos;
        use pumpkin_cluster::sleep_action::{BedRelease, FrozenWake, WakeClaim};
        use pumpkin_data::block_properties::{BedPart, WhiteBedLikeProperties};
        let (mut state, lease, body, epoch) = fixture();
        let Target::Chunk { key, .. } = body else {
            panic!();
        };
        let head = BlockPos { x: 0, y: 0, z: 0 };
        let foot = BlockPos { x: 0, y: 0, z: 1 };
        let sleeping = PlayerSleepingState {
            bed: head,
            since: TickStamp(1),
        };
        let Value::Living(living) = &mut state.local.get_mut(&body).unwrap().value else {
            panic!();
        };
        living.avatar.as_mut().unwrap().presentation.sleeping = Some(sleeping);
        let block = &pumpkin_data::Block::WHITE_BED;
        let mut beds = Vec::new();
        for (position, part) in [(head, BedPart::Head), (foot, BedPart::Foot)] {
            let mut properties = WhiteBedLikeProperties::from_state_id(block.default_state.id);
            properties.part = part;
            properties.occupied = true;
            let before = properties.to_state_id(block).as_u16();
            properties.occupied = false;
            let target = Target::Chunk {
                key,
                part: ChunkPart::Block(position),
                inventory: None,
            };
            state.local.insert(
                target,
                Cell {
                    revision: 0,
                    value: Value::Block(before),
                },
            );
            beds.push(BedRelease {
                target,
                before,
                after: properties.to_state_id(block).as_u16(),
            });
        }
        let bed_target = Target::Chunk {
            key,
            part: ChunkPart::BlockEntity(head),
            inventory: None,
        };
        let bed_entity = Cell {
            revision: 0,
            value: Value::BlockEntity(vec![10, 0, 0, 0]),
        };
        state.local.insert(bed_target, bed_entity.clone());
        state.ground = state.local.clone();
        let original = state.local.clone();
        let frozen = FrozenWake::capture(
            &state,
            lease,
            key,
            &WakeClaim {
                owner_install_generation: 1,
                source_epoch: epoch,
                sleeping,
                bed_entity: Some(bed_entity),
                beds: beds.clone(),
                morning: None,
            },
            ActionRef {
                id: ActionId {
                    actor: lease.gid.into(),
                    incarnation: lease.incarnation,
                    sequence: 1,
                    family: 7,
                },
                tick: TickStamp(2),
            },
        )
        .unwrap();
        let mut forged = frozen.clone();
        forged.beds[0].after = pumpkin_data::Block::STONE.default_state.id.as_u16();
        let forged_action = forged
            .materialize(
                ActionId {
                    actor: lease.gid.into(),
                    incarnation: lease.incarnation,
                    sequence: 1,
                    family: 7,
                },
                TickStamp(2),
            )
            .unwrap();
        assert!(apply_action(&mut state, &forged_action, TruthView::Local).is_err());
        assert_eq!(state.local, original);
        let action = frozen
            .materialize(
                ActionId {
                    actor: lease.gid.into(),
                    incarnation: lease.incarnation,
                    sequence: 1,
                    family: 7,
                },
                TickStamp(2),
            )
            .unwrap();
        let applied = apply_action(&mut state, &action, TruthView::Local).unwrap();
        assert_eq!(
            state.local[&frozen.rest_target].value,
            Value::LivingProperty(LivingPropertyValue::Unsigned(0))
        );
        let Value::Living(living) = &state.local[&body].value else {
            panic!();
        };
        assert!(
            living
                .avatar
                .as_ref()
                .unwrap()
                .presentation
                .sleeping
                .is_none()
        );
        for bed in beds {
            assert_eq!(state.local[&bed.target].value, Value::Block(bed.after));
        }
        assert_ne!(
            state.local[&bed_target].revision,
            original[&bed_target].revision
        );
        assert_eq!(state.ground, original);
        undo_action(&mut state, &applied).unwrap();
        assert_eq!(state.local, original);
    }
}
