use std::collections::BTreeMap;
use std::sync::Arc;

use pumpkin_cluster::canonical_action::{
    ActionProposal, ActionState, ApplyFailure, Cell, ChunkPart, ClaimedAction, Condition, Expected,
    InventoryId, LivePlayerLease, Target, TruthView, Value,
};
use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
use pumpkin_cluster::inventory::{
    INV_MAIN, INV_SELECTED, InvLoc, InventorySlotRef, InventoryTarget,
};
use pumpkin_cluster::living_property::{LivingProperty, LivingPropertyValue};

pub(super) struct MiningReadContext {
    pub lease: LivePlayerLease,
    pub actor_target: Target,
    pub epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
    pub source: InventorySlotBinding,
    pub source_cell: Cell,
    pub selected: Condition,
    pub facts: BTreeMap<Target, Option<Cell>>,
    pub evidence: pumpkin_cluster::block_action::MiningEvidence,
    pub living: pumpkin_cluster::item_action::ActorLivingState,
    pub equipment: pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding,
    pub statistics: Vec<Target>,
}

pub(super) struct FrozenMiningInput {
    pub proposal: ActionProposal,
    pub snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    pub draft: pumpkin_cluster::actor_action_scope::ActorActionDraft,
    pub context: MiningReadContext,
    pub decision: Option<pumpkin_cluster::block_action::MiningDecision>,
    pub identity_bound: bool,
    pub cosmetic_prepared: bool,
    pub cosmetic: Option<super::cluster_player_cosmetics::LocalPlayerCosmetic>,
    pub loot: crate::world::loot::LootContextParameters,
}

impl FrozenMiningInput {
    pub(super) fn capture_from_snapshot(
        snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        server: Arc<super::Server>,
        _owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        draft: &pumpkin_cluster::actor_action_scope::ActorActionDraft,
        mut proposal: ActionProposal,
        tick: pumpkin_cluster::time::TickStamp,
        executors: &BTreeMap<
            pumpkin_cluster::protocol::ChunkKey,
            pumpkin_cluster::realm_completion::PeerGeneration,
        >,
    ) -> Result<Self, ApplyFailure> {
        proposal.tick = tick;
        let ClaimedAction::Block(claim) = &mut proposal.claim else {
            return Err(ApplyFailure::InvalidAction);
        };
        claim.intent.tick = tick;
        if claim.intent.owner_install_generation == 0
            || snapshot
                .player_install_generation(proposal.lease.ok_or(ApplyFailure::InvalidAction)?)
                != Some(claim.intent.owner_install_generation)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let projection = snapshot
            .player_projection(claim.intent.actor)
            .filter(|projection| projection.chunk == claim.intent.actor_chunk)
            .ok_or(ApplyFailure::MissingStakeholder(
                pumpkin_cluster::canonical_action::Stakeholder::Chunk(claim.intent.actor_chunk),
            ))?;
        claim.effective_frame = projection.frame;
        let actor_target = Target::Chunk {
            key: claim.intent.actor_chunk,
            part: ChunkPart::Player(claim.intent.actor),
            inventory: None,
        };
        let body = draft
            .view(snapshot.as_ref())
            .read(TruthView::Local, actor_target)
            .ok_or(ApplyFailure::MissingStakeholder(actor_target.stakeholder()))?;
        let Value::Living(living) = body.value else {
            return Err(ApplyFailure::Type(actor_target));
        };
        let mut pose = super::cluster_living_metadata::player_pose(&living);
        if projection.gliding {
            pose = pumpkin_data::entity::EntityPose::FallFlying;
        } else if projection.crawling || projection.swimming {
            pose = pumpkin_data::entity::EntityPose::Swimming;
        } else if projection.sneaking {
            pose = pumpkin_data::entity::EntityPose::Crouching;
        }
        claim.eye_height =
            f64::from(super::cluster_living_metadata::player_dimensions(&living, pose).eye_height);
        let mut context = capture(&draft.view(snapshot.as_ref()), &proposal)?;
        let Value::Inventory(tool) = &context.source_cell.value else {
            return Err(ApplyFailure::Inventory(context.source.target));
        };
        if tool.item != 0 {
            let item = pumpkin_data::item::Item::from_id(tool.item)
                .ok_or(ApplyFailure::Inventory(context.source.target))?;
            let game_mode = context
                .living
                .avatar
                .as_ref()
                .ok_or(ApplyFailure::Type(context.actor_target))?
                .game_mode;
            context.evidence.can_break &= server
                .item_registry
                .can_mine_owned(item, game_mode)
                .ok_or(ApplyFailure::InvalidAction)?;
        }
        let ClaimedAction::Block(claim) = &proposal.claim else {
            unreachable!()
        };
        let center = claim.intent.target_chunk;
        let world = server
            .worlds
            .load()
            .iter()
            .find(|world| super::cluster_world_delta::world_id(world) == center.world)
            .cloned()
            .ok_or(ApplyFailure::InvalidAction)?;
        let mut loot = crate::world::loot::LootContextParameters::default();
        loot.tool = Some(
            super::cluster_invsee::item_stack_from_inventory_cell(tool)
                .map_err(|_| ApplyFailure::Inventory(context.source.target))?,
        );
        let luck_id = u16::from(pumpkin_data::attributes::Attributes::LUCK.id);
        if let Some(base) = context.living.combat.effective_attributes.get(&luck_id) {
            loot.luck = context.living.attribute(luck_id, f64::from_bits(*base)) as f32;
        }
        let snapshot = snapshot.with_scheduled_executors(executors);
        Ok(Self {
            proposal,
            snapshot,
            draft: draft.clone(),
            context,
            decision: None,
            identity_bound: false,
            cosmetic_prepared: false,
            cosmetic: None,
            loot,
        })
    }

    pub(super) fn view(
        &self,
    ) -> pumpkin_cluster::actor_action_scope::ActorActionDraftView<
        '_,
        super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    > {
        self.draft.view(self.snapshot.as_ref())
    }

    pub(super) fn stage(
        &mut self,
        owner: &mut super::cluster_world_block_actions::BlockActionOwner,
    ) -> pumpkin_cluster::block_action::MiningDecision {
        *self.decision.get_or_insert_with(|| {
            let ClaimedAction::Block(claim) = &self.proposal.claim else {
                unreachable!()
            };
            let Value::Inventory(tool) = &self.context.source_cell.value else {
                unreachable!()
            };
            owner.stage(claim.intent, self.context.evidence, tool)
        })
    }
}

fn read<S: ActionState>(
    state: &S,
    target: Target,
    facts: &mut BTreeMap<Target, Option<Cell>>,
) -> Result<Cell, ApplyFailure> {
    if !state.available(TruthView::Local, target) {
        return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
    }
    let cell = state
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?;
    facts.insert(target, Some(cell.clone()));
    Ok(cell)
}

pub(super) fn capture<S: ActionState>(
    state: &S,
    proposal: &ActionProposal,
) -> Result<MiningReadContext, ApplyFailure> {
    let ClaimedAction::Block(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if lease.gid != claim.intent.actor
        || proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation
        || !state.validate_live_player_lease(lease)
        || claim.effective_frame.effective.actor != lease.gid
        || !claim.eye_height.is_finite()
        || !(0.0..=32.0).contains(&claim.eye_height)
        || claim
            .effective_frame
            .effective
            .position
            .iter()
            .any(|value| !value.is_finite())
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let actor_target = Target::Chunk {
        key: claim.intent.actor_chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let mut facts = BTreeMap::new();
    let body = read(state, actor_target, &mut facts)?;
    let Value::Living(mut living) = body.value else {
        return Err(ApplyFailure::Type(actor_target));
    };
    let epoch = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(actor_target))?
        .spatial_epoch;
    let efficiency_id = u16::from(pumpkin_data::attributes::Attributes::MINING_EFFICIENCY.id);
    let reach_id = u16::from(pumpkin_data::attributes::Attributes::BLOCK_INTERACTION_RANGE.id);
    let mut attributes = BTreeMap::new();
    for property in [
        LivingProperty::GameMode,
        LivingProperty::Food,
        LivingProperty::Health,
        LivingProperty::StatusEffects,
        LivingProperty::Attribute(efficiency_id),
        LivingProperty::Attribute(reach_id),
        LivingProperty::Attribute(u16::from(
            pumpkin_data::attributes::Attributes::SUBMERGED_MINING_SPEED.id,
        )),
        LivingProperty::Attribute(u16::from(pumpkin_data::attributes::Attributes::LUCK.id)),
    ] {
        let target = Target::Chunk {
            key: claim.intent.actor_chunk,
            part: ChunkPart::LivingProperty {
                actor: proposal.id.actor,
                property,
            },
            inventory: None,
        };
        let cell = read(state, target, &mut facts)?;
        let Value::LivingProperty(value) = cell.value else {
            return Err(ApplyFailure::Type(target));
        };
        if let LivingProperty::Attribute(id) = property {
            let LivingPropertyValue::Attribute {
                base: Some(base),
                modifiers,
                ..
            } = &value
            else {
                return Err(ApplyFailure::Type(target));
            };
            let mut equipment = pumpkin_cluster::combat_action::CombatEquipmentFacts::default();
            equipment.modifiers = modifiers.clone();
            let value = equipment.attribute(id, f64::from_bits(*base));
            if !value.is_finite() || (id == efficiency_id || id == reach_id) && value < 0.0 {
                return Err(ApplyFailure::InvalidAction);
            }
            attributes.insert(id, value);
        }
        if !property.write(&mut living, &value) {
            return Err(ApplyFailure::Type(target));
        }
    }
    let selector = Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(InventoryId::Player {
            inv: INV_SELECTED,
            slot: 0,
        }),
    };
    let selected_cell = read(state, selector, &mut facts)?;
    let Value::Inventory(selected_item) = &selected_cell.value else {
        return Err(ApplyFailure::Inventory(selector));
    };
    if selected_item.count != 1
        || !selected_item.nbt.is_empty()
        || !(1..=9).contains(&selected_item.item)
    {
        return Err(ApplyFailure::Inventory(selector));
    }
    let slot = selected_item.item - 1;
    let source_target = Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(InventoryId::Player {
            inv: INV_MAIN,
            slot,
        }),
    };
    let source_cell = read(state, source_target, &mut facts)?;
    let Value::Inventory(item) = &source_cell.value else {
        return Err(ApplyFailure::Inventory(source_target));
    };
    let tool = super::cluster_invsee::item_stack_from_inventory_cell(item)
        .map_err(|_| ApplyFailure::Inventory(source_target))?;
    let block_target = Target::Chunk {
        key: claim.intent.target_chunk,
        part: ChunkPart::Block(claim.intent.position),
        inventory: None,
    };
    if proposal.claimed_targets != [block_target]
        || claim.intent.face >= 6
        || living.avatar.as_ref().is_none_or(|avatar| {
            avatar.world != claim.intent.target_chunk.world
                || avatar.world != claim.intent.actor_chunk.world
        })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let block_cell = read(state, block_target, &mut facts)?;
    let Value::Block(block) = block_cell.value else {
        return Err(ApplyFailure::Type(block_target));
    };
    let block = pumpkin_data::BlockStateId::new(block).ok_or(ApplyFailure::Type(block_target))?;
    let position = claim.effective_frame.effective.position;
    let target = claim.intent.position;
    let distance = [target.x, target.y, target.z]
        .into_iter()
        .enumerate()
        .map(|(axis, coordinate)| {
            let origin = position[axis] + if axis == 1 { claim.eye_height } else { 0.0 };
            let delta = origin - origin.clamp(f64::from(coordinate), f64::from(coordinate) + 1.0);
            delta * delta
        })
        .sum::<f64>();
    let reach = attributes[&reach_id];
    if !distance.is_finite() || distance > reach * reach {
        return Err(ApplyFailure::InvalidAction);
    }
    let mode = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(actor_target))?
        .game_mode;
    let eye = [position[0], position[1] + claim.eye_height, position[2]];
    let eye_position = pumpkin_cluster::protocol::BlockPos {
        x: eye[0].floor() as i32,
        y: eye[1].floor() as i32,
        z: eye[2].floor() as i32,
    };
    let fluid_state = |position: pumpkin_cluster::protocol::BlockPos,
                       facts: &mut BTreeMap<Target, Option<Cell>>|
     -> Result<pumpkin_data::BlockStateId, ApplyFailure> {
        let target = Target::Chunk {
            key: pumpkin_cluster::protocol::ChunkKey::new(
                claim.intent.actor_chunk.world,
                position.x.div_euclid(16),
                position.z.div_euclid(16),
            ),
            part: ChunkPart::Block(position),
            inventory: None,
        };
        let Value::Block(id) = read(state, target, facts)?.value else {
            return Err(ApplyFailure::Type(target));
        };
        pumpkin_data::BlockStateId::new(id).ok_or(ApplyFailure::Type(target))
    };
    let eye_state = fluid_state(eye_position, &mut facts)?;
    let water = |state: pumpkin_data::BlockStateId| {
        pumpkin_data::fluid::Fluid::from_state_id(state)
            .is_some_and(|fluid| fluid.matches_type(&pumpkin_data::fluid::Fluid::WATER))
            || state.to_block().is_waterlogged(state)
    };
    let mut submerged_multiplier = 1.0;
    if water(eye_state) {
        let above = pumpkin_cluster::protocol::BlockPos {
            y: eye_position
                .y
                .checked_add(1)
                .ok_or(ApplyFailure::InvalidAction)?,
            ..eye_position
        };
        let height = if water(fluid_state(above, &mut facts)?) {
            1.0
        } else {
            pumpkin_data::fluid::Fluid::from_state_id(eye_state)
                .map_or(8.0 / 9.0, |fluid| f64::from(fluid.get_height(eye_state)))
        };
        if eye[1] < f64::from(eye_position.y) + height {
            submerged_multiplier = attributes
                [&u16::from(pumpkin_data::attributes::Attributes::SUBMERGED_MINING_SPEED.id)];
        }
    }
    let evidence = super::cluster_world_mining::mining_evidence(
        block,
        &tool,
        source_cell.revision,
        &living,
        attributes[&efficiency_id],
        submerged_multiplier,
        claim.effective_frame.effective.on_ground,
        mode != 2,
    )?;
    let equipment = pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
        lease,
        claim.intent.actor_chunk,
        state.inventory_equipment_attribute_ids(),
    );
    for target in equipment.targets() {
        if !facts.contains_key(&target) {
            read(state, target, &mut facts)?;
        }
    }
    let mut statistics = Vec::new();
    if mode != 1 {
        for (category, key) in std::iter::once((0, u32::from(block.to_block().id.as_u16())))
            .chain((item.item != 0).then_some((3, u32::from(item.item))))
        {
            let target = Target::Chunk {
                key: claim.intent.actor_chunk,
                part: ChunkPart::LivingProperty {
                    actor: proposal.id.actor,
                    property: LivingProperty::Statistic { category, key },
                },
                inventory: None,
            };
            read(state, target, &mut facts)?;
            statistics.push(target);
        }
    }
    Ok(MiningReadContext {
        lease,
        actor_target,
        epoch,
        source: InventorySlotBinding {
            slot: InventorySlotRef::new(
                InventoryTarget::LeasedPlayer {
                    key: lease.key.0,
                    player: lease.gid,
                    incarnation: lease.incarnation,
                },
                InvLoc::new(INV_MAIN, slot),
            ),
            target: source_target,
            max_stack: 99,
        },
        source_cell,
        selected: Condition {
            target: selector,
            expected: Expected::Exact(selected_cell),
        },
        facts,
        evidence,
        living: *living,
        equipment,
        statistics,
    })
}

pub(super) struct MiningCallbackContext {
    pub proposal: ActionProposal,
    pub producer: pumpkin_cluster::realm_completion::PeerGeneration,
    pub world: Arc<crate::world::World>,
    pub context: MiningReadContext,
}

impl MiningCallbackContext {
    pub(super) fn location(
        &self,
    ) -> (
        pumpkin_cluster::protocol::WorldId,
        pumpkin_util::math::position::BlockPos,
    ) {
        let ClaimedAction::Block(claim) = &self.proposal.claim else {
            unreachable!()
        };
        (
            claim.intent.target_chunk.world,
            pumpkin_util::math::position::BlockPos::new(
                claim.intent.position.x,
                claim.intent.position.y,
                claim.intent.position.z,
            ),
        )
    }

    pub(super) fn capture<S: ActionState>(
        &self,
        view: &S,
        snapshot: &mut crate::block::neighbor::NeighborSnapshot,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<
        super::cluster_world_callbacks::CallbackCapture,
        super::cluster_world_callbacks::CallbackCaptureError,
    > {
        let (world, position) = self.location();
        {
            let mut flags = pumpkin_world::world::BlockFlags::NOTIFY_ALL;
            let drops = if let Value::Inventory(item) = &self.context.source_cell.value {
                super::cluster_invsee::item_stack_from_inventory_cell(item)
                    .ok()
                    .zip(pumpkin_data::BlockStateId::new(
                        self.context.evidence.block_state,
                    ))
                    .is_some_and(|(tool, state)| {
                        !state.to_state().tool_required()
                            || tool.is_correct_for_drops(state.to_block())
                    })
            } else {
                false
            };
            if self.context.evidence.creative || !drops {
                flags.insert(pumpkin_world::world::BlockFlags::SKIP_DROPS);
            }
            super::cluster_world_callbacks::capture_effects(
                view,
                world,
                &self.world.block_registry,
                snapshot,
                facts,
                &[crate::block::neighbor::BlockCallbackEffect::PlayerBreak {
                    position,
                    flags,
                    context: crate::block::neighbor::PlayerBreakContext {
                        state: pumpkin_data::BlockStateId::new(self.context.evidence.block_state)
                            .ok_or(
                            super::cluster_world_callbacks::CallbackCaptureError::Budget,
                        )?,
                        creative: self.context.evidence.creative,
                        silk_touch: match &self.context.source_cell.value {
                            Value::Inventory(item) => {
                                super::cluster_invsee::item_stack_from_inventory_cell(item)
                                    .ok()
                                    .is_some_and(|tool| {
                                        tool.get_enchantment_level(
                                            &pumpkin_data::enchantment::Enchantment::SILK_TOUCH,
                                        ) > 0
                                    })
                            }
                            _ => false,
                        },
                        water_evaporates: self.world.dimension.water_evaporates,
                    },
                }],
                4096,
            )
        }
    }

    pub(super) fn complete<S: ActionState>(
        &self,
        view: &S,
        facts: &mut BTreeMap<Target, Option<Cell>>,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
    ) -> Result<pumpkin_cluster::canonical_action::Action, ApplyFailure> {
        let position = self.location().1;
        if !resolution.writes.iter().any(|(changed, before, after)| {
            *changed == position
                && before.as_u16() == self.context.evidence.block_state
                && before != after
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        for (&target, cell) in &self.context.facts {
            facts.entry(target).or_insert_with(|| cell.clone());
        }
        let ClaimedAction::Block(claim) = &self.proposal.claim else {
            unreachable!()
        };
        let reference = pumpkin_cluster::canonical_action::ActionRef {
            id: self.proposal.id,
            tick: self.proposal.tick,
        };
        let mut action = super::cluster_world_callbacks::build_callback_action(
            view,
            reference,
            self.producer,
            Some(claim.intent.actor_chunk),
            Some(self.context.lease),
            claim.intent.target_chunk.world,
            facts,
            resolution,
        )?;
        action.client_origin = self.proposal.client_origin;
        action.kind = pumpkin_cluster::canonical_action::ActionKind::Block;
        let Value::Inventory(tool) = &self.context.source_cell.value else {
            return Err(ApplyFailure::Inventory(self.context.source.target));
        };
        if tool.item != 0 && !self.context.evidence.creative {
            let block = pumpkin_data::BlockStateId::new(self.context.evidence.block_state)
                .ok_or(ApplyFailure::InvalidAction)?;
            action
                .effects
                .push(pumpkin_cluster::canonical_action::Effect::TransformItems {
                intent: pumpkin_cluster::canonical_inventory::ItemTransformIntent {
                    source: self.context.source.clone(),
                    item: tool.item,
                    components: tool.nbt.clone(),
                    operation:
                        pumpkin_cluster::canonical_inventory::ItemTransformOperation::MineDamage {
                            hardness: block.to_state().hardness.to_bits(),
                        },
                    remainder_destinations: Vec::new(),
                },
            });
        }
        for &target in &self.context.statistics {
            action
                .conditions
                .retain(|condition| condition.target != target);
            action
                .effects
                .push(pumpkin_cluster::canonical_action::Effect::Add { target, amount: 1 });
        }
        Ok(action)
    }

    pub(super) fn finalize<S: ActionState>(
        &self,
        view: &S,
        action: &mut pumpkin_cluster::canonical_action::Action,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
    ) -> Result<(), ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ActionClass, SemanticIntent, Stakeholder};
        use pumpkin_cluster::world_action::{WorldReplayIntent, WorldReplayStep};
        let ClaimedAction::Block(claim) = &self.proposal.claim else {
            unreachable!()
        };
        let exhaustion = pumpkin_cluster::combat_action::FoodExhaustionIntent {
            lease: self.context.lease,
            chunk: claim.intent.actor_chunk,
            epoch: self.context.epoch,
            tick: action.tick,
            amount_milli: 5,
        };
        let guard = exhaustion.guard();
        action
            .conditions
            .retain(|condition| condition.target != guard.target);
        action.conditions.push(guard);
        let equipment_values = self
            .context
            .facts
            .iter()
            .filter_map(|(target, value)| value.clone().map(|cell| (*target, cell)))
            .collect();
        pumpkin_cluster::canonical_inventory::append_equipment_effects(
            &equipment_values,
            &mut action.effects,
            std::slice::from_ref(&self.context.equipment),
            view,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
        let health = Target::Chunk {
            key: claim.intent.actor_chunk,
            part: ChunkPart::LivingProperty {
                actor: action.id.actor,
                property: LivingProperty::Health,
            },
            inventory: None,
        };
        action
            .conditions
            .retain(|condition| condition.target != health);
        action.conditions.push(Condition {
            target: health,
            expected: Expected::LivingUnsignedAtLeast(1),
        });
        if let Value::Inventory(tool) = &self.context.source_cell.value
            && tool.item != 0
        {
            action
                .conditions
                .retain(|condition| condition.target != self.context.source.target);
            action.conditions.push(Condition {
                target: self.context.source.target,
                expected: Expected::InventoryItem {
                    item: tool.item,
                    nbt: tool.nbt.clone(),
                    minimum: 1,
                },
            });
        }
        let food_target = exhaustion.targets()[0];
        let required = action
            .conditions
            .iter()
            .filter(|condition| condition.target != food_target)
            .cloned()
            .collect();
        let mut steps = vec![
            WorldReplayStep::RequiredEffects {
                conditions: required,
                effects: action.effects.clone(),
            },
            WorldReplayStep::ExhaustFood(exhaustion),
        ];
        let plans = super::cluster_world_callbacks::resolution_light_plans(
            &self.world,
            claim.intent.target_chunk.world,
            resolution,
        )?;
        let mut light_reads = BTreeMap::new();
        for &plan in &plans {
            for target in plan
                .section_targets()?
                .into_iter()
                .chain(plan.light_targets()?)
            {
                let cell = view
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?;
                light_reads.insert(target, cell.revision);
            }
        }
        for (target, revision) in light_reads {
            action
                .conditions
                .retain(|condition| condition.target != target);
            action.conditions.push(Condition {
                target,
                expected: Expected::RevisionEquals(revision),
            });
        }
        steps.extend(plans.into_iter().map(WorldReplayStep::DerivedLight));
        let intent = WorldReplayIntent { steps };
        let targets = intent.targets().collect::<Vec<_>>();
        let values = targets
            .iter()
            .filter_map(|target| {
                view.read(TruthView::Local, *target)
                    .map(|cell| (*target, cell))
            })
            .collect();
        action.effects = intent.derive_effects_with(&values, view)?;
        action.counterfactual_conditions = intent.read_targets().into_iter().collect();
        action.counterfactual_effects = intent.effect_targets().into_iter().collect();
        action
            .stakeholders
            .extend(targets.into_iter().map(Target::stakeholder));
        action
            .stakeholders
            .push(Stakeholder::Player(self.context.lease.key));
        action.stakeholders.sort_unstable();
        action.stakeholders.dedup();
        action.class = ActionClass::Replayable;
        action.semantic_intent = Some(SemanticIntent::World(intent));
        action.validate()
    }

    pub(super) fn reject(&self, server: &super::Server) {
        let ClaimedAction::Block(claim) = &self.proposal.claim else {
            unreachable!()
        };
        let _ = super::cluster_inventory_protocol::publish_block_action_resolution(
            server,
            claim.intent.actor,
            claim.intent.position,
            None,
            claim.intent.client_sequence,
            false,
        );
    }
}
