use std::{collections::BTreeMap, sync::Arc};

use pumpkin_cluster::{
    canonical_action::{
        Action, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, Condition, Effect, Expected,
        Stakeholder, Target, TruthView, Value,
    },
    entity_body::{EntityBody, EntityPayload, EntitySpatial},
    item_contact::ContactObservation,
    object_tick::ObjectTerminal,
    protocol::ChunkKey,
    realm_completion::PeerGeneration,
};
use pumpkin_data::{Block, BlockStateId};
use pumpkin_util::math::position::BlockPos;

use super::cluster_world_callbacks::{CallbackCapture, CallbackCaptureError};

#[derive(Clone)]
pub(super) struct TerminalContact {
    pub contact: ContactObservation,
    pub eye_height: f64,
    pub yaw: u32,
}

pub(super) struct ObjectTerminalContext {
    pub reference: ActionRef,
    pub producer: PeerGeneration,
    pub world: Arc<crate::world::World>,
    pub chunk: ChunkKey,
    pub body: EntityBody,
    pub spatial: EntitySpatial,
    pub terminal: ObjectTerminal,
    pub witness: pumpkin_cluster::object_tick::ObjectTerminalWitness,
    pub support: Option<BlockPos>,
    pub contacts: Arc<Vec<TerminalContact>>,
    pub relation_participants: Arc<Vec<pumpkin_cluster::actor_relation::ActorRelationParticipant>>,
}

impl ObjectTerminalContext {
    pub(super) fn location(&self) -> (pumpkin_cluster::protocol::WorldId, BlockPos) {
        let position = self.spatial.position.map(f64::from_bits);
        (
            self.chunk.world,
            BlockPos::new(
                position[0].floor() as i32,
                position[1].floor() as i32,
                position[2].floor() as i32,
            ),
        )
    }

    pub(super) fn valid(&self) -> bool {
        self.witness.valid(self.reference.id, self.reference.tick)
            && self.witness.body == self.body
            && self.witness.effective_spatial == self.spatial
            && self.witness.terminal == self.terminal
            && self.reference.id.actor == pumpkin_cluster::actor::ActorId::Entity(self.body.id)
            && self.reference.id.incarnation == self.body.id.generation
            && self.producer == self.body.owner
            && self.body.spatial.world == self.chunk.world
            && self.spatial.world == self.chunk.world
            && self.body.belongs_to(self.body.id, self.chunk)
            && self.contacts.iter().all(|contact| {
                contact.contact.valid()
                    && contact.eye_height.is_finite()
                    && f32::from_bits(contact.yaw).is_finite()
                    && contact.eye_height >= 0.0
                    && contact.eye_height <= f64::from_bits(contact.contact.height)
                    && contact.contact.world == self.chunk.world
                    && contact.contact.simulation_tick == self.reference.tick
            })
    }

    fn read<S: ActionState>(
        &self,
        view: &S,
        target: Target,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<Cell, CallbackCaptureError> {
        if !view.available(TruthView::Local, target) {
            return Err(CallbackCaptureError::InvalidWrite(target));
        }
        let cell = view
            .read(TruthView::Local, target)
            .ok_or(CallbackCaptureError::InvalidWrite(target))?;
        facts.insert(target, Some(cell.clone()));
        Ok(cell)
    }

    fn block<S: ActionState>(
        &self,
        view: &S,
        position: BlockPos,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<BlockStateId, CallbackCaptureError> {
        let target = Target::Chunk {
            key: ChunkKey::new(
                self.chunk.world,
                position.0.x.div_euclid(16),
                position.0.z.div_euclid(16),
            ),
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
                x: position.0.x,
                y: position.0.y,
                z: position.0.z,
            }),
            inventory: None,
        };
        let Value::Block(state) = self.read(view, target, facts)?.value else {
            return Err(CallbackCaptureError::InvalidWrite(target));
        };
        BlockStateId::new(state).ok_or(CallbackCaptureError::InvalidWrite(target))
    }

    fn landing_state<S: ActionState>(
        &self,
        view: &S,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<Option<BlockStateId>, CallbackCaptureError> {
        let (_, position) = self.location();
        let ObjectTerminal::FallingLanded { block_state, .. } = self.terminal else {
            return Ok(None);
        };
        let target = self.block(view, position, facts)?;
        let support = self.block(
            view,
            self.support.ok_or(CallbackCaptureError::Budget)?,
            facts,
        )?;
        if !target.to_state().replaceable()
            || crate::block::blocks::falling::FallingBlock::can_fall_through(
                support.to_state(),
                support.to_block(),
            )
        {
            return Ok(None);
        }
        let mut state = BlockStateId::new(block_state).ok_or(CallbackCaptureError::Budget)?;
        use pumpkin_data::tag::Taggable;
        if state
            .to_block()
            .has_tag(&pumpkin_data::tag::Block::MINECRAFT_CONCRETE_POWDERS)
        {
            let mut solidify =
                crate::block::blocks::falling::FallingBlock::can_solidify(target.to_state());
            for direction in pumpkin_data::BlockDirection::all() {
                if direction == pumpkin_data::BlockDirection::Down {
                    continue;
                }
                let neighbor = self.block(view, position.offset(direction.to_offset()), facts)?;
                solidify |=
                    crate::block::blocks::falling::FallingBlock::can_solidify(neighbor.to_state());
            }
            if solidify {
                let name = state
                    .to_block()
                    .name
                    .strip_suffix("_powder")
                    .ok_or(CallbackCaptureError::Budget)?;
                state = Block::from_name(name)
                    .ok_or(CallbackCaptureError::Budget)?
                    .default_state
                    .id;
            }
        }
        Ok(Some(state))
    }

    pub(super) fn capture<S: ActionState>(
        &self,
        view: &S,
        snapshot: &mut crate::block::neighbor::NeighborSnapshot,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<CallbackCapture, CallbackCaptureError> {
        if !self.valid() {
            return Err(CallbackCaptureError::Budget);
        }
        let target = Target::Chunk {
            key: self.chunk,
            part: ChunkPart::Entity(self.body.id),
            inventory: None,
        };
        self.read(view, target, facts)?;
        let mut effects = Vec::new();
        match self.terminal {
            ObjectTerminal::Explosion { power, .. } => {
                let geometry = super::cluster_world_explosion::ExplosionGeometry {
                    world: self.chunk.world,
                    center: self.spatial.position.map(f64::from_bits),
                    power: f32::from_bits(power),
                    seed: self.body.id.local_id as u64
                        ^ self.reference.id.incarnation.rotate_left(17)
                        ^ u64::from(self.reference.id.sequence).rotate_left(43),
                    min_y: self.world.dimension.min_y,
                    height: self.world.dimension.height,
                    preserve_rails: matches!(self.body.payload, EntityPayload::Vehicle(_)),
                };
                let plan =
                    super::cluster_world_explosion::capture_block_rays(view, &geometry, 4096)?;
                facts.extend(plan.facts);
                for position in plan.affected {
                    effects.push(crate::block::neighbor::BlockCallbackEffect::BreakBlock {
                        position: BlockPos::new(position.x, position.y, position.z),
                        flags: pumpkin_world::world::BlockFlags::NOTIFY_ALL,
                    });
                }
            }
            ObjectTerminal::FallingLanded { .. } => {
                if let Some(state) = self.landing_state(view, facts)? {
                    effects.push(crate::block::neighbor::BlockCallbackEffect::SetBlock {
                        position: self.location().1,
                        state,
                        flags: pumpkin_world::world::BlockFlags::NOTIFY_ALL,
                    });
                }
            }
            ObjectTerminal::FallingExpired { .. } => {}
        }
        let capture = super::cluster_world_callbacks::capture_effects(
            view,
            self.chunk.world,
            &self.world.block_registry,
            snapshot,
            facts,
            &effects,
            4096,
        )?;
        if let CallbackCapture::Ready(mut resolution) = capture {
            if self.chain_parameters().is_some() {
                resolution
                    .breaks
                    .retain(|output| output.before.to_block() != &Block::TNT);
            }
            if let ObjectTerminal::FallingExpired {
                block_state,
                drop_item,
            }
            | ObjectTerminal::FallingLanded {
                block_state,
                drop_item,
            } = self.terminal
            {
                let placed = resolution
                    .writes
                    .iter()
                    .any(|(position, _, _)| *position == self.location().1);
                if drop_item && self.witness.do_entity_drops && !placed {
                    let state =
                        BlockStateId::new(block_state).ok_or(CallbackCaptureError::Budget)?;
                    if state.to_block().item_id != 0 {
                        resolution
                            .breaks
                            .push(crate::block::neighbor::NaturalBreakResult {
                                position: self.location().1,
                                before: state,
                                items: vec![crate::world::loot::LootItemFact {
                                    item: state.to_block().item_id,
                                    count: 1,
                                }],
                                experience: 0,
                                particles: false,
                            });
                    }
                }
            }
            Ok(CallbackCapture::Ready(resolution))
        } else {
            Ok(capture)
        }
    }

    fn append_explosion_damage<S: ActionState>(
        &self,
        view: &S,
        action: &mut Action,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<(), ApplyFailure> {
        let ObjectTerminal::Explosion { power, .. } = self.terminal else {
            return Ok(());
        };
        let center = self.spatial.position.map(f64::from_bits);
        let diameter = f64::from(f32::from_bits(power)) * 2.0;
        let mut combat = pumpkin_cluster::combat_action::CombatIntent::default();
        for observed in self.contacts.iter() {
            let contact = &observed.contact;
            let position = contact.position.map(f64::from_bits);
            if position
                .iter()
                .zip(center)
                .map(|(at, origin)| (at - origin).powi(2))
                .sum::<f64>()
                > diameter * diameter
            {
                continue;
            }
            let cell = view
                .read(TruthView::Local, contact.target())
                .ok_or(ApplyFailure::Condition(contact.target()))?;
            let Some(living) = pumpkin_cluster::living_property::state(&cell.value) else {
                continue;
            };
            if living.combat.health_milli == 0 {
                continue;
            }
            let plan = pumpkin_cluster::explosion_exposure::ExplosionExposurePlan {
                center: self.spatial.position,
                contact: contact.clone(),
            };
            let mut exposure_facts = BTreeMap::new();
            for target in plan.targets()? {
                let cell = view
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?;
                exposure_facts.insert(target, cell.clone());
                facts.insert(target, Some(cell));
            }
            let exposure = plan.derive(view, &exposure_facts)?;
            let resistance_id =
                u16::from(pumpkin_data::attributes::Attributes::EXPLOSION_KNOCKBACK_RESISTANCE.id);
            let resistance = living.combat.equipment.attribute(
                resistance_id,
                living
                    .combat
                    .effective_attributes
                    .get(&resistance_id)
                    .map_or(0.0, |base| f64::from_bits(*base)),
            );
            let Some(damage) = pumpkin_cluster::combat_action::explosion_damage(
                contact,
                self.reference.id.actor,
                center,
                diameter,
                exposure,
                observed.eye_height,
                resistance,
                u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
                u16::from(pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id),
            )?
            else {
                continue;
            };
            combat
                .damage_contacts
                .push(pumpkin_cluster::combat_action::CombatDamageContact {
                    contact: contact.clone(),
                    damage,
                    attack_origin: self.spatial.position,
                    victim_yaw: observed.yaw,
                });
            match &contact.identity {
                pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => {
                    action.player_leases.push(*lease);
                    combat.equipment.push(
                        pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                            *lease,
                            contact.chunk,
                            view.inventory_equipment_attribute_ids(),
                        ),
                    );
                }
                pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => {
                    action.conditions.push(Condition {
                        target: contact.target(),
                        expected: Expected::EntityBirthOwned {
                            birth: birth.clone(),
                            owner: contact.owner,
                        },
                    });
                }
            }
            action.conditions.push(Condition {
                target: contact.target(),
                expected: Expected::SpatialEpochEquals(contact.epoch),
            });
        }
        self.finish_combat(view, action, facts, combat)
    }

    fn finish_combat<S: ActionState>(
        &self,
        view: &S,
        action: &mut Action,
        facts: &BTreeMap<Target, Option<Cell>>,
        mut combat: pumpkin_cluster::combat_action::CombatIntent,
    ) -> Result<(), ApplyFailure> {
        if combat.damage_contacts.is_empty() {
            return Ok(());
        }
        combat.equipment.sort_by_key(|binding| binding.lease);
        combat.equipment.dedup_by_key(|binding| binding.lease);
        for condition in &mut action.conditions {
            if let Expected::ObjectTerminal(witness) = &mut condition.expected {
                witness.combat = combat.clone();
            }
        }
        let mut values = BTreeMap::new();
        for target in combat.targets() {
            if let Some(cell) = view.read(TruthView::Local, target) {
                values.insert(target, cell);
            }
        }
        for target in combat.resolution_targets() {
            let cell = view
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            action.conditions.push(Condition {
                target,
                expected: Expected::Exact(cell.clone()),
            });
            values.insert(target, cell);
        }
        action
            .effects
            .extend(combat.derive_with(view, action.id, action.tick, &values)?);
        for (&target, cell) in facts.iter() {
            if let Some(cell) = cell {
                if matches!(cell.value, Value::Block(_)) {
                    action.conditions.push(Condition {
                        target,
                        expected: Expected::Exact(cell.clone()),
                    });
                }
            }
        }
        Ok(())
    }

    fn append_falling_damage<S: ActionState>(
        &self,
        view: &S,
        action: &mut Action,
        facts: &BTreeMap<Target, Option<Cell>>,
    ) -> Result<(), ApplyFailure> {
        use pumpkin_cluster::combat_action::{
            CombatDamageContact, CombatIntent, DamageIntent, DamageProtection, DamageReductionFacts,
        };
        let Some((kind, amount)) = self.witness.falling_damage(view) else {
            return Ok(());
        };
        let mut combat = CombatIntent::default();
        for observed in self
            .contacts
            .iter()
            .filter(|observed| self.witness.falling_intersects(&observed.contact))
        {
            let contact = &observed.contact;
            let cell = view
                .read(TruthView::Local, contact.target())
                .ok_or(ApplyFailure::Condition(contact.target()))?;
            let Some(living) = pumpkin_cluster::living_property::state(&cell.value) else {
                continue;
            };
            if living.combat.health_milli == 0 {
                continue;
            }
            combat.damage_contacts.push(CombatDamageContact {
                contact: contact.clone(),
                attack_origin: self.spatial.position,
                victim_yaw: observed.yaw,
                damage: DamageIntent {
                    kind,
                    source: self.reference.id.actor,
                    target: contact.target(),
                    tick: self.reference.tick,
                    damage_milli: amount,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: false,
                    fire_ticks: 0,
                    bypass_invulnerability: false,
                    reduction: Some(DamageReductionFacts {
                        armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
                        toughness_attribute: u16::from(
                            pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id,
                        ),
                        bypass_armor: false,
                        bypass_effects: false,
                        bypass_enchantments: false,
                        protection: DamageProtection::General,
                        breach: 0,
                    }),
                },
            });
            match &contact.identity {
                pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => {
                    action.player_leases.push(*lease);
                    combat.equipment.push(
                        pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                            *lease,
                            contact.chunk,
                            view.inventory_equipment_attribute_ids(),
                        ),
                    );
                }
                pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => {
                    action.conditions.push(Condition {
                        target: contact.target(),
                        expected: Expected::EntityBirthOwned {
                            birth: birth.clone(),
                            owner: contact.owner,
                        },
                    })
                }
            }
            action.conditions.push(Condition {
                target: contact.target(),
                expected: Expected::SpatialEpochEquals(contact.epoch),
            });
        }
        self.finish_combat(view, action, facts, combat)
    }

    pub(super) fn complete<S: ActionState>(
        &self,
        view: &S,
        facts: &mut BTreeMap<Target, Option<Cell>>,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
    ) -> Result<Action, ApplyFailure> {
        let mut action = super::cluster_world_callbacks::build_callback_action(
            view,
            self.reference,
            self.producer,
            Some(self.chunk),
            None,
            self.chunk.world,
            facts,
            resolution,
        )?;
        let leg = if matches!(self.body.payload, EntityPayload::Vehicle(_)) {
            pumpkin_cluster::object_tick::tnt_cart_terminal_leg(&self.witness, view)?
        } else {
            pumpkin_cluster::object_tick::terminal_leg_at(
                &self.body,
                self.chunk,
                view,
                &self.terminal,
                self.reference.tick,
                f64::from_bits(self.spatial.position[1]),
                self.world.dimension.min_y,
                self.world.dimension.min_y + self.world.dimension.height,
            )?
        };
        let detach = pumpkin_cluster::actor_relation::capture_owner_detach_from_participants(
            view,
            &self.body,
            self.chunk,
            &self.relation_participants,
        )
        .ok_or(ApplyFailure::InvalidAction)?;
        let retirement = self.witness.retirement().capture_action_with_detach(
            view,
            action.id,
            action.tick,
            pumpkin_cluster::canonical_action::ClientOrigin::None,
            Some(detach),
        )?;
        action.conditions.extend(retirement.conditions);
        action.effects.extend(retirement.effects);
        action.player_leases.extend(retirement.player_leases);
        action.stakeholders.extend(retirement.stakeholders);
        action.conditions.extend(leg.conditions);
        action.conditions.push(Condition {
            target: self.witness.target(),
            expected: Expected::ObjectTerminal(Box::new(self.witness.clone())),
        });
        action.effects.extend(
            leg.effects
                .into_iter()
                .filter(|effect| !matches!(effect, Effect::Delete { .. })),
        );
        action.dependencies.push(self.body.birth.action);
        if let ObjectTerminal::Explosion { power, .. } = self.terminal {
            let (_, position) = self.location();
            let chunk = ChunkKey::new(
                self.chunk.world,
                position.0.x.div_euclid(16),
                position.0.z.div_euclid(16),
            );
            action.stakeholders.push(Stakeholder::Chunk(chunk));
            action.presentation.push(
                pumpkin_cluster::world_presentation::WorldPresentationIntent::Explosion {
                    chunk,
                    center: self.spatial.position,
                    power,
                    block_count: resolution.writes.len() as u32,
                },
            );
        }
        self.append_explosion_damage(view, &mut action, facts)?;
        self.append_falling_damage(view, &mut action, facts)?;
        action.conditions.sort_by_key(|condition| condition.target);
        let mut seen = BTreeMap::<Target, Vec<Expected>>::new();
        action.conditions.retain(|condition| {
            let values = seen.entry(condition.target).or_default();
            if values.contains(&condition.expected) {
                false
            } else {
                values.push(condition.expected.clone());
                true
            }
        });
        action.player_leases.sort_unstable();
        action.player_leases.dedup();
        action
            .stakeholders
            .extend(action.targets().into_iter().map(Target::stakeholder));
        action.stakeholders.sort_unstable();
        action.stakeholders.dedup();
        super::cluster_world_callbacks::append_resolution_light(
            view,
            &self.world,
            self.chunk.world,
            &mut action,
            resolution,
        )?;
        Ok(action)
    }
}

impl ObjectTerminalContext {
    fn chain_parameters(&self) -> Option<pumpkin_cluster::object_state::TntState> {
        let ObjectTerminal::Explosion { igniter, .. } = &self.terminal else {
            return None;
        };
        Some(pumpkin_cluster::object_state::TntState {
            fuse: 80,
            power: 4.0f32.to_bits(),
            block_state: Block::TNT.default_state.id.as_u16(),
            igniter: igniter.clone(),
        })
    }

    fn chain_positions(
        &self,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
    ) -> Vec<BlockPos> {
        if self.chain_parameters().is_none() {
            return Vec::new();
        }
        resolution
            .writes
            .iter()
            .filter_map(|(position, before, after)| {
                (before.to_block() == &Block::TNT && *after == BlockStateId::AIR)
                    .then_some(*position)
            })
            .collect()
    }

    pub(super) fn extra_birth_count(
        &self,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
    ) -> usize {
        self.chain_positions(resolution).len()
    }

    pub(super) fn append_births<S: ActionState>(
        &self,
        _view: &S,
        action: &mut Action,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
        identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity],
    ) -> Result<(), ApplyFailure> {
        let positions = self.chain_positions(resolution);
        if positions.len() != identities.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        if positions.is_empty() {
            return Ok(());
        }
        let tnt = self.chain_parameters().ok_or(ApplyFailure::InvalidAction)?;
        for (position, identity) in positions.into_iter().zip(identities) {
            let chunk = ChunkKey::new(
                self.chunk.world,
                position.0.x.div_euclid(16),
                position.0.z.div_euclid(16),
            );
            let mut next = tnt.clone();
            next.fuse =
                10 + (identity.id.local_id as u64 ^ u64::from(action.id.sequence)) as u16 % 21;
            let body = super::cluster_object_birth::prepare_reserved(
                identity,
                self.reference,
                self.producer,
                pumpkin_data::entity::EntityType::TNT,
                EntitySpatial {
                    world: self.chunk.world,
                    position: [
                        f64::from(position.0.x) + 0.5,
                        f64::from(position.0.y),
                        f64::from(position.0.z) + 0.5,
                    ]
                    .map(f64::to_bits),
                    velocity_micro: [0, 200_000, 0],
                    yaw: 0.0f32.to_bits(),
                    pitch: 0.0f32.to_bits(),
                    on_ground: false,
                },
                pumpkin_cluster::object_state::ObjectState {
                    simulation_tick: Some(action.tick),
                    property_versions: Default::default(),
                    kind: pumpkin_cluster::object_state::ObjectKind::PrimedTnt(next),
                },
                chunk,
            )?;
            let target = Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            };
            action.conditions.push(Condition {
                target,
                expected: Expected::Missing,
            });
            action.effects.push(Effect::EntityAdmission {
                target,
                body,
                inventory: pumpkin_cluster::entity_inventory_admission::EntityInventoryAdmission {
                    imported_revision: 0,
                    cells: Vec::new(),
                },
            });
            action.stakeholders.push(Stakeholder::Chunk(chunk));
        }
        action.stakeholders.sort_unstable();
        action.stakeholders.dedup();
        Ok(())
    }
}
