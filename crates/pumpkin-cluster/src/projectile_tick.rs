use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};
use crate::actor::ActorId;
use crate::canonical_action::*;
use crate::combat_action::{CombatIntent, DamageIntent, DamageKind, DamageProtection, DamageReductionFacts};
use crate::entity_body::EntitySpatial;
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectileTickIntent {
    pub projectile: ContactObservation,
    pub original: EntitySpatial,
    pub blocks: Vec<Target>,
    pub victims: Vec<ContactObservation>,
    pub victim_yaws: BTreeMap<ActorId, u32>,
    pub equipment: Vec<crate::canonical_inventory::InventoryEquipmentBinding>,
    pub armor_attribute: u16,
    pub toughness_attribute: u16,
    pub rules: crate::world_rules::WorldRulesWitness,
    pub births: Vec<crate::entity_origin::EntityOriginIdentity>,
    pub status_attributes: Vec<u16>,
    pub max_health_attribute: u16,
    pub shooter: Option<ContactObservation>,
}

impl ProjectileTickIntent {
    pub fn block_targets(original: &EntitySpatial) -> Result<Vec<Target>, ApplyFailure> {
        let position = original.position.map(f64::from_bits);
        if position.iter().any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
            || original.velocity_micro.iter().any(|value| value.unsigned_abs() > 100_000_000) {
            return Err(ApplyFailure::InvalidAction);
        }
        let end: [f64; 3] = std::array::from_fn(|axis| position[axis] + original.velocity_micro[axis] as f64 / 1_000_000.0);
        let lower: [i32; 3] = std::array::from_fn(|axis| (position[axis].min(end[axis]) - if axis == 1 { 0.8 } else { 0.3 }).floor() as i32);
        let upper: [i32; 3] = std::array::from_fn(|axis| (position[axis].max(end[axis]) + if axis == 1 { 1.3 } else { 0.3 }).floor() as i32);
        let count = (0..3).try_fold(1_usize, |count, axis| count.checked_mul((upper[axis] - lower[axis] + 1) as usize))
            .ok_or(ApplyFailure::InvalidAction)?;
        if count > 4096 { return Err(ApplyFailure::Capacity); }
        let mut targets = Vec::with_capacity(count);
        for x in lower[0]..=upper[0] {
            for y in lower[1]..=upper[1] {
                for z in lower[2]..=upper[2] {
                    targets.push(Target::Chunk { key: crate::protocol::ChunkKey::new(original.world, x.div_euclid(16), z.div_euclid(16)),
                        part: ChunkPart::Block(crate::protocol::BlockPos { x, y, z }), inventory: None });
                }
            }
        }
        targets.sort_unstable();
        Ok(targets)
    }

    pub fn damage(&self, victim: &ContactObservation, source: ActorId, tick: TickStamp, amount: u32,
        knockback_micro: [i64; 3], fire_ticks: u32) -> DamageIntent {
        DamageIntent { kind: DamageKind::Projectile, source, target: victim.target(), tick, damage_milli: amount,
            knockback_micro, fire_ticks, bypass_hurt_cooldown: false, bypass_invulnerability: false,
            reduction: Some(DamageReductionFacts { armor_attribute: self.armor_attribute,
                toughness_attribute: self.toughness_attribute, bypass_armor: false, bypass_effects: false,
                bypass_enchantments: false, protection: DamageProtection::Projectile, breach: 0 }) }
    }

    pub fn outside_world_height(rules: &crate::world_rules::WorldRulesSnapshot, target: Target) -> bool {
        matches!(target, Target::Chunk { key, part: ChunkPart::Block(position), inventory: None }
            if key.world == rules.rules.world && (position.y < rules.min_y || i64::from(position.y) >= i64::from(rules.min_y) + i64::from(rules.height)))
    }

    pub fn victim_targets(&self) -> BTreeSet<Target> {
        let combat = CombatIntent { damages: self.victims.iter().map(|victim|
            self.damage(victim, self.projectile.actor(), self.projectile.simulation_tick, 1, [0;3], 0)).collect(), ..Default::default() };
        combat.targets().chain(self.equipment.iter().flat_map(crate::canonical_inventory::InventoryEquipmentBinding::targets))
            .chain(self.victims.iter().flat_map(|victim| {
                use crate::living_property::LivingProperty as P;
                [P::StatusEffects, P::Strength, P::Weakness, P::Resistance, P::FireResistance, P::Blindness].into_iter()
                    .chain(self.status_attributes.iter().copied().flat_map(|id| [P::Attribute(id), P::PermanentAttributeModifiers(id)])).map(move |property| Target::Chunk {
                        key: victim.chunk, part: ChunkPart::LivingProperty { actor: victim.actor(), property }, inventory: None })
            })).chain(self.victims.iter().flat_map(|victim| crate::combat_action::instant_health_targets(victim.target(), self.max_health_attribute))).collect()
    }

    pub fn permits_transform(&self, transform: &crate::canonical_inventory::ItemTransformIntent) -> bool {
        let Target::Player { lease: Some(lease), .. } = transform.source.target else { return false; };
        let Ok(index) = self.equipment.binary_search_by_key(&lease, |binding| binding.lease) else { return false; };
        let binding = &self.equipment[index];
        transform.valid() && transform.remainder_destinations.is_empty()
            && transform.source.slot.target == crate::inventory::InventoryTarget::Player(binding.lease.gid)
            && match transform.operation {
                    crate::canonical_inventory::ItemTransformOperation::ArmorDamage { armor_slot, .. } => binding.armor.get(usize::from(armor_slot)) == Some(&transform.source.target),
                    crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. } => binding.offhand == transform.source.target || binding.hotbar.contains(&transform.source.target),
                    _ => false,
                }
    }

    pub fn targets(&self) -> BTreeSet<Target> {
        std::iter::once(self.projectile.target()).chain(self.blocks.iter().copied()).chain(self.victim_targets())
            .chain(self.birth_targets()).chain(self.shooter_targets()).collect()
    }

    pub fn shooter_targets(&self) -> BTreeSet<Target> {
        let Some(shooter) = &self.shooter else { return BTreeSet::new(); };
        let combat = CombatIntent { damages: vec![self.damage(shooter, shooter.actor(), self.projectile.simulation_tick, 5000, [0;3], 0)], ..Default::default() };
        combat.targets().chain(self.teleport_destinations()).collect()
    }

    pub fn teleport_destinations(&self) -> BTreeSet<Target> {
        let Some(shooter) = &self.shooter else { return BTreeSet::new(); };
        let part = match shooter.actor() { ActorId::Player(player) => ChunkPart::Player(player), ActorId::Entity(entity) => ChunkPart::Entity(entity), _ => return BTreeSet::new() };
        Self::block_targets(&self.original).unwrap_or_default().into_iter().filter_map(|target| match target {
            Target::Chunk { key, .. } => Some(Target::Chunk { key, part, inventory: None }), _ => None }).collect()
    }

    pub fn permits_teleport(&self, teleport: &crate::projectile_teleport::ProjectileShooterTeleport) -> bool {
        teleport.valid() && self.shooter.as_ref() == Some(&teleport.shooter)
            && self.teleport_destinations().contains(&teleport.destination_target())
            && teleport.projectile_epoch == self.projectile.epoch
            && matches!(&self.projectile.identity, ContactIdentity::Entity { birth, .. } if *birth == teleport.projectile_birth)
    }

    pub fn birth_targets(&self) -> BTreeSet<Target> {
        let chunks = Self::block_targets(&self.original).unwrap_or_default().into_iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(key), _ => None })
            .chain(std::iter::once(self.projectile.chunk)).collect::<BTreeSet<_>>();
        chunks.into_iter().flat_map(|key| self.births.iter().map(move |identity| Target::Chunk {
            key, part: ChunkPart::Entity(identity.id), inventory: None })).collect()
    }

    pub fn valid(&self, id: ActionId, tick: TickStamp) -> bool {
        self.projectile.valid() && self.projectile.actor() == id.actor && self.projectile.simulation_tick == tick
            && matches!(self.projectile.identity, ContactIdentity::Entity { id: entity, .. } if entity.generation == id.incarnation)
            && self.original.world == self.projectile.world && self.original.position == self.projectile.position
            && f32::from_bits(self.original.yaw).is_finite() && f32::from_bits(self.original.pitch).is_finite()
            && self.blocks.windows(2).all(|pair| pair[0] < pair[1])
            && Self::block_targets(&self.original).is_ok_and(|targets| {
                let targets = targets.into_iter().collect::<BTreeSet<_>>();
                self.blocks.iter().all(|target| targets.contains(target))
            })
            && self.rules.rules.world == self.projectile.world && self.rules.slot as u16 == tick.0
            && self.rules.rules.epoch != 0 && self.rules.rules.issuer.generation != 0
            && self.births.len() <= 4
            && self.status_attributes.len() <= 256
            && self.status_attributes.iter().collect::<BTreeSet<_>>().len() == self.status_attributes.len()
            && self.shooter.as_ref().is_none_or(|shooter| shooter.valid() && shooter.world == self.projectile.world && shooter.simulation_tick == tick)
            && self.births.iter().all(|identity| identity.source == self.projectile.owner && identity.id.origin == identity.source.server
                && identity.id.generation == identity.source.generation && identity.id.local_id > 0 && !identity.uuid.is_nil())
            && self.births.iter().map(|identity| identity.id).collect::<BTreeSet<_>>().len() == self.births.len()
            && self.victims.len() <= 128
            && self.victims.iter().all(|victim| victim.valid() && victim.simulation_tick == tick
                && victim.world == self.projectile.world && victim.actor() != id.actor)
            && self.victims.iter().map(ContactObservation::actor).collect::<BTreeSet<_>>().len() == self.victims.len()
            && self.victim_yaws.len() == self.victims.len()
            && self.victims.iter().all(|victim| self.victim_yaws.get(&victim.actor()).is_some_and(|yaw| f32::from_bits(*yaw).is_finite()))
            && self.equipment.len() <= self.victims.len()
            && self.equipment.windows(2).all(|pair| pair[0].lease < pair[1].lease)
            && self.equipment.iter().all(|equipment| equipment.is_valid()
                && self.victims.iter().any(|victim| victim.identity == ContactIdentity::Player(equipment.lease)
                    && victim.target() == equipment.living))
    }

    pub fn derive<S: ActionState>(&self, context: &S, id: ActionId, tick: TickStamp, values: &BTreeMap<Target, Cell>)
        -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid(id, tick) { return Err(ApplyFailure::InvalidAction); }
        let rules = context.world_rules(self.rules, tick)?;
        let required = Self::block_targets(&self.original)?.into_iter().filter(|target| !Self::outside_world_height(&rules, *target)).collect::<Vec<_>>();
        if self.blocks != required { return Err(ApplyFailure::InvalidAction); }
        let effects = context.derive_projectile_tick(self, id, tick, values)?;
        let victims = self.victim_targets().into_iter().chain(self.shooter_targets()).collect::<BTreeSet<_>>();
        if effects.iter().any(|effect| match effect {
            Effect::Put { target, value: Value::Entity(body) } if *target == self.projectile.target() =>
                body.birth != match &self.projectile.identity { ContactIdentity::Entity { birth, .. } => birth.clone(), _ => return true }
                    || body.owner != self.projectile.owner || body.spatial_epoch != self.projectile.epoch,
            Effect::Put { target, value: Value::Entity(body) } => !self.birth_targets().contains(target)
                || !self.births.iter().any(|identity| identity.id == body.id && identity.uuid == body.birth.uuid && identity.source == body.birth.source)
                || body.birth.action != (ActionRef { tick, id }) || !matches!(body.payload, crate::entity_body::EntityPayload::Experience(_)),
            Effect::Put { target, value: Value::LivingProperty(_) } => !victims.contains(target),
            Effect::Delete { target } => *target != self.projectile.target(),
            Effect::TransformItems { intent } => !self.permits_transform(intent),
            Effect::ProjectileShooterTeleport { intent } => !self.permits_teleport(intent),
            _ => true,
        }) { return Err(ApplyFailure::InvalidAction); }
        Ok(effects)
    }
}

pub fn capture<S: ActionState>(context: &S, id: ActionId, tick: TickStamp, intent: ProjectileTickIntent)
    -> Result<Action, ApplyFailure> {
    if !intent.valid(id, tick) { return Err(ApplyFailure::InvalidAction); }
    let mut values = BTreeMap::new();
    let mut conditions = Vec::new();
    for target in intent.targets() {
        if intent.birth_targets().contains(&target) {
            if context.read(TruthView::Local, target).is_some() { return Err(ApplyFailure::Condition(target)); }
            conditions.push(Condition { target, expected: Expected::Missing });
            continue;
        }
        if intent.teleport_destinations().contains(&target) && intent.shooter.as_ref().is_none_or(|shooter| shooter.target() != target) {
            if context.read(TruthView::Local, target).is_some() { return Err(ApplyFailure::Condition(target)); }
            conditions.push(Condition { target, expected: Expected::Missing });
            continue;
        }
        let cell = context.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
        conditions.push(Condition { target, expected: Expected::Exact(cell.clone()) });
        values.insert(target, cell);
    }
    let ContactIdentity::Entity { birth, .. } = &intent.projectile.identity else { return Err(ApplyFailure::InvalidAction); };
    conditions.push(Condition { target: intent.projectile.target(), expected: Expected::ActorControlled { birth: birth.clone(), issuer: intent.projectile.owner } });
    conditions.push(Condition { target: intent.projectile.target(), expected: Expected::SpatialEpochEquals(intent.projectile.epoch) });
    let mut leases = Vec::new();
    let mut player_chunks = Vec::new();
    for victim in &intent.victims {
        conditions.push(Condition { target: victim.target(), expected: Expected::SpatialEpochEquals(victim.epoch) });
        match &victim.identity {
            ContactIdentity::Player(lease) => { leases.push(*lease); player_chunks.push((lease.key, victim.chunk)); },
            ContactIdentity::Entity { birth, .. } => conditions.push(Condition { target: victim.target(), expected: Expected::EntityBirthOwned { birth: birth.clone(), owner: victim.owner } }),
        }
    }
    if let Some(shooter) = &intent.shooter {
        if !conditions.iter().any(|condition| condition.target == shooter.target() && condition.expected == Expected::SpatialEpochEquals(shooter.epoch)) {
            conditions.push(Condition { target: shooter.target(), expected: Expected::SpatialEpochEquals(shooter.epoch) });
        }
        match &shooter.identity {
            ContactIdentity::Player(lease) => {
                if !leases.contains(lease) { leases.push(*lease); player_chunks.push((lease.key, shooter.chunk)); }
            }
            ContactIdentity::Entity { birth, .. } => {
                let expected = Expected::EntityBirthOwned { birth: birth.clone(), owner: shooter.owner };
                if !conditions.iter().any(|condition| condition.target == shooter.target() && condition.expected == expected) {
                    conditions.push(Condition { target: shooter.target(), expected });
                }
            }
        }
    }
    let footprint = intent.targets().into_iter().collect::<Vec<_>>();
    let action = Action { edit_authority: None, id, tick, class: ActionClass::Replayable, kind: ActionKind::Item,
        chunk_transition: false, actor_chunk: Some(intent.projectile.chunk), player_leases: leases, player_chunks,
        stakeholders: footprint.iter().map(|target| target.stakeholder()).collect::<BTreeSet<_>>().into_iter().collect(),
        dependencies: match intent.projectile.epoch { SpatialEpoch::Action(reference) => vec![reference], _ => Vec::new() }, conditions,
        effects: intent.derive(context, id, tick, &values)?, counterfactual_conditions: footprint.clone(), counterfactual_effects: footprint,
        semantic_intent: Some(SemanticIntent::ProjectileTick(intent)), factual_result: None, undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: ClientOrigin::None, presentation: Vec::new() };
    action.validate()?;
    Ok(action)
}
