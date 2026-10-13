use std::collections::BTreeSet;

use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{
    ActionState, InventoryId, Target, TruthView, Value,
};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_cluster::item_contact::{ContactIdentity, ContactObservation};
use pumpkin_cluster::mob_continuation::{MobBrain, MobFollowMode, MobSpecies};
use pumpkin_cluster::mob_goal::MobGoalEnvironment;
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};

use super::cluster_owned_mob_world::{MobWorldRules, MobWorldView};

pub(crate) struct OwnedMobGoalEnvironment<'a, S> {
    source: &'a S,
    draft: &'a ActorActionDraft,
    body: &'a EntityBody,
    living: &'a ActorLivingState,
    contact: &'a ContactObservation,
    contacts: &'a super::cluster_owned_mob_contacts::MobContactIndex,
    chunks: &'a BTreeSet<ChunkKey>,
    rules: MobWorldRules,
    swimming: bool,
    failure: std::cell::Cell<Option<pumpkin_cluster::canonical_action::ApplyFailure>>,
}

impl<'a, S: ActionState> OwnedMobGoalEnvironment<'a, S> {
    pub(crate) fn new(
        source: &'a S,
        draft: &'a ActorActionDraft,
        body: &'a EntityBody,
        living: &'a ActorLivingState,
        contact: &'a ContactObservation,
        contacts: &'a super::cluster_owned_mob_contacts::MobContactIndex,
        chunks: &'a BTreeSet<ChunkKey>,
        rules: MobWorldRules,
        swimming: bool,
    ) -> Self {
        Self {
            source,
            draft,
            body,
            living,
            contact,
            contacts,
            chunks,
            rules,
            swimming,
            failure: std::cell::Cell::new(None),
        }
    }

    pub(crate) fn finish(self) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.failure.into_inner().map_or(Ok(()), Err)
    }

    fn checked<T>(&self, result: Result<T, pumpkin_cluster::canonical_action::ApplyFailure>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(reason) => {
                let previous = self.failure.replace(Some(reason));
                if previous.is_some() { self.failure.set(previous); }
                None
            }
        }
    }

    fn cell(&self, target: Target) -> Option<pumpkin_cluster::canonical_action::Cell> {
        self.checked(self.draft.view(self.source).read(TruthView::Local, target)
            .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Condition(target)))
    }

    fn read_block(&self, position: BlockPos) -> Option<pumpkin_data::BlockStateId> {
        self.checked(self.world().block(position).map_err(Into::into))
    }

    fn world(&self) -> MobWorldView<'_, S> {
        MobWorldView::new(self.source, self.draft, self.contact.world, self.chunks)
            .with_rules(self.rules)
    }

    fn observation(&self, identity: &ContactIdentity) -> Option<&ContactObservation> {
        self.contacts.get(identity)
            .filter(|contact| contact.world == self.contact.world && self.chunks.contains(&contact.chunk))
    }

    fn living_at(&self, contact: &ContactObservation) -> Option<ActorLivingState> {
        match self.cell(contact.target())?
            .value
        {
            Value::Living(living) => Some(*living),
            Value::Entity(body) => match body.payload { EntityPayload::Living(living) => Some(living), _ => None },
            _ => None,
        }
    }

    fn eye_height(&self, contact: &ContactObservation) -> Option<f64> {
        let cell = self.cell(contact.target())?;
        let height = match &cell.value {
            Value::Living(living) => Some(f64::from(super::cluster_living_metadata::player_eye_height(living))),
            Value::Entity(body) => match &body.payload {
                EntityPayload::Living(living) => super::cluster_owned_mob_metadata::dimensions(living, body.kind)
                    .map(|dimensions| f64::from(dimensions.eye_height)),
                _ => pumpkin_data::entity::EntityType::from_raw(body.kind).map(|kind| f64::from(kind.eye_height)),
            },
            _ => None,
        };
        self.checked(height.ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Type(contact.target())))
    }

    fn closest(
        &self,
        range: f64,
        mut predicate: impl FnMut(&ContactObservation) -> bool,
    ) -> Option<ContactIdentity> {
        if !range.is_finite() || range < 0.0 { return None; }
        let origin = self.position();
        self.contacts
            .in_chunks(self.chunks)
            .filter(|contact| {
                contact.identity != self.contact.identity
                    && contact.world == self.contact.world
            })
            .filter_map(|contact| {
                let distance = distance_squared(origin, contact.position.map(f64::from_bits));
                (distance <= range * range).then_some((contact, distance))
            })
            .filter(|(contact, _)| predicate(contact))
            .min_by(|(a, ad), (b, bd)| ad.total_cmp(bd).then_with(|| a.actor().cmp(&b.actor())))
            .map(|(contact, _)| contact.identity.clone())
    }

    fn nearest_water(&self) -> Option<[f64; 3]> {
        let origin = self.position().map(|value| value.floor() as i32);
        let at = BlockPos { x: origin[0], y: origin[1], z: origin[2] };
        if self.read_block(at)?.to_state().get_block_collision_shapes().next().is_some() {
            return None;
        }
        let mut best = None;
        for x in -5_i32..=5 {
            for y in -1_i32..=1 {
                for z in -5_i32..=5 {
                    let at = BlockPos { x: origin[0] + x, y: origin[1] + y, z: origin[2] + z };
                    let state = self.read_block(at)?.to_state();
                    if !state.is_waterlogged() && state.id.to_block().id != pumpkin_data::Block::WATER.id { continue; }
                    let distance = x.abs() + y.abs() + z.abs();
                    if best.as_ref().is_none_or(|(previous, _)| distance < *previous) {
                        best = Some((distance, [f64::from(at.x) + 0.5, f64::from(at.y), f64::from(at.z) + 0.5]));
                    }
                }
            }
        }
        best.map(|(_, position)| position)
    }
    fn random_destination(&self, brain: &mut MobBrain, water: bool, horizontal: i32, vertical: i32) -> Option<[f64; 3]> {
        let origin = self.position();
        let mut best = None;
        for _ in 0..10 {
            let offset = [
                brain.next_random() % (horizontal * 2 + 1) as u64,
                brain.next_random() % (vertical * 2 + 1) as u64,
                brain.next_random() % (horizontal * 2 + 1) as u64,
            ];
            let at = BlockPos {
                x: origin[0].floor() as i32 + offset[0] as i32 - horizontal,
                y: origin[1].floor() as i32 + offset[1] as i32 - vertical,
                z: origin[2].floor() as i32 + offset[2] as i32 - horizontal,
            };
            if let Some((center, radius)) = brain.restriction {
                if distance_squared(
                    [
                        f64::from(center.x),
                        f64::from(center.y),
                        f64::from(center.z),
                    ],
                    [f64::from(at.x), f64::from(at.y), f64::from(at.z)],
                ) > f64::from(radius).powi(2)
                {
                    continue;
                }
            }
            let Some(state) = self.read_block(at) else {
                continue;
            };
            let feet = state.to_state();
            let Some(below) = self.read_block(BlockPos { y: at.y - 1, ..at }) else {
                continue;
            };
            let suitable = if water {
                feet.id.to_block().id == pumpkin_data::Block::WATER.id
            } else {
                feet.get_block_collision_shapes().next().is_none()
                    && below
                        .to_state()
                        .get_block_collision_shapes()
                        .next()
                        .is_some()
            };
            if !suitable {
                continue;
            }
            let candidate = [
                f64::from(at.x) + 0.5,
                f64::from(at.y),
                f64::from(at.z) + 0.5,
            ];
            let weight = if below.to_state().id.to_block().id == pumpkin_data::Block::GRASS_BLOCK.id {
                10.0
            } else {
                0.0
            };
            if best.as_ref().is_none_or(|(_, previous)| weight > *previous) {
                best = Some((candidate, weight));
            }
        }
        best.map(|(position, _)| position)
    }
}

impl<S: ActionState> MobGoalEnvironment for OwnedMobGoalEnvironment<'_, S> {
    fn position(&self) -> [f64; 3] {
        self.contact.position.map(f64::from_bits)
    }
    fn eye_position(&self) -> Option<[f64; 3]> {
        let mut position = self.position();
        position[1] += self.eye_height(self.contact)?;
        Some(position)
    }
    fn target_eye_position(&self, target: &ContactIdentity) -> Option<[f64; 3]> {
        let contact = self.observation(target)?;
        let mut position = self.target_position(target)?;
        position[1] += self.eye_height(contact)?;
        Some(position)
    }
    fn block(&self, position: BlockPos) -> Option<u16> {
        self.read_block(position).map(|state| state.as_u16())
    }
    fn edible_for_sheep(&self, state: u16) -> bool {
        pumpkin_data::BlockStateId::new(state).is_some_and(|state| {
            state.to_state().id.to_block().id == pumpkin_data::Block::SHORT_GRASS.id
        })
    }
    fn grass_block(&self, state: u16) -> bool {
        pumpkin_data::BlockStateId::new(state).is_some_and(|state| {
            state.to_state().id.to_block().id == pumpkin_data::Block::GRASS_BLOCK.id
        })
    }
    fn air_state(&self) -> u16 {
        pumpkin_data::Block::AIR.default_state.id.as_u16()
    }
    fn dirt_state(&self) -> u16 {
        pumpkin_data::Block::DIRT.default_state.id.as_u16()
    }
    fn mob_griefing(&self) -> bool {
        self.rules.mob_griefing
    }
    fn swimming(&self) -> bool {
        self.swimming
    }
    fn target_position(&self, target: &ContactIdentity) -> Option<[f64; 3]> {
        let contact = self.observation(target)?;
        self.living_at(contact)
            .filter(|living| living.combat.health_milli != 0)?;
        Some(contact.position.map(f64::from_bits))
    }
    fn nearest(&self, kind: Option<u16>, range: f64) -> Option<ContactIdentity> {
        self.closest(range, |contact| match contact.identity {
            ContactIdentity::Player(_) => {
                kind.is_none_or(|kind| kind == pumpkin_data::entity::EntityType::PLAYER.id)
            }
            ContactIdentity::Entity { .. } => match self.cell(contact.target())
            {
                Some(pumpkin_cluster::canonical_action::Cell {
                    value: Value::Entity(body),
                    ..
                }) => kind.is_none_or(|kind| kind == body.kind),
                _ => false,
            },
        })
    }
    fn attackable(&self, identity: &ContactIdentity) -> bool {
        if self.rules.difficulty == 0 && pumpkin_data::entity::EntityType::from_raw(self.body.kind)
            .is_some_and(|kind| kind.category == &pumpkin_data::entity::MobCategory::MONSTER) { return false; }
        self.observation(identity).and_then(|contact| self.living_at(contact)).is_some_and(|living|
            living.combat.health_milli != 0 && !living.combat.status.invulnerable
                && living.avatar.as_ref().is_none_or(|avatar| !matches!(avatar.game_mode, 1 | 3)))
    }
    fn nearest_attackable(&self, kind: Option<u16>, range: f64) -> Option<ContactIdentity> {
        self.closest(range, |contact| {
            if !self.attackable(&contact.identity) { return false; }
            match &contact.identity {
                ContactIdentity::Player(_) => kind.is_none_or(|kind| kind == pumpkin_data::entity::EntityType::PLAYER.id),
                ContactIdentity::Entity { .. } => matches!(self.cell(contact.target()),
                    Some(pumpkin_cluster::canonical_action::Cell { value: Value::Entity(body), .. }) if kind.is_none_or(|kind| kind == body.kind)),
            }
        })
    }
    fn visible(&self, target: &ContactIdentity) -> bool {
        let Some(contact) = self.observation(target) else {
            return false;
        };
        let mut from = self.position();
        let mut to = contact.position.map(f64::from_bits);
        let Some(source_eye) = self.eye_height(self.contact) else { return false; };
        let Some(target_eye) = self.eye_height(contact) else { return false; };
        from[1] += source_eye;
        to[1] += target_eye;
        self.checked(self.world().visible_between(from, to, 16384).map_err(Into::into)).unwrap_or(false)
    }
    fn breeding_partner(&self) -> Option<ContactIdentity> {
        self.closest(8.0, |contact| {
            let ContactIdentity::Entity { .. } = contact.identity else {
                return false;
            };
            let Some(pumpkin_cluster::canonical_action::Cell {
                value: Value::Entity(body),
                ..
            }) = self.cell(contact.target())
            else {
                return false;
            };
            body.kind == self.body.kind
                && self.living_at(contact).is_some_and(|living| {
                    living.combat.health_milli > 0
                        && living.mob.is_some_and(|mob| {
                            mob.reproduction.growth_age >= 0
                                && mob.reproduction.love_ticks > 0
                                && mob.reproduction.breeding_cooldown == 0
                        })
                })
        })
    }
    fn follow_target(&self, mode: MobFollowMode, range: f64) -> Option<ContactIdentity> {
        match mode {
            MobFollowMode::Parent => self.closest(range, |contact| {
                let Some(pumpkin_cluster::canonical_action::Cell { value: Value::Entity(body), .. }) =
                    self.cell(contact.target()) else { return false; };
                body.kind == self.body.kind && self.living_at(contact).is_some_and(|living|
                    living.mob.is_some_and(|mob| mob.reproduction.growth_age >= 0))
            }),
            MobFollowMode::Owner => {
                let owner = self.living.mob.as_ref().and_then(|mob| match mob.species {
                    MobSpecies::Horse { owner, .. } | MobSpecies::Llama { owner, .. } => owner,
                    _ => None,
                })?;
                self.closest(range, |contact| matches!(contact.identity, ContactIdentity::Player(lease) if lease.key.0 == owner))
            }
            MobFollowMode::DifferentSpecies => self.closest(range, |contact| {
                matches!(self.cell(contact.target()),
                    Some(pumpkin_cluster::canonical_action::Cell { value: Value::Entity(body), .. }) if body.kind != self.body.kind)
            }),
        }
    }
    fn tempted_by(&self, target: &ContactIdentity, items: &[u16]) -> bool {
        let ContactIdentity::Player(lease) = target else {
            return false;
        };
        let view = self.draft.view(self.source);
        let held = self.checked(pumpkin_cluster::canonical_inventory::capture_held_inventory_binding(&view, *lease)).is_some_and(|(_, cell, _)| matches!(cell.value, Value::Inventory(item) if item.count != 0 && items.contains(&item.item)));
        let offhand = Target::Player {
            key: lease.key,
            lease: Some(*lease),
            inventory: Some(InventoryId::Player {
                inv: pumpkin_cluster::inventory::INV_OFFHAND,
                slot: 0,
            }),
        };
        held || matches!(self.cell(offhand), Some(pumpkin_cluster::canonical_action::Cell {
            value: Value::Inventory(item), .. }) if item.count != 0 && items.contains(&item.item))
    }
    fn nearest_tempting(&self, items: &[u16], range: f64) -> Option<ContactIdentity> {
        self.closest(range, |contact| self.tempted_by(&contact.identity, items))
    }
    fn in_attack_range(&self, target: &ContactIdentity) -> bool {
        self.checked((|| {
            use pumpkin_cluster::canonical_action::ApplyFailure;
            use pumpkin_cluster::mob_melee::{MobMeleeShape, in_range};
            let target = self.observation(target).ok_or(ApplyFailure::InvalidAction)?;
            let source_cell = self.cell(self.contact.target()).ok_or(ApplyFailure::Condition(self.contact.target()))?;
            let target_cell = self.cell(target.target()).ok_or(ApplyFailure::Condition(target.target()))?;
            let source_shape = self.source.actor_collision_shape(self.contact.target(), &source_cell.value)?;
            let target_shape = self.source.actor_collision_shape(target.target(), &target_cell.value)?;
            let mount = self.body.mount.as_ref().map(|identity| {
                let participant = self.contacts.participant(identity).ok_or(ApplyFailure::InvalidAction)?;
                let cell = self.cell(participant.target()).ok_or(ApplyFailure::Condition(participant.target()))?;
                let Value::Entity(body) = &cell.value else { return Err(ApplyFailure::Type(participant.target())); };
                if pumpkin_cluster::actor_relation::ActorRelationParticipant::from_entity(body, participant.chunk).as_ref() != Some(&participant)
                    || body.passengers.values().filter(|rider| *rider == &self.contact.identity).count() != 1 {
                    return Err(ApplyFailure::InvalidAction);
                }
                let (owner, cut) = self.source.actor_occupancy_cut(participant.target(), &cell.value)?;
                if owner != participant.owner || cut.sample_tick.is_after(self.contact.simulation_tick) { return Err(ApplyFailure::InvalidAction); }
                Ok((cut.position, self.source.actor_collision_shape(participant.target(), &cell.value)?))
            }).transpose()?;
            in_range(MobMeleeShape { position: self.contact.position, shape: &source_shape },
                mount.as_ref().map(|(position, shape)| MobMeleeShape { position: *position, shape }),
                MobMeleeShape { position: target.position, shape: &target_shape })
        })()).unwrap_or(false)
    }
    fn follow_range(&self) -> f64 {
        let attribute = pumpkin_data::attributes::Attributes::FOLLOW_RANGE;
        let base = self
            .living
            .combat
            .effective_attributes
            .get(&u16::from(attribute.id))
            .copied()
            .map(f64::from_bits)
            .unwrap_or(attribute.default_value);
        self.living
            .attribute(u16::from(attribute.id), base)
    }
    fn in_danger(&self) -> bool {
        use pumpkin_cluster::combat_action::DamageKind;
        if self.living.combat.timing.last_damage_tick.is_none_or(|last|
            last.is_after(self.contact.simulation_tick) || self.contact.simulation_tick.distance_since(last) > 40) {
            return false;
        }
        self.living
            .combat
            .timing
            .last_damage_kind
            .is_some_and(|kind| {
                let name = match kind {
                    DamageKind::PlayerAttack => "player_attack",
                    DamageKind::MobAttack => "mob_attack",
                    DamageKind::Magic => "magic",
                    DamageKind::OnFire => "on_fire",
                    DamageKind::InFire => "in_fire",
                    DamageKind::Lava => "lava",
                    DamageKind::Cactus => "cactus",
                    DamageKind::SweetBerry => "sweet_berry_bush",
                    DamageKind::Suffocation => "in_wall",
                    DamageKind::Freeze => "freeze",
                    DamageKind::Wither => "wither",
                    DamageKind::Starvation => "starve",
                    DamageKind::Explosion => "explosion",
                    DamageKind::Fall => "fall",
                    DamageKind::Drown => "drown",
                    DamageKind::OutOfWorld => "out_of_world",
                    DamageKind::Projectile => "arrow",
                    DamageKind::FallingBlock => "falling_block",
                    DamageKind::Anvil => "anvil",
                };
                pumpkin_data::tag::DamageType::MINECRAFT_PANIC_CAUSES
                    .0
                    .contains(&name)
            })
    }
    fn escape_destination(&self, random: &mut MobBrain) -> Option<[f64; 3]> {
        if self.living.combat.timing.fire_ticks > 0 {
            if let Some(water) = self.nearest_water() { return Some(water); }
        }
        self.random_destination(random, false, 5, 4)
    }
    fn wander_destination(&self, random: &mut MobBrain) -> Option<[f64; 3]> {
        self.random_destination(random, false, 10, 7)
    }
}

fn distance_squared(a: [f64; 3], b: [f64; 3]) -> f64 {
    a.into_iter().zip(b).map(|(a, b)| (a - b).powi(2)).sum()
}
