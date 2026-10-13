use serde::{Deserialize, Serialize};

use crate::inventory::InventoryCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MerchantScheme {
    Villager,
    WanderingTrader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerchantOfferState {
    pub cost_a: InventoryCell,
    pub cost_b: Option<InventoryCell>,
    pub output: InventoryCell,
    pub cost_a_limit: u8,
    pub uses: u32,
    pub max_uses: u32,
    pub experience: u32,
    pub reward_experience: bool,
    pub special_price: i32,
    pub price_multiplier: u32,
    pub demand: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerchantOffersState {
    pub version: u64,
    pub scheme: MerchantScheme,
    pub level: u8,
    pub experience: u32,
    pub offers: Vec<MerchantOfferState>,
}

impl MerchantOfferState {
    pub fn valid(&self) -> bool {
        let multiplier = f32::from_bits(self.price_multiplier);
        !self.cost_a.is_empty() && !self.output.is_empty()
            && self.cost_a_limit > 0 && self.cost_a_limit <= 99
            && self.cost_a.count <= self.cost_a_limit
            && self.cost_b.as_ref().is_none_or(|cost| !cost.is_empty() && cost.count <= 99
                && cost.nbt.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES)
            && self.cost_a.nbt.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES
            && self.output.nbt.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES
            && self.output.count <= 99 && self.uses <= i32::MAX as u32
            && self.max_uses > 0 && self.max_uses <= i32::MAX as u32
            && self.experience <= i32::MAX as u32 && multiplier.is_finite() && multiplier >= 0.0
    }

    pub fn adjusted_cost_a(&self) -> Option<InventoryCell> {
        if !self.valid() { return None; }
        let demand = i32::from(self.cost_a.count).saturating_mul(self.demand) as f32;
        let bonus = (demand * f32::from_bits(self.price_multiplier)).floor().max(0.0) as i32;
        let count = i32::from(self.cost_a.count).saturating_add(bonus)
            .saturating_add(self.special_price).clamp(1, i32::from(self.cost_a_limit));
        let mut cost = self.cost_a.clone();
        cost.count = count as u8;
        Some(cost)
    }
}

impl MerchantOffersState {
    pub fn valid(&self) -> bool {
        self.version != 0 && (1..=5).contains(&self.level)
            && self.experience <= i32::MAX as u32 && self.offers.len() <= 256
            && self.offers.iter().all(MerchantOfferState::valid)
    }

    pub fn execute(&self, offer: usize, repetitions: u16) -> Option<Self> {
        if !self.valid() || repetitions == 0 || repetitions > 64 { return None; }
        let mut next = self.clone();
        let selected = next.offers.get_mut(offer)?;
        selected.uses = selected.uses.checked_add(u32::from(repetitions))?;
        if selected.uses > selected.max_uses { return None; }
        next.experience = next.experience.checked_add(selected.experience.checked_mul(u32::from(repetitions))?)?;
        next.version = next.version.checked_add(1)?;
        next.valid().then_some(next)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerchantMenuWitness {
    pub session: crate::inventory::InventoryScreenSession,
    pub lease: crate::canonical_action::LivePlayerLease,
    pub viewer: crate::item_contact::ContactObservation,
    pub merchant: crate::item_contact::ContactObservation,
    pub merchant_kind: u16,
    pub selected_offer: u32,
}

impl MerchantMenuWitness {
    pub fn valid(&self) -> bool {
        self.session.session != 0 && self.session.actor == self.lease.gid
            && matches!(self.viewer.identity, crate::item_contact::ContactIdentity::Player(lease) if lease == self.lease)
            && matches!(self.merchant.identity, crate::item_contact::ContactIdentity::Entity { .. })
            && self.viewer.valid() && self.merchant.valid()
            && self.viewer.world == self.merchant.world
            && self.viewer.position.iter().zip(&self.merchant.position)
                .map(|(a, b)| (f64::from_bits(*a) - f64::from_bits(*b)).powi(2)).sum::<f64>() <= 64.0
    }

    pub fn offers_target(&self) -> crate::canonical_action::Target {
        crate::canonical_action::Target::Chunk {
            key: self.merchant.chunk,
            part: crate::canonical_action::ChunkPart::LivingProperty {
                actor: self.merchant.actor(),
                property: crate::living_property::LivingProperty::MerchantOffers,
            },
            inventory: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerchantTradeIntent {
    pub operation: MerchantOperation,
    pub menu: MerchantMenuWitness,
    pub menu_target: crate::canonical_action::Target,
    pub cost_a: crate::canonical_inventory::InventorySlotBinding,
    pub cost_b: crate::canonical_inventory::InventorySlotBinding,
    pub output_destinations: Vec<crate::canonical_inventory::InventorySlotBinding>,
    pub result: crate::canonical_inventory::InventorySlotBinding,
    pub output_stack_limit: u8,
    pub repetitions: u16,
    pub rewards: Vec<MerchantTradeReward>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MerchantOperation { SelectOffer, Execute }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerchantTradeReward {
    pub target: crate::canonical_action::Target,
    pub template: crate::entity_body::EntityBody,
}

pub struct MerchantTradeDerivation {
    pub inventories: std::collections::BTreeMap<crate::canonical_action::Target, InventoryCell>,
    pub offers: Option<MerchantOffersState>,
    pub menu: Option<crate::canonical_menu::MenuSessionState>,
    pub experience_rewards: std::collections::BTreeMap<crate::canonical_action::Target, crate::entity_body::EntityBody>,
}

impl MerchantTradeIntent {
    pub fn birth_bodies(&self) -> impl Iterator<Item = &crate::entity_body::EntityBody> { self.rewards.iter().map(|reward| &reward.template) }
    pub fn health_targets(&self) -> [crate::canonical_action::Target; 2] {
        [&self.menu.viewer, &self.menu.merchant].map(|participant| crate::canonical_action::Target::Chunk {
            key: participant.chunk, part: crate::canonical_action::ChunkPart::LivingProperty {
                actor: participant.actor(), property: crate::living_property::LivingProperty::Health }, inventory: None })
    }
    pub fn bindings(&self) -> Vec<crate::canonical_inventory::InventorySlotBinding> {
        [&self.cost_a, &self.cost_b, &self.result].into_iter()
            .chain(&self.output_destinations).cloned().collect()
    }

    pub fn valid(&self) -> bool {
        let bindings = std::iter::once(&self.cost_a).chain(std::iter::once(&self.cost_b))
            .chain(std::iter::once(&self.result)).chain(&self.output_destinations).collect::<Vec<_>>();
        self.menu.valid() && match self.operation { MerchantOperation::SelectOffer => self.repetitions == 0,
            MerchantOperation::Execute => self.repetitions > 0 && self.repetitions <= 64 }
            && matches!(self.menu_target, crate::canonical_action::Target::Chunk {
                part: crate::canonical_action::ChunkPart::LivingProperty {
                    actor: crate::actor::ActorId::Player(gid), property: crate::living_property::LivingProperty::MenuSession,
                }, inventory: None, .. } if gid == self.menu.lease.gid)
            && self.output_stack_limit > 0 && self.output_stack_limit <= 99
            && !self.output_destinations.is_empty() && self.output_destinations.len() <= 36
            && self.cost_a.slot.loc == crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, 0)
            && self.cost_b.slot.loc == crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, 1)
            && self.result.slot.loc == crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, 2)
            && self.output_destinations.iter().all(|binding| matches!(binding.target,
                crate::canonical_action::Target::Player { inventory: Some(crate::canonical_action::InventoryId::Player { inv, slot }), .. }
                    if inv == crate::inventory::INV_MAIN && slot < 36
                        || self.operation == MerchantOperation::Execute && inv == crate::inventory::INV_CURSOR && slot == 0))
            && (self.operation != MerchantOperation::SelectOffer || self.output_destinations.len() == 36)
            && self.rewards.len() == usize::from(self.repetitions)
            && self.rewards.iter().map(|reward| reward.target).collect::<std::collections::BTreeSet<_>>().len() == self.rewards.len()
            && self.rewards.iter().all(|reward| !bindings.iter().any(|binding| binding.target == reward.target)
                && reward.target != self.menu_target && reward.target != self.menu.offers_target()
                && reward.target != self.menu.viewer.target() && reward.target != self.menu.merchant.target())
            && bindings.iter().all(|binding| binding.is_valid())
            && bindings.iter().all(|binding| matches!(binding.target,
                crate::canonical_action::Target::Player { key, lease: Some(lease), inventory: Some(_) }
                if lease == self.menu.lease && key == lease.key))
            && [&self.cost_a, &self.cost_b, &self.result].into_iter().all(|binding|
                matches!(binding.target, crate::canonical_action::Target::Player {
                    inventory: Some(crate::canonical_action::InventoryId::Virtual { session, .. }), ..
                } if session == self.menu.session.session))
            && bindings.iter().map(|binding| binding.target).collect::<std::collections::BTreeSet<_>>().len() == bindings.len()
    }

    pub fn targets(&self) -> Vec<crate::canonical_action::Target> {
        std::iter::once(self.menu.offers_target())
            .chain([self.menu_target, self.menu.viewer.target(), self.menu.merchant.target()])
            .chain(self.health_targets())
            .chain([self.cost_a.target, self.cost_b.target, self.result.target])
            .chain(self.output_destinations.iter().map(|binding| binding.target))
            .chain(self.rewards.iter().map(|reward| reward.target))
            .collect::<std::collections::BTreeSet<_>>().into_iter().collect()
    }

    pub fn derive<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        reference: crate::canonical_action::ActionRef,
        values: &std::collections::BTreeMap<crate::canonical_action::Target, crate::canonical_action::Cell>,
    ) -> Result<MerchantTradeDerivation, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Value};
        if !self.valid() || reference.id.actor != crate::actor::ActorId::Player(self.menu.lease.gid)
            || reference.id.incarnation != self.menu.lease.incarnation
            || self.menu.viewer.simulation_tick != reference.tick || self.menu.merchant.simulation_tick != reference.tick {
            return Err(ApplyFailure::InvalidAction);
        }
        for target in self.health_targets() {
            if !matches!(values.get(&target), Some(crate::canonical_action::Cell { value: Value::LivingProperty(
                crate::living_property::LivingPropertyValue::Unsigned(health)), .. }) if *health > 0) {
                return Err(ApplyFailure::Condition(target));
            }
        }
        if !matches!(values.get(&self.menu_target), Some(crate::canonical_action::Cell {
            value: Value::LivingProperty(crate::living_property::LivingPropertyValue::MenuSession(state)), .. })
            if state.session == self.menu.session.session
                && (self.operation == MerchantOperation::SelectOffer
                    || state.selection == crate::canonical_menu::MenuSelection::TradeOffer(self.menu.selected_offer))
                && matches!(&state.source, crate::canonical_menu::MenuSource::Merchant { contact, kind }
                    if contact.identity == self.menu.merchant.identity && *kind == self.menu.merchant_kind))
            || !crate::canonical_menu::contact_matches(&self.menu.viewer, values.get(&self.menu.viewer.target()))
            || !crate::canonical_menu::contact_matches(&self.menu.merchant, values.get(&self.menu.merchant.target())) {
            return Err(ApplyFailure::Condition(self.menu_target));
        }
        let target = self.menu.offers_target();
        let Some(crate::canonical_action::Cell { value: Value::LivingProperty(
            crate::living_property::LivingPropertyValue::MerchantOffers(Some(offers))), .. }) = values.get(&target)
            else { return Err(ApplyFailure::Type(target)); };
        let offer = offers.offers.get(self.menu.selected_offer as usize).ok_or(ApplyFailure::InvalidAction)?;
        let cost_a = offer.adjusted_cost_a().ok_or(ApplyFailure::InvalidAction)?;
        if offer.output.count > self.output_stack_limit { return Err(ApplyFailure::InvalidAction); }
        let updated_offers = if self.operation == MerchantOperation::SelectOffer { None }
            else { Some(offers.execute(self.menu.selected_offer as usize, self.repetitions).ok_or(ApplyFailure::InvalidAction)?) };
        for reward in &self.rewards {
            let crate::canonical_action::Target::Chunk { key, part: crate::canonical_action::ChunkPart::Entity(id), inventory: None } = reward.target
                else { return Err(ApplyFailure::InvalidAction); };
            if !reward.template.belongs_to(id, key) || reward.template.birth.action != reference
                || key != self.menu.merchant.chunk || reward.template.spatial.position != self.menu.merchant.position
                || reward.template.birth.source.server != self.menu.lease.gid.server
                || !matches!(&reward.template.payload, crate::entity_body::EntityPayload::Experience(orb)
                    if orb.count == 1 && orb.health_milli == 5_000 && orb.age == 0 && orb.last_lifetime_tick.is_none())
                || values.contains_key(&reward.target) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut cells = std::collections::BTreeMap::new();
        for binding in std::iter::once(&self.cost_a).chain(std::iter::once(&self.cost_b))
            .chain(std::iter::once(&self.result)).chain(&self.output_destinations) {
            let Some(crate::canonical_action::Cell { value: Value::Inventory(cell), .. }) = values.get(&binding.target)
                else { return Err(ApplyFailure::Inventory(binding.target)); };
            if cell.loc != binding.slot.loc { return Err(ApplyFailure::Inventory(binding.target)); }
            cells.insert(binding.target, cell.clone());
        }
        if self.operation == MerchantOperation::SelectOffer {
            for input in [&self.cost_a, &self.cost_b] {
                let mut remaining = cells[&input.target].clone();
                for empty in [false, true] {
                    for destination in &self.output_destinations {
                        let current = &cells[&destination.target];
                        if current.is_empty() != empty || !current.is_empty()
                            && (current.item != remaining.item || current.nbt != remaining.nbt) { continue; }
                        if remaining.is_empty() { break; }
                        let limit = destination.max_stack.min(context.inventory_limit(&remaining, destination.target)?);
                        let amount = remaining.count.min(limit.saturating_sub(current.count));
                        if amount == 0 { continue; }
                        let mut after = remaining.clone();
                        after.loc = current.loc;
                        after.count = current.count + amount;
                        cells.insert(destination.target, after);
                        remaining.count -= amount;
                    }
                }
                if remaining.count == 0 { remaining = InventoryCell::empty(input.slot.loc); }
                cells.insert(input.target, remaining);
            }
            for (input, cost) in [(&self.cost_a, Some(&cost_a)), (&self.cost_b, offer.cost_b.as_ref())] {
                let Some(cost) = cost else { continue; };
                let current = &cells[&input.target];
                if !current.is_empty() && (current.item != cost.item || current.nbt != cost.nbt) { continue; }
                let limit = input.max_stack.min(context.inventory_limit(cost, input.target)?);
                let mut remaining = limit.saturating_sub(current.count);
                for source in &self.output_destinations {
                    let current = &cells[&source.target];
                    if current.item != cost.item || current.nbt != cost.nbt { continue; }
                    let amount = remaining.min(current.count);
                    if amount == 0 { continue; }
                    let mut after = current.clone();
                    after.count -= amount;
                    if after.count == 0 { after = InventoryCell::empty(after.loc); }
                    cells.insert(source.target, after);
                    let current = &cells[&input.target];
                    let mut after = cost.clone();
                    after.loc = input.slot.loc;
                    after.count = current.count + amount;
                    cells.insert(input.target, after);
                    remaining -= amount;
                    if remaining == 0 { break; }
                }
            }
            cells.insert(self.result.target, InventoryCell::empty(self.result.slot.loc));
            cells.retain(|target, cell| !matches!(values.get(target),
                Some(crate::canonical_action::Cell { value: Value::Inventory(prior), .. }) if prior == cell));
            let Some(crate::canonical_action::Cell { value: Value::LivingProperty(
                crate::living_property::LivingPropertyValue::MenuSession(menu)), .. }) = values.get(&self.menu_target)
                else { return Err(ApplyFailure::Type(self.menu_target)); };
            let mut menu = menu.clone();
            menu.selection = crate::canonical_menu::MenuSelection::TradeOffer(self.menu.selected_offer);
            return Ok(MerchantTradeDerivation { inventories: cells, offers: None, menu: Some(menu), experience_rewards: Default::default() });
        }
        for _ in 0..self.repetitions {
            for (binding, cost) in [(&self.cost_a, Some(&cost_a)), (&self.cost_b, offer.cost_b.as_ref())] {
                let cell = cells.get_mut(&binding.target).ok_or(ApplyFailure::Inventory(binding.target))?;
                if let Some(cost) = cost {
                    if cell.item != cost.item || cell.nbt != cost.nbt || cell.count < cost.count {
                        return Err(ApplyFailure::Inventory(binding.target));
                    }
                    cell.count -= cost.count;
                    if cell.count == 0 { *cell = InventoryCell::empty(cell.loc); }
                } else if !cell.is_empty() {
                    return Err(ApplyFailure::Inventory(binding.target));
                }
            }
            let mut remaining = offer.output.count;
            for empty in [false, true] {
                for binding in &self.output_destinations {
                    let cell = cells.get_mut(&binding.target).ok_or(ApplyFailure::Inventory(binding.target))?;
                    if cell.is_empty() != empty || !cell.is_empty()
                        && (cell.item != offer.output.item || cell.nbt != offer.output.nbt) { continue; }
                    let limit = binding.max_stack.min(self.output_stack_limit).min(context.inventory_limit(&offer.output, binding.target)?);
                    let amount = remaining.min(limit.saturating_sub(cell.count));
                    if amount == 0 { continue; }
                    let count = cell.count + amount;
                    let loc = cell.loc;
                    *cell = offer.output.clone();
                    cell.loc = loc;
                    cell.count = count;
                    remaining -= amount;
                    if remaining == 0 { break; }
                }
                if remaining == 0 { break; }
            }
            if remaining != 0 { return Err(ApplyFailure::Inventory(self.output_destinations[0].target)); }
        }
        cells.insert(self.result.target, InventoryCell::empty(self.result.slot.loc));
        cells.retain(|target, cell| !matches!(values.get(target),
            Some(crate::canonical_action::Cell { value: Value::Inventory(prior), .. }) if prior == cell));
        let mut experience_rewards = std::collections::BTreeMap::new();
        if offer.reward_experience {
            for ordinal in 0..self.repetitions {
                let seed = postcard::to_allocvec(&(reference, ordinal)).map_err(|_| ApplyFailure::InvalidAction)?;
                let reward = &self.rewards[usize::from(ordinal)];
                let mut body = reward.template.clone();
                let crate::entity_body::EntityPayload::Experience(orb) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
                orb.amount = 3 + (xxhash_rust::xxh64::xxh64(&seed, 0) % 4) as u32;
                experience_rewards.insert(reward.target, body);
            }
        }
        Ok(MerchantTradeDerivation { inventories: cells, offers: updated_offers, menu: None, experience_rewards })
    }
}

impl MerchantTradeIntent {
    pub fn capture_action<S: crate::canonical_action::ActionState>(
        &self,
        owners: &S,
        capture: crate::canonical_inventory::InventoryActionCapture,
    ) -> Result<crate::canonical_action::Action, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{Action, ActionClass, ActionKind, ActionRef, ApplyFailure, Condition,
            Effect, Expected, SemanticIntent, Stakeholder, TruthView, UndoProcedure};
        if !self.valid() || !capture.player_leases.contains(&self.menu.lease) { return Err(ApplyFailure::InvalidAction); }
        let targets = self.targets();
        let mut values = std::collections::BTreeMap::new();
        let mut conditions = capture.guards;
        for target in &targets {
            if self.rewards.iter().any(|reward| reward.target == *target) {
                if owners.read(TruthView::Local, *target).is_some() { return Err(ApplyFailure::Condition(*target)); }
                conditions.push(Condition { target: *target, expected: Expected::Missing });
                continue;
            }
            let value = owners.read(TruthView::Local, *target).ok_or(ApplyFailure::Condition(*target))?;
            if self.health_targets().contains(target) {
                conditions.push(Condition { target: *target, expected: Expected::LivingUnsignedAtLeast(1) });
            } else if *target != self.menu.viewer.target() && *target != self.menu.merchant.target() {
                conditions.push(Condition { target: *target, expected: Expected::Exact(value.clone()) });
            }
            values.insert(*target, value);
        }
        for participant in [&self.menu.viewer, &self.menu.merchant] {
            conditions.push(Condition { target: participant.target(), expected: Expected::SpatialEpochEquals(participant.epoch) });
        }
        let crate::item_contact::ContactIdentity::Entity { birth, .. } = &self.menu.merchant.identity
            else { return Err(ApplyFailure::InvalidAction); };
        conditions.push(Condition { target: self.menu.merchant.target(), expected: Expected::EntityBirthKind {
            birth: birth.clone(), owner: self.menu.merchant.owner, kind: self.menu.merchant_kind } });
        self.derive(owners, ActionRef { tick: capture.tick, id: capture.id }, &values)?;
        let stakeholders = targets.iter().map(|target| target.stakeholder())
            .chain(conditions.iter().map(|condition| condition.target.stakeholder()))
            .chain(capture.actor_chunk.map(Stakeholder::Chunk))
            .chain(capture.player_chunks.iter().map(|(_, chunk)| Stakeholder::Chunk(*chunk)))
            .collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        let action = Action {
            id: capture.id, tick: capture.tick, class: ActionClass::Replayable, chunk_transition: false,
            actor_chunk: capture.actor_chunk, player_leases: capture.player_leases, player_chunks: capture.player_chunks,
            stakeholders, dependencies: Vec::new(), conditions,
            effects: vec![Effect::TradeItems { intent: self.clone() }],
            counterfactual_conditions: targets.clone(), counterfactual_effects: targets,
            semantic_intent: Some(SemanticIntent::MerchantTrade(self.clone())), factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets], kind: ActionKind::Inventory, presentation: Vec::new(),
            client_origin: capture.client_origin, edit_authority: capture.edit_authority,
        };
        action.validate()?;
        Ok(action)
    }
}
