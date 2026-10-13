use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionContext, ActionRef, ActionState, AppliedWrite, ApplyFailure, Cell,
    LivePlayerLease, Target, TruthView, prepare_action,
};
use crate::inventory::InventoryCell;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeFailure {
    Capacity,
    Identity,
    IssueOrder,
    TransitionLast,
    DuplicateLifetime,
    Action(ApplyFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenEntityIssueAuthority {
    pub target: Target,
    pub birth: crate::entity_body::EntityBirth,
    pub producer: crate::realm_completion::PeerGeneration,
    pub controlled_owner: crate::realm_completion::PeerGeneration,
    pub effective_tick: TickStamp,
    pub next_sequence: u16,
}

pub struct ActorActionScope<'a, S> {
    owners: &'a S,
    actor: ActorId,
    incarnation: u64,
    tick: TickStamp,
    action_capacity: usize,
    target_capacity: usize,
    overlay: BTreeMap<Target, (Option<Cell>, ActionRef)>,
    actions: Vec<Action>,
    entity_authority: Option<FrozenEntityIssueAuthority>,
}

#[derive(Clone)]
pub struct ActorActionDraft {
    actor: ActorId,
    incarnation: u64,
    tick: TickStamp,
    action_capacity: usize,
    target_capacity: usize,
    overlay: BTreeMap<Target, (Option<Cell>, ActionRef)>,
    actions: Vec<Action>,
    entity_authority: Option<FrozenEntityIssueAuthority>,
}

pub struct ActorActionDraftView<'a, S> {
    owners: &'a S,
    draft: &'a ActorActionDraft,
}

impl ActorActionDraft {
    pub fn new(
        actor: ActorId,
        incarnation: u64,
        tick: TickStamp,
        action_capacity: usize,
        target_capacity: usize,
    ) -> Self {
        Self {
            actor,
            incarnation,
            tick,
            action_capacity,
            target_capacity,
            overlay: BTreeMap::new(),
            actions: Vec::new(),
            entity_authority: None,
        }
    }

    pub fn new_entity<S: ActionState>(
        owners: &S,
        authority: FrozenEntityIssueAuthority,
        tick: TickStamp,
        action_capacity: usize,
        target_capacity: usize,
    ) -> Result<Self, ScopeFailure> {
        let scope = ActorActionScope::new_entity(
            owners,
            authority,
            tick,
            action_capacity,
            target_capacity,
        )?;
        Ok(Self {
            actor: scope.actor,
            incarnation: scope.incarnation,
            tick: scope.tick,
            action_capacity: scope.action_capacity,
            target_capacity: scope.target_capacity,
            overlay: scope.overlay,
            actions: scope.actions,
            entity_authority: scope.entity_authority,
        })
    }

    pub fn reserve_source_resources(&mut self, actions: usize, targets: usize) -> Result<(), ScopeFailure> {
        let action_capacity = self.actions.len().checked_add(actions).ok_or(ScopeFailure::Capacity)?;
        let target_capacity = self.overlay.len().checked_add(targets).ok_or(ScopeFailure::Capacity)?;
        self.action_capacity = self.action_capacity.max(action_capacity);
        self.target_capacity = self.target_capacity.max(target_capacity);
        Ok(())
    }

    pub fn view<'a, S: ActionState>(&'a self, owners: &'a S) -> ActorActionDraftView<'a, S> {
        ActorActionDraftView {
            owners,
            draft: self,
        }
    }

    pub fn borrow_local<'a, S: ActionState>(
        &'a self,
        owners: &'a S,
        target: Target,
    ) -> Option<Cow<'a, Cell>> {
        if let Some((cell, _)) = self.overlay.get(&target) {
            return cell.as_ref().map(Cow::Borrowed);
        }
        if let Some(body) = crate::derived_living::actor_body_target(target)
            && let Some((cell, _)) = self.overlay.get(&body)
        {
            return cell
                .as_ref()
                .and_then(|cell| crate::derived_living::actor_facet_cell(cell, target))
                .map(Cow::Owned);
        }
        owners.borrow(TruthView::Local, target)
    }

    pub fn next_sequence(&self) -> Option<u16> {
        self.entity_authority.as_ref().map(|authority| {
            self.actions
                .last()
                .map_or(authority.next_sequence, |action| {
                    action.id.sequence.wrapping_add(1)
                })
        })
    }

    pub fn player_association(
        &self,
        player: crate::identity::GlobalPlayerId,
        original: crate::protocol::ChunkKey,
    ) -> Option<crate::protocol::ChunkKey> {
        let mut association = None;
        for (target, (cell, _)) in &self.overlay {
            let Target::Chunk {
                key,
                part: crate::canonical_action::ChunkPart::Player(gid),
                inventory: None,
            } = target
            else {
                continue;
            };
            if *gid != player {
                continue;
            }
            if matches!(
                cell.as_ref().map(|cell| &cell.value),
                Some(crate::canonical_action::Value::Living(_))
            ) {
                if association.replace(*key).is_some_and(|prior| prior != *key) {
                    return None;
                }
            }
        }
        association.or_else(|| {
            (!self.overlay.contains_key(&Target::Chunk {
                key: original,
                part: crate::canonical_action::ChunkPart::Player(player),
                inventory: None,
            }))
            .then_some(original)
        })
    }

    pub fn issue<S: ActionState>(
        &mut self,
        owners: &S,
        action: Action,
    ) -> Result<(), (ScopeFailure, Action)> {
        if let Err(error) = self.reserve_source_resources(1, action.targets().len()) {
            return Err((error, action));
        }
        let mut scope = ActorActionScope {
            owners,
            actor: self.actor,
            incarnation: self.incarnation,
            tick: self.tick,
            action_capacity: self.action_capacity,
            target_capacity: self.target_capacity,
            overlay: std::mem::take(&mut self.overlay),
            actions: std::mem::take(&mut self.actions),
            entity_authority: self.entity_authority.take(),
        };
        let result = scope.issue(action);
        self.overlay = scope.overlay;
        self.actions = scope.actions;
        self.entity_authority = scope.entity_authority;
        result
    }

    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    pub fn resource_usage(&self) -> Result<(usize,usize),ApplyFailure> {
        let authority=self.entity_authority.as_ref().map(|authority|(&authority.target,&authority.birth,&authority.producer,&authority.controlled_owner,&authority.effective_tick,&authority.next_sequence));
        let payload=(self.actor,self.incarnation,self.tick,self.action_capacity,self.target_capacity,&self.actions,&self.overlay,authority);
        let records=crate::logical_records::count(&payload)?;
        let bytes=postcard::experimental::serialized_size(&payload).map_err(|_|ApplyFailure::InvalidAction)?;
        crate::canonical_action::check_logical_budget(records,bytes)?;
        Ok((records,bytes))
    }

    pub fn frozen_overlay(&self) -> std::sync::Arc<BTreeMap<Target, (Option<Cell>, ActionRef)>> {
        std::sync::Arc::new(self.overlay.clone())
    }

    pub fn finish(self) -> Vec<Action> {
        self.actions
    }
}

impl<S: ActionState> ActionState for ActorActionDraftView<'_, S> {
    fn wake_rest_stat(&self) -> Result<(u16, u32), crate::canonical_action::ApplyFailure> { self.owners.wake_rest_stat() }

    fn validate_wake_beds(&self, world: crate::protocol::WorldId, sleeping: crate::item_action::PlayerSleepingState,
        beds: &[crate::sleep_action::BedRelease]) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.owners.validate_wake_beds(world, sleeping, beds)
    }

    fn validate_morning(&self, world: crate::protocol::WorldId, morning: crate::world_time::MorningTransition, source: crate::canonical_action::ActionRef) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.owners.validate_morning(world, morning, source)
    }

    fn world_tool_companion(&self, location: crate::world_command::BlockLocation, current: u16) -> Result<Option<crate::world_command::ToolCompanion>, ApplyFailure> {
        self.owners.world_tool_companion(location, current)
    }
    fn world_tool_attributes(&self) -> Result<crate::world_command::ToolAttributes, ApplyFailure> {
        self.owners.world_tool_attributes()
    }
    fn world_tool_transform(&self, operation: crate::world_command::ToolOperation, current: u16, item: u16) -> Result<Option<u16>, ApplyFailure> {
        self.owners.world_tool_transform(operation, current, item)
    }
    fn world_tool_eye_height(&self, living: &crate::item_action::ActorLivingState) -> Result<f64, ApplyFailure> {
        self.owners.world_tool_eye_height(living)
    }
    fn world_tool_light_plans(&self, location: crate::world_command::BlockLocation, operation: crate::world_command::ToolOperation, current: u16) -> Result<Vec<crate::world_action::WorldLightPlan>, ApplyFailure> {
        self.owners.world_tool_light_plans(location, operation, current)
    }
    fn actor_occupancy_cut(&self, target: Target, value: &crate::canonical_action::Value) -> Result<(crate::realm_completion::PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        self.owners.actor_occupancy_cut(target, value)
    }
    fn actor_collision_shape(&self, target: Target, value: &crate::canonical_action::Value) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> { self.owners.actor_collision_shape(target, value) }
    fn validate_world_border(&self, witness: crate::world_time::WorldBorderWitness, source: crate::canonical_action::ActionRef) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.owners.validate_world_border(witness, source)
    }

    fn combat_parent_damaged(&self, parent: ActionRef, victim: &crate::item_contact::ContactIdentity) -> Option<bool> {
        self.draft.actions.iter().find(|action| action.id == parent.id && action.tick == parent.tick)
            .and_then(|action| action.factual_result.as_ref().map(|result| crate::combat_action::damaging_parent(action, result, victim)))
            .or_else(|| self.owners.combat_parent_damaged(parent, victim))
    }
    fn combat_parent_charge(&self, parent: ActionRef) -> Option<u64> {
        self.draft.actions.iter().find(|action| action.id == parent.id && action.tick == parent.tick)
            .and_then(crate::combat_action::resolved_attack_charge)
            .or_else(|| self.owners.combat_parent_charge(parent))
    }
    fn resolve_world_respawn(
        &self,
        plan: crate::world_respawn::WorldRespawnPlan,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Option<crate::world_respawn::WorldRespawnResolution>, ApplyFailure> {
        self.owners.resolve_world_respawn(plan, values)
    }

    fn scheduled_executor(&self, chunk: crate::protocol::ChunkKey) -> Result<crate::realm_completion::PeerGeneration, ApplyFailure> {
        self.owners.scheduled_executor(chunk)
    }
    fn block_segment_obstructs(
        &self,
        state: u16,
        position: crate::protocol::BlockPos,
        start: [u64; 3],
        end: [u64; 3],
    ) -> Result<bool, ApplyFailure> {
        self.owners
            .block_segment_obstructs(state, position, start, end)
    }
    fn fall_damage_factor(&self, state: u16, sneaking: bool) -> Result<u16, ApplyFailure> {
        self.owners.fall_damage_factor(state, sneaking)
    }
    fn environmental_contact(&self, intent: &crate::combat_action::EnvironmentalDamageIntent, values: &std::collections::BTreeMap<Target,Cell>)
        -> Result<crate::combat_action::EnvironmentalContactFacts,ApplyFailure> { self.owners.environmental_contact(intent,values) }
    fn environmental_hazard(
        &self,
        kind: crate::combat_action::EnvironmentalHazardKind,
        block_state: u16,
    ) -> Result<Option<crate::combat_action::EnvironmentalHazardFacts>, ApplyFailure> {
        self.owners.environmental_hazard(kind, block_state)
    }
    fn status_effect_facts(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<crate::combat_action::CombatStatusFacts, ApplyFailure> {
        self.owners.status_effect_facts(effects)
    }
    fn status_effect_modifiers(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<Vec<crate::combat_action::CombatAttributeModifier>, ApplyFailure> {
        self.owners.status_effect_modifiers(effects)
    }
    fn mob_breeding_advancements(&self, kind: u16, credit: &crate::mob_breeding::MobBreedingCredit,
        state: &crate::player_advancement::PlayerAdvancementState)
        -> Result<crate::player_advancement::PlayerAdvancementState, ApplyFailure> {
        self.owners.mob_breeding_advancements(kind, credit, state)
    }
    fn derive_item_use(
        &self,
        intent: &crate::item_use::ItemUseIntent,
        id: crate::canonical_action::ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.owners.derive_item_use(intent, id, tick, values)
    }
    fn crossbow_items(&self, intent: &crate::crossbow_items::CrossbowItems, id: crate::canonical_action::ActionId, tick: TickStamp,
        values: &BTreeMap<Target, Cell>) -> Result<BTreeMap<Target, InventoryCell>, ApplyFailure> {
        self.owners.crossbow_items(intent, id, tick, values)
    }
    fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
        self.owners.vehicle_activator(state)
    }
    fn vehicle_drop_item(&self, body: &crate::entity_body::EntityBody)
        -> Result<InventoryCell, ApplyFailure> {
        self.owners.vehicle_drop_item(body)
    }
    fn issued_item_birth(&self, identity: &crate::entity_origin::EntityOriginIdentity,
        cause: crate::canonical_action::ActionRef, owner: crate::realm_completion::PeerGeneration,
        spatial: crate::entity_body::EntitySpatial, cell: &crate::inventory::InventoryCell, pickup_delay: u16)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        self.owners.issued_item_birth(identity, cause, owner, spatial, cell, pickup_delay)
    }
    fn dropped_item_body(&self, template: &crate::entity_body::EntityBody, source: &InventoryCell)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        self.owners.dropped_item_body(template, source)
    }
    fn recipe_known(&self, id: &str) -> bool { self.owners.recipe_known(id) }
    fn action_source_slot(&self, reference: crate::canonical_action::ActionRef) -> Option<u64> {
        self.owners.action_source_slot(reference)
    }
    fn player_advancement_outcome(&self, event: &crate::player_advancement::AdvancementEvent,
        state: &crate::player_advancement::PlayerAdvancementState, achieved_millis: u64)
        -> Result<crate::player_advancement::AdvancementOutcome, crate::canonical_action::ApplyFailure> {
        self.owners.player_advancement_outcome(event, state, achieved_millis)
    }

    fn entity_placement_matches(&self, intent: &crate::entity_placement::EntityPlacementIntent) -> bool { self.owners.entity_placement_matches(intent) }
    fn falling_damage_kind(&self, state: u16) -> Option<crate::combat_action::DamageKind> { self.owners.falling_damage_kind(state) }
    fn primed_tnt_block(&self, state: u16) -> bool {
        self.owners.primed_tnt_block(state)
    }
    fn primed_tnt_identity(&self) -> Option<(u16, u16)> {
        self.owners.primed_tnt_identity()
    }
    fn issuer_entity_admission_authority(&self, reference: crate::canonical_action::ActionRef,
        witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.owners.issuer_entity_admission_authority(reference, witness)
    }
    fn issuer_entity_initialization_matches(&self, witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.owners.issuer_entity_initialization_matches(witness)
    }
    fn world_rules(&self, witness: crate::world_rules::WorldRulesWitness, tick: crate::time::TickStamp)
        -> Result<crate::world_rules::WorldRulesSnapshot, ApplyFailure> {
        self.owners.world_rules(witness, tick)
    }
    fn death_inventory_rule(&self, action: crate::canonical_action::ActionRef, rules: crate::world_rules::WorldRulesWitness, keep_inventory: bool, target: crate::canonical_action::Target)
        -> Result<(), ApplyFailure> {
        self.owners.death_inventory_rule(action, rules, keep_inventory, target)
    }
    fn death_inventory_drop(&self, template: &crate::entity_body::EntityBody, source: &crate::inventory::InventoryCell)
        -> Result<Option<crate::entity_body::EntityBody>, ApplyFailure> {
        self.owners.death_inventory_drop(template, source)
    }
    fn derive_cloud_potion(&self, intent: &crate::area_cloud::AreaCloudIntent, victim: &crate::item_contact::ContactObservation,
        id: crate::canonical_action::ActionId, tick: crate::time::TickStamp, values: &BTreeMap<crate::canonical_action::Target, crate::canonical_action::Cell>) -> Result<crate::area_cloud::CloudPotionOutcome, crate::canonical_action::ApplyFailure> { self.owners.derive_cloud_potion(intent, victim, id, tick, values) }
    fn derive_projectile_tick(
&self, intent: &crate::projectile_tick::ProjectileTickIntent, id: crate::canonical_action::ActionId, tick: crate::time::TickStamp,
        values: &BTreeMap<Target, crate::canonical_action::Cell>) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.owners.derive_projectile_tick(intent, id, tick, values)
    }
    fn mending_repair(&self, source: &crate::inventory::InventoryCell, experience: u32)
        -> Result<(crate::canonical_inventory::ItemTransformOutput, u32), ApplyFailure> {
        self.owners.mending_repair(source, experience)
    }
    fn item_cooldown_group(&self, source: &crate::inventory::InventoryCell) -> Result<String, ApplyFailure> {
        self.owners.item_cooldown_group(source)
    }
    fn derive_block_inventory_tick(&self, intent: &crate::canonical_block_inventory_tick::BlockInventoryTickIntent,
        values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>) -> Result<crate::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
        self.owners.derive_block_inventory_tick(intent, values)
    }
    fn machine_genesis(&self, chunk: crate::protocol::ChunkKey) -> Result<[u8; 32], ApplyFailure> { self.owners.machine_genesis(chunk) }
    fn machine_tick_footprint(&self, identity: crate::action_identity::MachineActionId, values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>) -> Result<crate::action_command::ActionFootprint, ApplyFailure> { self.owners.machine_tick_footprint(identity, values) }
    fn derive_machine_tick(&self, identity: crate::action_identity::MachineActionId, values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>) -> Result<Vec<crate::canonical_action::AppliedWrite>, ApplyFailure> { self.owners.derive_machine_tick(identity, values) }
    fn menu_inventory_transform(&self, kind: crate::canonical_menu_transform::MenuInventoryTransformKind,
        selection: &crate::canonical_menu::MenuSelection, inputs: &[crate::inventory::InventoryCell])
        -> Result<crate::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure> {
        self.owners.menu_inventory_transform(kind, selection, inputs)
    }
    fn item_transform(
        &self,
        source: &InventoryCell,
        operation: &crate::canonical_inventory::ItemTransformOperation,
        id: crate::canonical_action::ActionId,
        tick: TickStamp,
    ) -> Result<crate::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        self.owners.item_transform(source, operation, id, tick)
    }
    fn derive_world_light(
        &self,
        plan: crate::world_action::WorldLightPlan,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.owners.derive_world_light(plan, values)
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        self.owners.inventory_equipment_attribute_ids()
    }
    fn inventory_equipment_facts(
        &self,
        cells: &[(
            crate::canonical_inventory::InventoryEquipmentSlot,
            InventoryCell,
        )],
    ) -> Result<crate::combat_action::CombatEquipmentFacts, ApplyFailure> {
        self.owners.inventory_equipment_facts(cells)
    }
    fn borrow(&self, view: TruthView, target: Target) -> Option<Cow<'_, Cell>> {
        if view == TruthView::Local {
            return self.draft.borrow_local(self.owners, target);
        }
        self.owners.borrow(view, target)
    }

    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        self.borrow(view, target).map(Cow::into_owned)
    }

    fn validate_live_player_lease(&self, lease: LivePlayerLease) -> bool {
        self.owners.validate_live_player_lease(lease)
    }

    fn available(&self, view: TruthView, target: Target) -> bool {
        if view == TruthView::Local && self.borrow(view, target).is_some() {
            return true;
        }
        self.owners.available(view, target)
    }

    fn entity_equipment_compatible(
        &self,
        kind: u16,
        slots: &[(crate::canonical_inventory::InventoryEquipmentSlot, crate::inventory::InventoryCell)],
    ) -> Result<(), ApplyFailure> {
        self.owners.entity_equipment_compatible(kind, slots)
    }

    fn validate_block_inventory_layout(&self, block: Target, state: u16, block_entity: &Cell,
        bindings: &[crate::canonical_inventory::InventorySlotBinding]) -> Result<(), ApplyFailure> {
        self.owners.validate_block_inventory_layout(block, state, block_entity, bindings)
    }
    fn entity_inventory_storage_capacity(
        &self,
        kind: u16,
        facets: &[(crate::mob_species_property::MobSpeciesProperty, crate::mob_species_property::MobSpeciesPropertyValue)],
        storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        self.owners.entity_inventory_storage_capacity(kind, facets, storage_slots)
    }

    fn entity_inventory_layout(
        &self,
        body: &crate::entity_body::EntityBody,
    ) -> Result<Vec<crate::inventory::InvLoc>, ApplyFailure> {
        self.owners.entity_inventory_layout(body)
    }
    fn entity_inventory_limit(
        &self,
        body: &crate::entity_body::EntityBody,
        item: &InventoryCell,
        target: Target,
    ) -> Result<u8, ApplyFailure> {
        self.owners.entity_inventory_limit(body, item, target)
    }

    fn inventory_limit(
        &self,
        source: &InventoryCell,
        destination: Target,
    ) -> Result<u8, ApplyFailure> {
        self.owners.inventory_limit(source, destination)
    }

    fn commit(
        &mut self,
        _: ActionContext,
        _: TruthView,
        _: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}

impl<'a, S: ActionState> ActorActionScope<'a, S> {
    pub fn new(
        owners: &'a S,
        actor: ActorId,
        incarnation: u64,
        tick: TickStamp,
        action_capacity: usize,
        target_capacity: usize,
    ) -> Self {
        Self {
            owners,
            actor,
            incarnation,
            tick,
            action_capacity,
            target_capacity,
            overlay: BTreeMap::new(),
            actions: Vec::new(),
            entity_authority: None,
        }
    }

    pub fn new_entity(
        owners: &'a S,
        authority: FrozenEntityIssueAuthority,
        tick: TickStamp,
        action_capacity: usize,
        target_capacity: usize,
    ) -> Result<Self, ScopeFailure> {
        let Target::Chunk {
            key,
            part: crate::canonical_action::ChunkPart::Entity(entity),
            inventory: None,
        } = authority.target
        else {
            return Err(ScopeFailure::Identity);
        };
        if tick.distance_since(authority.effective_tick) >= 32_768
            || authority
                .effective_tick
                .distance_since(authority.birth.action.tick)
                >= 32_768
        {
            return Err(ScopeFailure::Identity);
        }
        let Some(cell) = owners.borrow(TruthView::Local, authority.target) else {
            return Err(ScopeFailure::Identity);
        };
        let crate::canonical_action::Value::Entity(body) = &cell.value else {
            return Err(ScopeFailure::Identity);
        };
        if !body.belongs_to(entity, key)
            || body.birth != authority.birth
            || body.owner != authority.controlled_owner
            || authority.producer.generation == 0
            || authority.producer != authority.controlled_owner
                && !matches!(body.payload, crate::entity_body::EntityPayload::Item(_))
        {
            return Err(ScopeFailure::Identity);
        }
        let mut scope = Self::new(
            owners,
            ActorId::Entity(entity),
            u64::from(entity.generation),
            tick,
            action_capacity,
            target_capacity,
        );
        scope.entity_authority = Some(authority);
        Ok(scope)
    }

    pub fn next_sequence(&self) -> Option<u16> {
        self.entity_authority.as_ref().map(|authority| {
            self.actions
                .last()
                .map_or(authority.next_sequence, |action| {
                    action.id.sequence.wrapping_add(1)
                })
        })
    }

    pub fn read(&self, target: Target) -> Option<Cow<'_, Cell>> {
        match self.overlay.get(&target) {
            Some((cell, _)) => cell.as_ref().map(Cow::Borrowed),
            None => {
                if let Some(body) = crate::derived_living::actor_body_target(target)
                    && let Some((cell, _)) = self.overlay.get(&body)
                {
                    return cell
                        .as_ref()
                        .and_then(|cell| crate::derived_living::actor_facet_cell(cell, target))
                        .map(Cow::Owned);
                }
                self.owners.borrow(TruthView::Local, target)
            }
        }
    }

    pub fn issue(&mut self, mut action: Action) -> Result<(), (ScopeFailure, Action)> {
        if self
            .actions
            .last()
            .is_some_and(|previous| previous.chunk_transition)
        {
            return Err((ScopeFailure::TransitionLast, action));
        }
        if action.id.actor != self.actor
            || action.id.incarnation != self.incarnation
            || action.tick != self.tick
            || self.actions.iter().any(|previous| previous.id == action.id)
        {
            return Err((ScopeFailure::Identity, action));
        }
        if self.actions.len() >= self.action_capacity {
            return Err((ScopeFailure::Capacity, action));
        }
        if let (Some(first), Some(previous)) = (self.actions.first(), self.actions.last()) {
            let step = action.id.sequence.wrapping_sub(previous.id.sequence);
            let span = action.id.sequence.wrapping_sub(first.id.sequence);
            if step == 0 || step > 4096 || span > 4096 {
                return Err((ScopeFailure::IssueOrder, action));
            }
        }
        if let ActorId::Entity(entity) = self.actor {
            let Some(authority) = self.entity_authority.as_ref() else {
                return Err((ScopeFailure::Identity, action));
            };
            if action.id.sequence != self.next_sequence().unwrap()
                || !matches!(authority.target, Target::Chunk { key, .. } if action.actor_chunk == Some(key))
                || action.conditions.iter().any(|condition| condition.target == authority.target && matches!(&condition.expected,
                    crate::canonical_action::Expected::ActorControlled { birth, issuer }
                        if *birth != authority.birth || *issuer != authority.controlled_owner))
                || action.effects.iter().any(|effect| matches!(effect,
                    crate::canonical_action::Effect::ChangeDroppedItem { target, birth, issuer, body_owner, .. }
                        if *target == authority.target && (*birth != authority.birth || *issuer != authority.producer || *body_owner != authority.controlled_owner)))
                || action.effects.iter().any(|effect| matches!(effect,
                    crate::canonical_action::Effect::UpdateMobContinuation { target, authority: witness, .. }
                        if crate::living_property::body_target(*target).is_none_or(|(body, _)| body != authority.target)
                            || witness.birth != authority.birth || witness.issuer != authority.producer || witness.body_owner != authority.controlled_owner))
                || action.effects.iter().any(|effect| matches!(effect,
                    crate::canonical_action::Effect::UpdateVehicleProperty { target, authority: witness, .. }
                        if crate::vehicle_property::body_target(*target).is_none_or(|(body, _)| body != authority.target)
                            || witness.birth != authority.birth || witness.issuer != authority.producer || witness.body_owner != authority.controlled_owner))
            { return Err((ScopeFailure::Identity, action)); }
            if authority.producer != authority.controlled_owner
                && action.effects.iter().any(|effect| match effect {
                    crate::canonical_action::Effect::ChangeDroppedItem { target, .. } => {
                        *target != authority.target
                    }
                    crate::canonical_action::Effect::MoveEntityBody {
                        source,
                        authority: Some(witness),
                        ..
                    } => {
                        *source != authority.target
                            || witness.birth != authority.birth
                            || witness.body_owner != authority.controlled_owner
                            || witness.issuer != authority.producer
                    }
                    crate::canonical_action::Effect::MergeDroppedItems { source, .. } => {
                        *source != authority.target
                    }
                    _ => true,
                })
            {
                return Err((ScopeFailure::Identity, action));
            }
            let is_lifetime = |candidate: &Action| {
                candidate.effects.iter().any(|effect| matches!(effect,
                crate::canonical_action::Effect::ChangeDroppedItem { target, change: crate::entity_body::DroppedItemChange::AdvanceLifetime, .. }
                    if *target == authority.target))
            };
            if is_lifetime(&action) && self.actions.iter().any(is_lifetime) {
                return Err((ScopeFailure::DuplicateLifetime, action));
            }
            if action.class != crate::canonical_action::ActionClass::Infallible {
                let Some(key) = action.actor_chunk else {
                    return Err((ScopeFailure::Identity, action));
                };
                let target = Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::Entity(entity),
                    inventory: None,
                };
                let Some(cell) = self.read(target) else {
                    return Err((ScopeFailure::Identity, action));
                };
                let crate::canonical_action::Value::Entity(body) = &cell.value else {
                    return Err((ScopeFailure::Identity, action));
                };
                if !body.belongs_to(entity, key)
                    || body.birth != authority.birth
                    || body.owner != authority.controlled_owner
                {
                    return Err((ScopeFailure::Identity, action));
                }
                let condition = crate::canonical_action::Condition {
                    target,
                    expected: crate::canonical_action::Expected::ActorControlled {
                        birth: body.birth.clone(),
                        issuer: body.owner,
                    },
                };
                if !action.conditions.contains(&condition) {
                    action.conditions.push(condition);
                }
            }
        }
        for target in action
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(action.effects.iter().flat_map(|effect| effect.targets()))
        {
            if let Some((_, predecessor)) = self
                .overlay
                .get(&target)
                .or_else(|| {
                    crate::derived_living::actor_body_target(target)
                        .and_then(|body| self.overlay.get(&body))
                })
                .or_else(|| {
                    let Target::Chunk {
                        key,
                        part: crate::canonical_action::ChunkPart::Block(position),
                        inventory: None,
                    } = target
                    else {
                        return None;
                    };
                    self.overlay.get(&Target::Chunk {
                        key,
                        part: crate::canonical_action::ChunkPart::BlockSection(
                            position.y.div_euclid(16),
                        ),
                        inventory: None,
                    })
                })
            {
                action.dependencies.push(*predecessor);
            }
        }
        action.dependencies.sort_unstable();
        action.dependencies.dedup();
        let result = match prepare_action(self, &action, TruthView::Local) {
            Ok(result) => result,
            Err(reason) => return Err((ScopeFailure::Action(reason), action)),
        };
        let mut projected = result
            .writes
            .iter()
            .map(|write| (write.target, write.after.clone()))
            .collect::<BTreeMap<_, _>>();
        for write in result.writes.iter().filter(|write| matches!(write.target, Target::Chunk { part: crate::canonical_action::ChunkPart::ActorOccupancy, inventory: None, .. })) {
            let Some(prior) = self.read(write.target) else { return Err((ScopeFailure::Action(ApplyFailure::Condition(write.target)), action)); };
            let Some(after) = crate::actor_occupancy::materialize_write(write, &prior) else { return Err((ScopeFailure::Action(ApplyFailure::Condition(write.target)), action)); };
            projected.insert(write.target, Some(after));
        }
        let sections = result
            .writes
            .iter()
            .filter_map(|write| {
                let Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::Block(position),
                    inventory: None,
                } = write.target
                else {
                    return None;
                };
                Some(Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::BlockSection(
                        position.y.div_euclid(16),
                    ),
                    inventory: None,
                })
            })
            .collect::<std::collections::BTreeSet<_>>();
        for section in sections {
            if let Some(before) = self.read(section) {
                let after =
                    match crate::block_section::project_writes(section, &before, &result.writes) {
                        Ok(after) => after,
                        Err(reason) => return Err((ScopeFailure::Action(reason), action)),
                    };
                projected.insert(section, Some(after));
            }
        }
        let bodies = result
            .writes
            .iter()
            .filter_map(|write| {
                crate::derived_living::actor_body_target(write.target)
                    .or_else(|| crate::frame_inventory_projection::body_target(write.target)).or_else(|| {
                    matches!(
                        write.target,
                        Target::Chunk {
                            part: crate::canonical_action::ChunkPart::Player(_)
                                | crate::canonical_action::ChunkPart::Entity(_),
                            inventory: None,
                            ..
                        }
                    )
                    .then_some(write.target)
                })
            })
            .collect::<std::collections::BTreeSet<_>>();
        for body in bodies {
            let before = self.read(body).map(Cow::into_owned);
            let after = match crate::derived_living::project_body_writes(
                body,
                before.as_ref(),
                &result.writes,
            ) {
                Ok(after) => after,
                Err(reason) => return Err((ScopeFailure::Action(reason), action)),
            };
            let properties = before
                .iter()
                .chain(after.iter())
                .filter_map(|cell| crate::living_property::state(&cell.value))
                .flat_map(crate::living_property::LivingProperty::all)
                .collect::<std::collections::BTreeSet<_>>();
            let Target::Chunk { key, part, .. } = body else {
                unreachable!()
            };
            let actor = match part {
                crate::canonical_action::ChunkPart::Player(id) => ActorId::Player(id),
                crate::canonical_action::ChunkPart::Entity(id) => ActorId::Entity(id),
                _ => unreachable!(),
            };
            for property in properties {
                let target = Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::LivingProperty { actor, property },
                    inventory: None,
                };
                let value = after
                    .as_ref()
                    .and_then(|cell| crate::derived_living::facet_cell(cell, property));
                if self.overlay.contains_key(&target)
                    && self.read(target).as_deref() != value.as_ref()
                {
                    projected.insert(target, value);
                }
            }
            let relations = match actor {
                ActorId::Player(_) => vec![crate::actor_relation::ActorRelation::Mount],
                ActorId::Entity(_) => std::iter::once(crate::actor_relation::ActorRelation::Mount)
                    .chain((0..32).map(crate::actor_relation::ActorRelation::Passenger))
                    .collect(),
                ActorId::Peer(_) => Vec::new(),
            };
            for relation in relations {
                let target = Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::ActorRelation { actor, relation },
                    inventory: None,
                };
                let value = after
                    .as_ref()
                    .and_then(|cell| crate::derived_living::actor_facet_cell(cell, target));
                let old_value = before
                    .as_ref()
                    .and_then(|cell| crate::derived_living::actor_facet_cell(cell, target));
                if self.overlay.contains_key(&target) && old_value != value {
                    projected.insert(target, value);
                }
            }
            if before != after {
                projected.insert(body, after);
            }
        }
        let added = projected
            .keys()
            .filter(|target| !self.overlay.contains_key(target))
            .count();
        if added > self.target_capacity.saturating_sub(self.overlay.len()) {
            return Err((ScopeFailure::Capacity, action));
        }
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        for (target, value) in projected {
            self.overlay.insert(target, (value, reference));
        }
        action.factual_result = Some(result);
        self.actions.push(action);
        Ok(())
    }

    pub fn finish(self) -> Vec<Action> {
        self.actions
    }
}

impl<S: ActionState> ActionState for ActorActionScope<'_, S> {
    fn wake_rest_stat(&self) -> Result<(u16, u32), crate::canonical_action::ApplyFailure> { self.owners.wake_rest_stat() }

    fn validate_wake_beds(&self, world: crate::protocol::WorldId, sleeping: crate::item_action::PlayerSleepingState,
        beds: &[crate::sleep_action::BedRelease]) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.owners.validate_wake_beds(world, sleeping, beds)
    }

    fn validate_morning(&self, world: crate::protocol::WorldId, morning: crate::world_time::MorningTransition, source: crate::canonical_action::ActionRef) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.owners.validate_morning(world, morning, source)
    }

    fn world_tool_companion(&self, location: crate::world_command::BlockLocation, current: u16) -> Result<Option<crate::world_command::ToolCompanion>, ApplyFailure> {
        self.owners.world_tool_companion(location, current)
    }
    fn world_tool_attributes(&self) -> Result<crate::world_command::ToolAttributes, ApplyFailure> {
        self.owners.world_tool_attributes()
    }
    fn world_tool_transform(&self, operation: crate::world_command::ToolOperation, current: u16, item: u16) -> Result<Option<u16>, ApplyFailure> {
        self.owners.world_tool_transform(operation, current, item)
    }
    fn world_tool_eye_height(&self, living: &crate::item_action::ActorLivingState) -> Result<f64, ApplyFailure> {
        self.owners.world_tool_eye_height(living)
    }
    fn world_tool_light_plans(&self, location: crate::world_command::BlockLocation, operation: crate::world_command::ToolOperation, current: u16) -> Result<Vec<crate::world_action::WorldLightPlan>, ApplyFailure> {
        self.owners.world_tool_light_plans(location, operation, current)
    }
    fn actor_occupancy_cut(&self, target: Target, value: &crate::canonical_action::Value) -> Result<(crate::realm_completion::PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        self.owners.actor_occupancy_cut(target, value)
    }
    fn actor_collision_shape(&self, target: Target, value: &crate::canonical_action::Value) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> { self.owners.actor_collision_shape(target, value) }
    fn validate_world_border(&self, witness: crate::world_time::WorldBorderWitness, source: crate::canonical_action::ActionRef) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.owners.validate_world_border(witness, source)
    }

    fn combat_parent_damaged(&self, parent: ActionRef, victim: &crate::item_contact::ContactIdentity) -> Option<bool> {
        self.actions.iter().find(|action| action.id == parent.id && action.tick == parent.tick)
            .and_then(|action| action.factual_result.as_ref().map(|result| crate::combat_action::damaging_parent(action, result, victim)))
            .or_else(|| self.owners.combat_parent_damaged(parent, victim))
    }
    fn combat_parent_charge(&self, parent: ActionRef) -> Option<u64> {
        self.actions.iter().find(|action| action.id == parent.id && action.tick == parent.tick)
            .and_then(crate::combat_action::resolved_attack_charge)
            .or_else(|| self.owners.combat_parent_charge(parent))
    }
    fn resolve_world_respawn(
        &self,
        plan: crate::world_respawn::WorldRespawnPlan,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Option<crate::world_respawn::WorldRespawnResolution>, ApplyFailure> {
        self.owners.resolve_world_respawn(plan, values)
    }

    fn scheduled_executor(&self, chunk: crate::protocol::ChunkKey) -> Result<crate::realm_completion::PeerGeneration, ApplyFailure> {
        self.owners.scheduled_executor(chunk)
    }
    fn block_segment_obstructs(
        &self,
        state: u16,
        position: crate::protocol::BlockPos,
        start: [u64; 3],
        end: [u64; 3],
    ) -> Result<bool, ApplyFailure> {
        self.owners
            .block_segment_obstructs(state, position, start, end)
    }
    fn fall_damage_factor(&self, state: u16, sneaking: bool) -> Result<u16, ApplyFailure> {
        self.owners.fall_damage_factor(state, sneaking)
    }
    fn environmental_contact(&self, intent: &crate::combat_action::EnvironmentalDamageIntent, values: &std::collections::BTreeMap<Target,Cell>)
        -> Result<crate::combat_action::EnvironmentalContactFacts,ApplyFailure> { self.owners.environmental_contact(intent,values) }
    fn environmental_hazard(
        &self,
        kind: crate::combat_action::EnvironmentalHazardKind,
        block_state: u16,
    ) -> Result<Option<crate::combat_action::EnvironmentalHazardFacts>, ApplyFailure> {
        self.owners.environmental_hazard(kind, block_state)
    }
    fn status_effect_facts(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<crate::combat_action::CombatStatusFacts, ApplyFailure> {
        self.owners.status_effect_facts(effects)
    }
    fn status_effect_modifiers(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<Vec<crate::combat_action::CombatAttributeModifier>, ApplyFailure> {
        self.owners.status_effect_modifiers(effects)
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        self.owners.inventory_equipment_attribute_ids()
    }
    fn mob_breeding_advancements(&self, kind: u16, credit: &crate::mob_breeding::MobBreedingCredit,
        state: &crate::player_advancement::PlayerAdvancementState)
        -> Result<crate::player_advancement::PlayerAdvancementState, ApplyFailure> {
        self.owners.mob_breeding_advancements(kind, credit, state)
    }
    fn derive_item_use(
        &self,
        intent: &crate::item_use::ItemUseIntent,
        id: crate::canonical_action::ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.owners.derive_item_use(intent, id, tick, values)
    }
    fn crossbow_items(&self, intent: &crate::crossbow_items::CrossbowItems, id: crate::canonical_action::ActionId, tick: TickStamp,
        values: &BTreeMap<Target, Cell>) -> Result<BTreeMap<Target, InventoryCell>, ApplyFailure> {
        self.owners.crossbow_items(intent, id, tick, values)
    }
    fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
        self.owners.vehicle_activator(state)
    }
    fn vehicle_drop_item(&self, body: &crate::entity_body::EntityBody)
        -> Result<InventoryCell, ApplyFailure> {
        self.owners.vehicle_drop_item(body)
    }
    fn issued_item_birth(&self, identity: &crate::entity_origin::EntityOriginIdentity,
        cause: crate::canonical_action::ActionRef, owner: crate::realm_completion::PeerGeneration,
        spatial: crate::entity_body::EntitySpatial, cell: &crate::inventory::InventoryCell, pickup_delay: u16)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        self.owners.issued_item_birth(identity, cause, owner, spatial, cell, pickup_delay)
    }
    fn dropped_item_body(&self, template: &crate::entity_body::EntityBody, source: &InventoryCell)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        self.owners.dropped_item_body(template, source)
    }
    fn recipe_known(&self, id: &str) -> bool { self.owners.recipe_known(id) }
    fn action_source_slot(&self, reference: crate::canonical_action::ActionRef) -> Option<u64> {
        self.owners.action_source_slot(reference)
    }
    fn player_advancement_outcome(&self, event: &crate::player_advancement::AdvancementEvent,
        state: &crate::player_advancement::PlayerAdvancementState, achieved_millis: u64)
        -> Result<crate::player_advancement::AdvancementOutcome, crate::canonical_action::ApplyFailure> {
        self.owners.player_advancement_outcome(event, state, achieved_millis)
    }

    fn entity_placement_matches(&self, intent: &crate::entity_placement::EntityPlacementIntent) -> bool { self.owners.entity_placement_matches(intent) }
    fn falling_damage_kind(&self, state: u16) -> Option<crate::combat_action::DamageKind> { self.owners.falling_damage_kind(state) }
    fn primed_tnt_block(&self, state: u16) -> bool {
        self.owners.primed_tnt_block(state)
    }
    fn primed_tnt_identity(&self) -> Option<(u16, u16)> {
        self.owners.primed_tnt_identity()
    }
    fn issuer_entity_admission_authority(&self, reference: crate::canonical_action::ActionRef,
        witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.owners.issuer_entity_admission_authority(reference, witness)
    }
    fn issuer_entity_initialization_matches(&self, witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.owners.issuer_entity_initialization_matches(witness)
    }
    fn world_rules(&self, witness: crate::world_rules::WorldRulesWitness, tick: crate::time::TickStamp)
        -> Result<crate::world_rules::WorldRulesSnapshot, ApplyFailure> {
        self.owners.world_rules(witness, tick)
    }
    fn death_inventory_rule(&self, action: crate::canonical_action::ActionRef, rules: crate::world_rules::WorldRulesWitness, keep_inventory: bool, target: crate::canonical_action::Target)
        -> Result<(), ApplyFailure> {
        self.owners.death_inventory_rule(action, rules, keep_inventory, target)
    }
    fn death_inventory_drop(&self, template: &crate::entity_body::EntityBody, source: &crate::inventory::InventoryCell)
        -> Result<Option<crate::entity_body::EntityBody>, ApplyFailure> {
        self.owners.death_inventory_drop(template, source)
    }
    fn derive_cloud_potion(&self, intent: &crate::area_cloud::AreaCloudIntent, victim: &crate::item_contact::ContactObservation,
        id: crate::canonical_action::ActionId, tick: crate::time::TickStamp, values: &BTreeMap<crate::canonical_action::Target, crate::canonical_action::Cell>) -> Result<crate::area_cloud::CloudPotionOutcome, crate::canonical_action::ApplyFailure> { self.owners.derive_cloud_potion(intent, victim, id, tick, values) }
    fn derive_projectile_tick(
&self, intent: &crate::projectile_tick::ProjectileTickIntent, id: crate::canonical_action::ActionId, tick: crate::time::TickStamp,
        values: &BTreeMap<Target, crate::canonical_action::Cell>) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.owners.derive_projectile_tick(intent, id, tick, values)
    }
    fn mending_repair(&self, source: &crate::inventory::InventoryCell, experience: u32)
        -> Result<(crate::canonical_inventory::ItemTransformOutput, u32), ApplyFailure> {
        self.owners.mending_repair(source, experience)
    }
    fn item_cooldown_group(&self, source: &crate::inventory::InventoryCell) -> Result<String, ApplyFailure> {
        self.owners.item_cooldown_group(source)
    }
    fn derive_block_inventory_tick(&self, intent: &crate::canonical_block_inventory_tick::BlockInventoryTickIntent,
        values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>) -> Result<crate::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
        self.owners.derive_block_inventory_tick(intent, values)
    }
    fn machine_genesis(&self, chunk: crate::protocol::ChunkKey) -> Result<[u8; 32], ApplyFailure> { self.owners.machine_genesis(chunk) }
    fn machine_tick_footprint(&self, identity: crate::action_identity::MachineActionId, values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>) -> Result<crate::action_command::ActionFootprint, ApplyFailure> { self.owners.machine_tick_footprint(identity, values) }
    fn derive_machine_tick(&self, identity: crate::action_identity::MachineActionId, values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>) -> Result<Vec<crate::canonical_action::AppliedWrite>, ApplyFailure> { self.owners.derive_machine_tick(identity, values) }
    fn menu_inventory_transform(&self, kind: crate::canonical_menu_transform::MenuInventoryTransformKind,
        selection: &crate::canonical_menu::MenuSelection, inputs: &[crate::inventory::InventoryCell])
        -> Result<crate::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure> {
        self.owners.menu_inventory_transform(kind, selection, inputs)
    }
    fn item_transform(
        &self,
        source: &InventoryCell,
        operation: &crate::canonical_inventory::ItemTransformOperation,
        id: crate::canonical_action::ActionId,
        tick: TickStamp,
    ) -> Result<crate::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        self.owners.item_transform(source, operation, id, tick)
    }
    fn derive_world_light(
        &self,
        plan: crate::world_action::WorldLightPlan,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.owners.derive_world_light(plan, values)
    }
    fn inventory_equipment_facts(
        &self,
        cells: &[(
            crate::canonical_inventory::InventoryEquipmentSlot,
            InventoryCell,
        )],
    ) -> Result<crate::combat_action::CombatEquipmentFacts, ApplyFailure> {
        self.owners.inventory_equipment_facts(cells)
    }
    fn borrow(&self, view: TruthView, target: Target) -> Option<Cow<'_, Cell>> {
        match view {
            TruthView::Local => ActorActionScope::read(self, target),
            TruthView::Ground => self.owners.borrow(view, target),
        }
    }

    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        match view {
            TruthView::Local => ActorActionScope::read(self, target).map(Cow::into_owned),
            TruthView::Ground => self.owners.read(view, target),
        }
    }

    fn validate_live_player_lease(&self, lease: LivePlayerLease) -> bool {
        self.owners.validate_live_player_lease(lease)
    }

    fn available(&self, view: TruthView, target: Target) -> bool {
        if view == TruthView::Local && self.read(target).is_some() {
            return true;
        }
        self.owners.available(view, target)
    }

    fn entity_equipment_compatible(
        &self,
        kind: u16,
        slots: &[(crate::canonical_inventory::InventoryEquipmentSlot, crate::inventory::InventoryCell)],
    ) -> Result<(), ApplyFailure> {
        self.owners.entity_equipment_compatible(kind, slots)
    }

    fn validate_block_inventory_layout(&self, block: Target, state: u16, block_entity: &Cell,
        bindings: &[crate::canonical_inventory::InventorySlotBinding]) -> Result<(), ApplyFailure> {
        self.owners.validate_block_inventory_layout(block, state, block_entity, bindings)
    }
    fn entity_inventory_storage_capacity(
        &self,
        kind: u16,
        facets: &[(crate::mob_species_property::MobSpeciesProperty, crate::mob_species_property::MobSpeciesPropertyValue)],
        storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        self.owners.entity_inventory_storage_capacity(kind, facets, storage_slots)
    }

    fn entity_inventory_layout(
        &self,
        body: &crate::entity_body::EntityBody,
    ) -> Result<Vec<crate::inventory::InvLoc>, ApplyFailure> {
        self.owners.entity_inventory_layout(body)
    }
    fn entity_inventory_limit(
        &self,
        body: &crate::entity_body::EntityBody,
        item: &InventoryCell,
        target: Target,
    ) -> Result<u8, ApplyFailure> {
        self.owners.entity_inventory_limit(body, item, target)
    }

    fn inventory_limit(
        &self,
        source: &InventoryCell,
        destination: Target,
    ) -> Result<u8, ApplyFailure> {
        self.owners.inventory_limit(source, destination)
    }

    fn commit(
        &mut self,
        _context: ActionContext,
        _view: TruthView,
        _writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}
