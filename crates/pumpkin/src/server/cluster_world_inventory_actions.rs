use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{
    Action, ActionId, ActionProposal, ActionState, ApplyFailure, Cell, ChunkPart, ClaimedAction, InventoryId,
    LivePlayerLease, PlayerKey, Target, TruthView, Value,
};
use pumpkin_cluster::canonical_inventory::{
    InventoryActionCapture, InventoryEquipmentBinding, InventoryGestureReplayIntent,
    InventorySlotBinding,
};
use pumpkin_cluster::inventory::{
    InventoryApplyError, InventoryIntentPayload, InventorySlotRef, InventoryTarget,
};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;

use super::Server;

pub(super) fn pickup_destinations<S: pumpkin_cluster::canonical_action::ActionState>(
    view: &S,
    lease: LivePlayerLease,
    item: &pumpkin_cluster::inventory::InventoryCell,
) -> Result<
    (
        Vec<pumpkin_cluster::canonical_action::InventoryReplayCandidate>,
        pumpkin_cluster::canonical_action::InventoryDestinationPolicy,
        pumpkin_cluster::canonical_action::Condition,
    ),
    ApplyFailure,
> {
    let target = |inv, slot| Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(InventoryId::Player { inv, slot }),
    };
    let selector = target(pumpkin_cluster::inventory::INV_SELECTED, 0);
    let cell = view
        .read(TruthView::Local, selector)
        .ok_or(ApplyFailure::Inventory(selector))?;
    let pumpkin_cluster::canonical_action::Value::Inventory(selected) = &cell.value else {
        return Err(ApplyFailure::Type(selector));
    };
    let selected = selected
        .item
        .checked_sub(1)
        .filter(|slot| *slot < 9)
        .ok_or(ApplyFailure::Inventory(selector))?;
    let mut destinations = Vec::with_capacity(37);
    for destination in std::iter::once(target(pumpkin_cluster::inventory::INV_MAIN, selected))
        .chain(std::iter::once(target(
            pumpkin_cluster::inventory::INV_OFFHAND,
            0,
        )))
        .chain(
            (0..36)
                .filter(|slot| *slot != selected)
                .map(|slot| target(pumpkin_cluster::inventory::INV_MAIN, slot)),
        )
    {
        if view.read(TruthView::Local, destination).is_none() {
            return Err(ApplyFailure::Inventory(destination));
        }
        destinations.push(
            pumpkin_cluster::canonical_action::InventoryReplayCandidate {
                target: destination,
                max_stack: view.inventory_limit(item, destination)?,
            },
        );
    }
    Ok((
        destinations,
        pumpkin_cluster::canonical_action::InventoryDestinationPolicy::MergeThenEmpty {
            merge_only: vec![target(pumpkin_cluster::inventory::INV_OFFHAND, 0)],
        },
        pumpkin_cluster::canonical_action::Condition {
            target: selector,
            expected: pumpkin_cluster::canonical_action::Expected::Exact(cell),
        },
    ))
}
use super::cluster_invsee::LocalInventoryAction;
use super::cluster_world_inventory::InventoryOwner;

pub(super) struct InventoryPreviewCapture<'a, S> {
    base: &'a S,
    screen: Option<pumpkin_cluster::inventory::InventoryScreenSnapshot>,
}

impl<'a, S> InventoryPreviewCapture<'a, S> {
    pub(super) fn new(
        base: &'a S,
        server: &Server,
        session: Option<pumpkin_cluster::inventory::InventoryScreenSession>,
    ) -> Self {
        let screen = session
            .and_then(|session| super::cluster_invsee::inventory_screen_snapshot(server, session))
            .filter(|screen| trusted_preview(server, screen));
        Self { base, screen }
    }

    fn witness(&self, target: Target) -> Option<pumpkin_cluster::canonical_action::Cell> {
        let screen = self.screen.as_ref()?;
        let lease = screen.preview_lease?;
        let authority = screen.preview_authority.as_ref()?;
        match target {
            Target::Player {
                key,
                lease: Some(expected),
                inventory: Some(InventoryId::Player { inv, slot }),
            } if key == lease.key && expected == lease => authority
                .inventory_cells
                .get(&pumpkin_cluster::inventory::InvLoc::new(inv, slot))
                .cloned(),
            Target::Chunk {
                key,
                part: ChunkPart::Player(gid),
                inventory: None,
            } if gid == lease.gid && screen.affected.as_slice() == [key] => {
                Some(authority.living.clone())
            }
            target @ Target::Chunk {
                key,
                part:
                    ChunkPart::LivingProperty {
                        actor: ActorId::Player(gid),
                        ..
                    }
                    | ChunkPart::ActorRelation {
                        actor: ActorId::Player(gid),
                        ..
                    },
                inventory: None,
            } if gid == lease.gid && screen.affected.as_slice() == [key] => {
                pumpkin_cluster::derived_living::actor_facet_cell(&authority.living, target)
            }
            _ => None,
        }
    }
}

impl<S: pumpkin_cluster::canonical_action::ActionState>
    pumpkin_cluster::canonical_action::ActionState for InventoryPreviewCapture<'_, S>
{
    fn wake_rest_stat(&self) -> Result<(u16, u32), pumpkin_cluster::canonical_action::ApplyFailure> { self.base.wake_rest_stat() }

    fn validate_wake_beds(&self, world: pumpkin_cluster::protocol::WorldId, sleeping: pumpkin_cluster::item_action::PlayerSleepingState,
        beds: &[pumpkin_cluster::sleep_action::BedRelease]) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.base.validate_wake_beds(world, sleeping, beds)
    }

    fn validate_morning(&self, world: pumpkin_cluster::protocol::WorldId, morning: pumpkin_cluster::world_time::MorningTransition, source: pumpkin_cluster::canonical_action::ActionRef) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.base.validate_morning(world, morning, source)
    }

    fn validate_world_border(&self, witness: pumpkin_cluster::world_time::WorldBorderWitness, source: pumpkin_cluster::canonical_action::ActionRef) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.base.validate_world_border(witness, source)
    }

    fn fall_damage_factor(&self, state: u16, sneaking: bool) -> Result<u16, ApplyFailure> {
        self.base.fall_damage_factor(state, sneaking)
    }
    fn environmental_hazard(
        &self,
        kind: pumpkin_cluster::combat_action::EnvironmentalHazardKind,
        block_state: u16,
    ) -> Result<Option<pumpkin_cluster::combat_action::EnvironmentalHazardFacts>, ApplyFailure>
    {
        self.base.environmental_hazard(kind, block_state)
    }
    fn status_effect_facts(
        &self,
        effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
    ) -> Result<pumpkin_cluster::combat_action::CombatStatusFacts, ApplyFailure> {
        self.base.status_effect_facts(effects)
    }
    fn status_effect_modifiers(
        &self,
        effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
    ) -> Result<Vec<pumpkin_cluster::combat_action::CombatAttributeModifier>, ApplyFailure> {
        self.base.status_effect_modifiers(effects)
    }
    fn derive_item_use(
        &self,
        intent: &pumpkin_cluster::item_use::ItemUseIntent,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    ) -> Result<Vec<pumpkin_cluster::canonical_action::Effect>, ApplyFailure> {
        self.base.derive_item_use(intent, id, tick, values)
    }
    fn crossbow_items(&self, intent: &pumpkin_cluster::crossbow_items::CrossbowItems, id: ActionId, tick: TickStamp,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>) -> Result<BTreeMap<Target, pumpkin_cluster::inventory::InventoryCell>, ApplyFailure> {
        self.base.crossbow_items(intent, id, tick, values)
    }
    fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
        self.base.vehicle_activator(state)
    }
    fn vehicle_drop_item(&self, body: &pumpkin_cluster::entity_body::EntityBody)
        -> Result<pumpkin_cluster::inventory::InventoryCell, ApplyFailure> {
        self.base.vehicle_drop_item(body)
    }
    fn dropped_item_body(&self, template: &pumpkin_cluster::entity_body::EntityBody, source: &pumpkin_cluster::inventory::InventoryCell)
        -> Result<pumpkin_cluster::entity_body::EntityBody, ApplyFailure> {
        self.base.dropped_item_body(template, source)
    }
    fn recipe_known(&self, id: &str) -> bool { self.base.recipe_known(id) }
    fn action_source_slot(&self, reference: pumpkin_cluster::canonical_action::ActionRef) -> Option<u64> {
        self.base.action_source_slot(reference)
    }
    fn player_advancement_outcome(&self, event: &pumpkin_cluster::player_advancement::AdvancementEvent,
        state: &pumpkin_cluster::player_advancement::PlayerAdvancementState, achieved_millis: u64)
        -> Result<pumpkin_cluster::player_advancement::AdvancementOutcome, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.base.player_advancement_outcome(event, state, achieved_millis)
    }

    fn issuer_entity_admission_authority(&self, reference: pumpkin_cluster::canonical_action::ActionRef,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.base.issuer_entity_admission_authority(reference, witness)
    }
    fn issuer_entity_initialization_matches(&self,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.base.issuer_entity_initialization_matches(witness)
    }
    fn world_rules(&self, witness: pumpkin_cluster::world_rules::WorldRulesWitness, tick: pumpkin_cluster::time::TickStamp)
        -> Result<pumpkin_cluster::world_rules::WorldRulesSnapshot, ApplyFailure> {
        self.base.world_rules(witness, tick)
    }
    fn death_inventory_rule(&self, action: pumpkin_cluster::canonical_action::ActionRef, rules: pumpkin_cluster::world_rules::WorldRulesWitness, keep_inventory: bool, target: pumpkin_cluster::canonical_action::Target)
        -> Result<(), ApplyFailure> {
        self.base.death_inventory_rule(action, rules, keep_inventory, target)
    }
    fn death_inventory_drop(&self, template: &pumpkin_cluster::entity_body::EntityBody, source: &pumpkin_cluster::inventory::InventoryCell)
        -> Result<Option<pumpkin_cluster::entity_body::EntityBody>, ApplyFailure> {
        self.base.death_inventory_drop(template, source)
    }
    fn derive_projectile_tick(&self, intent: &pumpkin_cluster::projectile_tick::ProjectileTickIntent, id: pumpkin_cluster::canonical_action::ActionId, tick: pumpkin_cluster::time::TickStamp,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>) -> Result<Vec<pumpkin_cluster::canonical_action::Effect>, ApplyFailure> {
        self.base.derive_projectile_tick(intent, id, tick, values)
    }
    fn mending_repair(&self, source: &pumpkin_cluster::inventory::InventoryCell, experience: u32)
        -> Result<(pumpkin_cluster::canonical_inventory::ItemTransformOutput, u32), ApplyFailure> {
        self.base.mending_repair(source, experience)
    }
    fn derive_block_inventory_tick(&self, intent: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
        values: &std::collections::BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>) -> Result<pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
        self.base.derive_block_inventory_tick(intent, values)
    }
    fn menu_inventory_transform(&self, kind: pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind,
        selection: &pumpkin_cluster::canonical_menu::MenuSelection, inputs: &[pumpkin_cluster::inventory::InventoryCell])
        -> Result<pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure> {
        self.base.menu_inventory_transform(kind, selection, inputs)
    }
    fn item_transform(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
        operation: &pumpkin_cluster::canonical_inventory::ItemTransformOperation,
        id: pumpkin_cluster::canonical_action::ActionId,
        tick: TickStamp,
    ) -> Result<pumpkin_cluster::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        self.base.item_transform(source, operation, id, tick)
    }
    fn read(
        &self,
        view: TruthView,
        target: Target,
    ) -> Option<pumpkin_cluster::canonical_action::Cell> {
        self.base.read(view, target).or_else(|| {
            (view == TruthView::Local)
                .then(|| self.witness(target))
                .flatten()
        })
    }
    fn available(&self, view: TruthView, target: Target) -> bool {
        self.base.available(view, target)
            || view == TruthView::Local && self.witness(target).is_some()
    }
    fn validate_live_player_lease(&self, lease: LivePlayerLease) -> bool {
        self.base.validate_live_player_lease(lease)
            || self
                .screen
                .as_ref()
                .is_some_and(|screen| screen.preview_lease == Some(lease))
    }
    fn entity_equipment_compatible(
        &self,
        kind: u16,
        slots: &[(pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot, pumpkin_cluster::inventory::InventoryCell)],
    ) -> Result<(), ApplyFailure> {
        self.base.entity_equipment_compatible(kind, slots)
    }

    fn validate_block_inventory_layout(&self, block: Target, state: u16, block_entity: &Cell,
        bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding]) -> Result<(), ApplyFailure> {
        super::cluster_inventory_block_roots::validate_layout(block, state, block_entity, bindings)
    }
    fn entity_inventory_storage_capacity(
        &self,
        kind: u16,
        facets: &[(pumpkin_cluster::mob_species_property::MobSpeciesProperty, pumpkin_cluster::mob_species_property::MobSpeciesPropertyValue)],
        storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        self.base.entity_inventory_storage_capacity(kind, facets, storage_slots)
    }

    fn entity_inventory_layout(
        &self,
        body: &pumpkin_cluster::entity_body::EntityBody,
    ) -> Result<Vec<pumpkin_cluster::inventory::InvLoc>, ApplyFailure> {
        self.base.entity_inventory_layout(body)
    }

    fn entity_inventory_limit(
        &self,
        body: &pumpkin_cluster::entity_body::EntityBody,
        item: &pumpkin_cluster::inventory::InventoryCell,
        target: Target,
    ) -> Result<u8, ApplyFailure> {
        self.base.entity_inventory_limit(body, item, target)
    }

    fn inventory_limit(
        &self,
        source: &pumpkin_cluster::inventory::InventoryCell,
        destination: Target,
    ) -> Result<u8, ApplyFailure> {
        self.base.inventory_limit(source, destination)
    }
    fn actor_occupancy_cut(&self, target: Target, value: &Value) -> Result<(pumpkin_cluster::realm_completion::PeerGeneration, pumpkin_cluster::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        self.base.actor_occupancy_cut(target, value)
    }
    fn actor_collision_shape(&self, target: Target, value: &Value) -> Result<pumpkin_cluster::actor_occupancy::CollisionShape, ApplyFailure> {
        self.base.actor_collision_shape(target, value)
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        self.base.inventory_equipment_attribute_ids()
    }
    fn inventory_equipment_facts(
        &self,
        cells: &[(
            pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot,
            pumpkin_cluster::inventory::InventoryCell,
        )],
    ) -> Result<pumpkin_cluster::combat_action::CombatEquipmentFacts, ApplyFailure> {
        self.base.inventory_equipment_facts(cells)
    }
    fn derive_world_light(
        &self,
        plan: pumpkin_cluster::world_action::WorldLightPlan,
        values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    ) -> Result<Vec<pumpkin_cluster::canonical_action::Effect>, ApplyFailure> {
        self.base.derive_world_light(plan, values)
    }
    fn commit(
        &mut self,
        _: pumpkin_cluster::canonical_action::ActionContext,
        _: TruthView,
        _: &[pumpkin_cluster::canonical_action::AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}

fn trusted_preview(
    server: &Server,
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
) -> bool {
    let (Some(lease), Some(authority), Some(preview)) = (
        screen.preview_lease,
        screen.preview_authority.as_ref(),
        screen.preview.as_ref(),
    ) else {
        return false;
    };
    matches!(preview.target, InventoryTarget::Player(gid) if gid == lease.gid)
        && screen.preview_incarnation == Some(lease.incarnation)
        && authority.is_valid(lease, &screen.affected)
        && server
            .cluster_peer_incarnations
            .load()
            .get(&authority.provider.server)
            .copied()
            == Some(authority.provider.generation)
}

trait InventoryCaptureSource {
    fn current_inventory_address(&self, target: InventoryTarget) -> Option<InventoryTarget>;
    fn player_lease_for_gid(&self, gid: pumpkin_cluster::identity::GlobalPlayerId) -> Option<LivePlayerLease>;
    fn live_player_lease_matches(&self, lease: LivePlayerLease) -> bool;
    fn local_target_chunks(&self, target: InventoryTarget) -> Option<Vec<ChunkKey>>;
    fn phase_snapshot(&self, view: TruthView, target: InventoryTarget) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot>;
    fn source_screen(&self, server: &Server, session: pumpkin_cluster::inventory::InventoryScreenSession) -> Option<pumpkin_cluster::inventory::InventoryScreenSnapshot>;
    fn trusted_preview(&self, server: &Server, screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot) -> bool;
}

impl InventoryCaptureSource for InventoryOwner {
    fn current_inventory_address(&self, target: InventoryTarget) -> Option<InventoryTarget> { InventoryOwner::current_inventory_address(self, target) }
    fn player_lease_for_gid(&self, gid: pumpkin_cluster::identity::GlobalPlayerId) -> Option<LivePlayerLease> { InventoryOwner::player_lease_for_gid(self, gid) }
    fn live_player_lease_matches(&self, lease: LivePlayerLease) -> bool { InventoryOwner::live_player_lease_matches(self, lease) }
    fn local_target_chunks(&self, target: InventoryTarget) -> Option<Vec<ChunkKey>> { InventoryOwner::local_target_chunks(self, target) }
    fn phase_snapshot(&self, view: TruthView, target: InventoryTarget) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> { InventoryOwner::phase_snapshot(self, view, target) }
    fn source_screen(&self, server: &Server, session: pumpkin_cluster::inventory::InventoryScreenSession) -> Option<pumpkin_cluster::inventory::InventoryScreenSnapshot> { super::cluster_invsee::inventory_screen_snapshot(server, session) }
    fn trusted_preview(&self, server: &Server, screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot) -> bool { trusted_preview(server, screen) }
}

impl InventoryCaptureSource for super::cluster_world_callback_snapshot::CallbackTickSnapshot {
    fn current_inventory_address(&self, target: InventoryTarget) -> Option<InventoryTarget> {
        let resolved = match target {
            InventoryTarget::Player(gid) => { let lease = self.inventory_source_lease(gid)?; InventoryTarget::LeasedPlayer { key: lease.key.0, player: gid, incarnation: lease.incarnation } }
            InventoryTarget::Virtual { host, owner: ActorId::Player(gid), session, kind } => { let lease = self.inventory_source_lease(gid)?; InventoryTarget::LeasedVirtual { host, key: lease.key.0, player: gid, incarnation: lease.incarnation, session, kind } }
            _ => target,
        };
        self.captured_inventory_targets().any(|(_, owner, _)| owner == resolved).then_some(resolved)
            .or_else(|| self.captured_inventory_targets().any(|(_, owner, _)| owner == target).then_some(target))
    }
    fn player_lease_for_gid(&self, gid: pumpkin_cluster::identity::GlobalPlayerId) -> Option<LivePlayerLease> { self.inventory_source_lease(gid) }
    fn live_player_lease_matches(&self, lease: LivePlayerLease) -> bool { self.validate_live_player_lease(lease) }
    fn local_target_chunks(&self, target: InventoryTarget) -> Option<Vec<ChunkKey>> {
        let target = self.current_inventory_address(target)?;
        match target {
            InventoryTarget::Player(gid) | InventoryTarget::LeasedPlayer { player: gid, .. } | InventoryTarget::LeasedVirtual { player: gid, .. } | InventoryTarget::Virtual { owner: ActorId::Player(gid), .. } => self.player_relation_participant(self.inventory_source_lease(gid)?).map(|participant| vec![participant.chunk]),
            _ => { let chunks = self.captured_inventory_targets().filter_map(|(address, owner, _)| (owner == target).then_some(address).and_then(|address| match address { Target::Chunk { key, .. } => Some(key), _ => None })).collect::<BTreeSet<_>>(); (!chunks.is_empty()).then(|| chunks.into_iter().collect()) }
        }
    }
    fn phase_snapshot(&self, view: TruthView, target: InventoryTarget) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        if view != TruthView::Local { return None; }
        let target = self.current_inventory_address(target)?;
        let mut revision = 0;
        let mut cells = Vec::new();
        for (address, owner, _) in self.captured_inventory_targets().filter(|(_, owner, _)| *owner == target) {
            let _ = owner;
            let Cell { revision: causal, value: Value::Inventory(cell) } = self.read(view, address)? else { return None; };
            revision = revision.max(causal); cells.push(cell);
        }
        (!cells.is_empty()).then_some(pumpkin_cluster::inventory::InventoryStateSnapshot { target, revision, cells })
    }
    fn source_screen(&self, _: &Server, session: pumpkin_cluster::inventory::InventoryScreenSession) -> Option<pumpkin_cluster::inventory::InventoryScreenSnapshot> { self.inventory_source_screen(session) }
    fn trusted_preview(&self, _: &Server, screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot) -> bool { self.inventory_source_preview_trusted(screen) }
}

pub(super) fn binding_target(
    owner: &InventoryOwner,
    preview: Option<&pumpkin_cluster::inventory::InventoryScreenSnapshot>,
    slot: InventorySlotRef,
) -> Option<Target> {
    binding_target_in(owner, preview, slot)
}

pub(super) fn binding_target_from_snapshot(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    preview: Option<&pumpkin_cluster::inventory::InventoryScreenSnapshot>, slot: InventorySlotRef,
) -> Option<Target> { binding_target_in(original, preview, slot) }

pub(super) fn current_inventory_address_from_snapshot(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot, target: InventoryTarget,
) -> Option<InventoryTarget> { InventoryCaptureSource::current_inventory_address(original, target) }

fn binding_target_in(
    owner: &impl InventoryCaptureSource,
    preview: Option<&pumpkin_cluster::inventory::InventoryScreenSnapshot>,
    slot: InventorySlotRef,
) -> Option<Target> {
    let loc = slot.loc;
    let player_target = |lease: LivePlayerLease, inventory: InventoryId| Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(inventory),
    };
    let current_lease = |gid| {
        owner
            .player_lease_for_gid(gid)
            .filter(|lease| owner.live_player_lease_matches(*lease))
            .or_else(|| {
                preview
                    .and_then(|screen| screen.preview_lease)
                    .filter(|lease| lease.gid == gid)
            })
    };
    let chunk = || {
        let chunks = owner.local_target_chunks(slot.target)?;
        (chunks.len() == 1).then_some(chunks[0])
    };
    match slot.target {
        InventoryTarget::Player(gid) => Some(player_target(
            current_lease(gid)?,
            InventoryId::Player {
                inv: loc.inv,
                slot: loc.slot,
            },
        )),
        InventoryTarget::LeasedPlayer {
            key,
            player,
            incarnation,
        } => {
            let lease = current_lease(player)?;
            (lease.key.0 == key && lease.incarnation == incarnation).then_some(player_target(
                lease,
                InventoryId::Player {
                    inv: loc.inv,
                    slot: loc.slot,
                },
            ))
        }
        InventoryTarget::Block { chunk, position } => Some(Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(position),
            inventory: Some(InventoryId::Block {
                inv: loc.inv,
                slot: loc.slot,
            }),
        }),
        InventoryTarget::Entity(entity) => Some(Target::Chunk {
            key: chunk()?,
            part: ChunkPart::Entity(pumpkin_cluster::protocol::GlobalEntityId {
                origin: entity.origin,
                local_id: entity.local_id,
                generation: entity.generation,
            }),
            inventory: Some(InventoryId::Entity {
                inv: loc.inv,
                slot: loc.slot,
            }),
        }),
        InventoryTarget::Virtual {
            host,
            owner: ActorId::Player(gid),
            session,
            kind,
        } => Some(player_target(
            current_lease(gid)?,
            InventoryId::Virtual {
                host,
                kind,
                session,
                inv: loc.inv,
                slot: loc.slot,
            },
        )),
        InventoryTarget::LeasedVirtual {
            host,
            key,
            player,
            incarnation,
            session,
            kind,
        } => {
            let lease = current_lease(player)?;
            (lease.key.0 == key && lease.incarnation == incarnation).then_some(player_target(
                lease,
                InventoryId::Virtual {
                    host,
                    kind,
                    session,
                    inv: loc.inv,
                    slot: loc.slot,
                },
            ))
        }
        InventoryTarget::Virtual {
            host,
            owner: ActorId::Entity(entity),
            session,
            kind,
        } => Some(Target::Chunk {
            key: chunk()?,
            part: ChunkPart::Entity(entity),
            inventory: Some(InventoryId::Virtual {
                host,
                kind,
                session,
                inv: loc.inv,
                slot: loc.slot,
            }),
        }),
        InventoryTarget::BlockVirtual {
            host,
            chunk,
            position,
            session,
            kind,
        } => Some(Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(position),
            inventory: Some(InventoryId::Virtual {
                host,
                kind,
                session,
                inv: loc.inv,
                slot: loc.slot,
            }),
        }),
        InventoryTarget::Virtual {
            owner: ActorId::Peer(_),
            ..
        } => None,
    }
}

pub(super) fn entity_inventory_slot_limit(
    body: &pumpkin_cluster::entity_body::EntityBody,
    item: &pumpkin_cluster::inventory::InventoryCell,
    target: Target,
) -> Result<u8, ApplyFailure> {
    let Target::Chunk {
        part: ChunkPart::Entity(entity),
        inventory: Some(InventoryId::Entity { inv, slot }),
        ..
    } = target
    else {
        return Err(ApplyFailure::Inventory(target));
    };
    if entity != body.id || item.loc != pumpkin_cluster::inventory::InvLoc::new(inv, slot) {
        return Err(ApplyFailure::Inventory(target));
    }
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
        .map_err(|_| ApplyFailure::Inventory(target))?;
    super::cluster_owned_mob_birth::inventory_slot_limit(body, &stack, item.loc)
}

pub(super) fn inventory_slot_limit(
    source: &pumpkin_cluster::inventory::InventoryCell,
    address: InventoryTarget,
    loc: pumpkin_cluster::inventory::InvLoc,
    destination: Target,
) -> Result<u8, ApplyFailure> {
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| ApplyFailure::Inventory(destination))?;
    if matches!(address, InventoryTarget::Entity(_))
        && loc.inv == pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT
        && (2..=7).contains(&loc.slot)
    {
        return Ok(1);
    }
    if matches!(
        address,
        InventoryTarget::Player(_) | InventoryTarget::LeasedPlayer { .. }
    ) && loc.inv == pumpkin_cluster::inventory::INV_ARMOR
    {
        let allowed = match loc.slot {
            0 => stack.is_boots(),
            1 => stack.is_leggings(),
            2 => stack.is_chestplate() || stack.item == &pumpkin_data::item::Item::ELYTRA,
            3 => {
                stack.is_helmet()
                    || stack.is_skull()
                    || stack.item == &pumpkin_data::item::Item::CARVED_PUMPKIN
            }
            _ => false,
        };
        if !allowed {
            return Err(ApplyFailure::Inventory(destination));
        }
        Ok(1)
    } else {
        Ok(stack.get_max_stack_size())
    }
}

pub(super) fn entity_equipment_binding<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S, body: &pumpkin_cluster::entity_body::EntityBody, chunk: ChunkKey,
    bindings: &[InventorySlotBinding],
) -> Result<Option<pumpkin_cluster::canonical_inventory::EntityInventoryEquipmentBinding>, ApplyFailure> {
    use pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot as R;
    use pumpkin_cluster::mob_species_property::MobSpeciesProperty as P;
    let pumpkin_cluster::entity_body::EntityPayload::Living(actor) = &body.payload else { return Ok(None); };
    let entity = body.id;
    let mut slots = Vec::new();
    let mut storage_slots = Vec::new();
    for loc in state.entity_inventory_layout(body)? {
        let target = Target::Chunk { key: chunk, part: ChunkPart::Entity(entity), inventory: Some(InventoryId::Entity { inv: loc.inv, slot: loc.slot }) };
        if !bindings.iter().any(|binding| binding.target == target && binding.slot.loc == loc
            && binding.slot.target == InventoryTarget::Entity(pumpkin_cluster::inventory::EntityInventoryId {
                origin: entity.origin, local_id: entity.local_id, generation: entity.generation })) {
            return Err(ApplyFailure::Inventory(target));
        }
        if loc.inv == pumpkin_cluster::inventory::INV_MAIN { storage_slots.push(target); continue; }
        if loc.inv != pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT { return Err(ApplyFailure::InvalidAction); }
        let role = match loc.slot { 0 => R::MainHand, 1 => R::OffHand, 2 => R::Feet, 3 => R::Legs,
            4 => R::Chest, 5 => R::Head, 6 => R::Body, 7 => R::Saddle, _ => return Err(ApplyFailure::InvalidAction) };
        slots.push((role, target));
    }
    let storage_properties = match pumpkin_data::entity::EntityType::from_raw(body.kind).map(|kind| kind.resource_name) {
        Some("donkey" | "mule") => vec![P::HorseChested],
        Some("llama" | "trader_llama") => vec![P::LlamaChested, P::LlamaStrength],
        _ => Vec::new(),
    };
    let mut attribute_ids = state.inventory_equipment_attribute_ids();
    attribute_ids.extend(actor.combat.effective_attributes.keys().copied());
    attribute_ids.sort_unstable(); attribute_ids.dedup();
    let binding = pumpkin_cluster::canonical_inventory::EntityInventoryEquipmentBinding {
        entity, birth: body.birth.clone(), kind: body.kind,
        living: Target::Chunk { key: chunk, part: ChunkPart::Entity(entity), inventory: None },
        slots, storage_slots, storage_properties, attribute_ids,
    };
    if !binding.is_valid() { return Err(ApplyFailure::InvalidAction); }
    Ok(Some(binding))
}

pub(super) fn entity_inventory_storage_capacity(
    kind: u16,
    facets: &[(pumpkin_cluster::mob_species_property::MobSpeciesProperty, pumpkin_cluster::mob_species_property::MobSpeciesPropertyValue)],
    storage_slots: u16,
) -> Result<u16, ApplyFailure> {
    use pumpkin_cluster::mob_species_property::{MobSpeciesProperty as P, MobSpeciesPropertyValue as V};
    let kind = pumpkin_data::entity::EntityType::from_raw(kind).ok_or(ApplyFailure::InvalidAction)?;
    let potential = match kind.resource_name {
        "donkey" | "mule" | "llama" | "trader_llama" => 15,
        "piglin" | "villager" => 8,
        _ => 0,
    };
    if storage_slots != potential { return Err(ApplyFailure::InvalidAction); }
    match kind.resource_name {
        "donkey" | "mule" => match facets {
            [(P::HorseChested, V::Flag(chested))] => Ok(if *chested { 15 } else { 0 }),
            _ => Err(ApplyFailure::InvalidAction),
        },
        "llama" | "trader_llama" => {
            if facets.len() != 2 { return Err(ApplyFailure::InvalidAction); }
            let chested = facets.iter().find_map(|(property, value)| match (property, value) {
                (P::LlamaChested, V::Flag(value)) => Some(*value), _ => None,
            }).ok_or(ApplyFailure::InvalidAction)?;
            let strength = facets.iter().find_map(|(property, value)| match (property, value) {
                (P::LlamaStrength, V::Byte(value)) if (1..=5).contains(value) => Some(*value), _ => None,
            }).ok_or(ApplyFailure::InvalidAction)?;
            Ok(if chested { u16::from(strength) * 3 } else { 0 })
        }
        "piglin" | "villager" if facets.is_empty() => Ok(8),
        _ if facets.is_empty() => Ok(0),
        _ => Err(ApplyFailure::InvalidAction),
    }
}

 pub(super) fn entity_equipment_compatible(
    kind: u16,
    cells: &[(pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot, pumpkin_cluster::inventory::InventoryCell)],
) -> Result<(), ApplyFailure> {
    use pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot as Role;
    use pumpkin_data::data_component_impl::EquipmentSlot;
    let kind = pumpkin_data::entity::EntityType::from_raw(kind).ok_or(ApplyFailure::InvalidAction)?;
    let mut seen = std::collections::BTreeSet::new();
    for (role, cell) in cells {
        if !seen.insert(*role) { return Err(ApplyFailure::InvalidAction); }
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if stack.is_empty() || matches!(role, Role::MainHand | Role::OffHand) { continue; }
        let slot = match role {
            Role::Feet => EquipmentSlot::FEET,
            Role::Legs => EquipmentSlot::LEGS,
            Role::Chest => EquipmentSlot::CHEST,
            Role::Head => EquipmentSlot::HEAD,
            Role::Body => EquipmentSlot::BODY,
            Role::Saddle => EquipmentSlot::SADDLE,
            Role::MainHand | Role::OffHand => unreachable!(),
        };
        if !crate::entity::mob::equipment::is_equippable_in_slot(kind, &stack, &slot) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    Ok(())
}

pub(super) fn inventory_equipment_facts(
    cells: &[(
        pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot,
        pumpkin_cluster::inventory::InventoryCell,
    )],
) -> Result<pumpkin_cluster::combat_action::CombatEquipmentFacts, ApplyFailure> {
    use pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot;
    use pumpkin_data::data_component_impl::EquipmentSlot;
    let mut seen = std::collections::BTreeSet::new();
    let mut stacks = Vec::with_capacity(cells.len());
    for (slot, cell) in cells {
        if !seen.insert(*slot) {
            return Err(ApplyFailure::InvalidAction);
        }
        let slot = match slot {
            InventoryEquipmentSlot::MainHand => EquipmentSlot::MAIN_HAND,
            InventoryEquipmentSlot::OffHand => EquipmentSlot::OFF_HAND,
            InventoryEquipmentSlot::Feet => EquipmentSlot::FEET,
            InventoryEquipmentSlot::Legs => EquipmentSlot::LEGS,
            InventoryEquipmentSlot::Chest => EquipmentSlot::CHEST,
            InventoryEquipmentSlot::Head => EquipmentSlot::HEAD,
            InventoryEquipmentSlot::Body => EquipmentSlot::BODY,
            InventoryEquipmentSlot::Saddle => EquipmentSlot::SADDLE,
        };
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        stacks.push((slot, stack));
    }
    crate::entity::combat::normalized_equipment(stacks.iter().map(|(slot, stack)| (slot, stack)))
        .ok_or(ApplyFailure::InvalidAction)
}

pub(super) fn materialize_inventory_proposal<S: pumpkin_cluster::canonical_action::ActionState>(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    view: &S,
    proposal: &ActionProposal,
    tick: TickStamp,
) -> Result<Action, ApplyFailure> {
    let (intent, capture) = capture_inventory_proposal(original, state, proposal, tick)?;
    intent.capture_action(view, capture)
}

pub(super) fn capture_inventory_proposal(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    proposal: &ActionProposal,
    tick: TickStamp,
) -> Result<
    (
        pumpkin_cluster::canonical_inventory::InventoryGestureReplayIntent,
        pumpkin_cluster::canonical_inventory::InventoryActionCapture,
    ),
    ApplyFailure,
> {
    let ClaimedAction::Inventory(intent) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if proposal.id.actor != ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation
        || proposal.id.family != 2
        || proposal.screen_generation != intent.screen_session.map_or(0, |session| session.session)
        || !original.validate_live_player_lease(lease)
        || original.player_relation_participant(lease).is_none_or(|actor| actor.chunk != intent.actor_chunk)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let staged = LocalInventoryAction {
        open: None,
        lease: Some(lease),
        intent: pumpkin_cluster::inventory::InventoryMutationIntent {
            actor: proposal.id.actor,
            tick,
            payload: InventoryIntentPayload::Gestures(intent.gestures.clone()),
            screen_session: intent.screen_session,
            actor_chunk: intent.actor_chunk,
            affected: Vec::new(),
        },
        origin: None,
        input_sequence: Some(pumpkin_cluster::identity::PlayerSeq(proposal.id.sequence)),
    };
    let (mut intent, mut capture) = prepare_inventory_gesture_capture_in(
        state.server,
        original,
        &staged,
        tick,
        proposal.id,
    )
    .map_err(|reason| match reason {
        InventoryApplyError::TargetUnavailable => ApplyFailure::MissingStakeholder(
            pumpkin_cluster::canonical_action::Stakeholder::Chunk(intent.actor_chunk),
        ),
        InventoryApplyError::QueueFull => ApplyFailure::Capacity,
        _ => ApplyFailure::InvalidAction,
    })?;
    let entities = intent.bindings.iter().filter_map(|binding| match binding.target {
        Target::Chunk { key, part: ChunkPart::Entity(entity),
            inventory: Some(InventoryId::Entity { inv, .. }) }
            if inv == pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT || inv == pumpkin_cluster::inventory::INV_MAIN => Some((entity, key)),
        _ => None,
    }).collect::<BTreeSet<_>>();
    for (entity, chunk) in entities {
        use pumpkin_cluster::canonical_action::ActionState;
        let living = Target::Chunk { key: chunk, part: ChunkPart::Entity(entity), inventory: None };
        let Some(pumpkin_cluster::canonical_action::Cell {
            value: pumpkin_cluster::canonical_action::Value::Entity(body), ..
        }) = original.read(TruthView::Local, living) else {
            return Err(ApplyFailure::MissingStakeholder(living.stakeholder()));
        };
        let pumpkin_cluster::entity_body::EntityPayload::Living(_) = &body.payload else {
            continue;
        };
        for loc in original.entity_inventory_layout(&body)? {
            let target = Target::Chunk { key: chunk, part: ChunkPart::Entity(entity), inventory: Some(InventoryId::Entity { inv: loc.inv, slot: loc.slot }) };
            if !intent.bindings.iter().any(|binding| binding.target == target) {
                intent.bindings.push(InventorySlotBinding { slot: InventorySlotRef::new(
                    InventoryTarget::Entity(pumpkin_cluster::inventory::EntityInventoryId { origin: entity.origin, local_id: entity.local_id, generation: entity.generation }), loc),
                    target, max_stack: if loc.inv == pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT && loc.slot >= 2 { 1 } else { pumpkin_cluster::inventory::INV_MAX_STACK } });
            }
        }
        intent.entity_equipment.push(entity_equipment_binding(original, &body, chunk, &intent.bindings)?.ok_or(ApplyFailure::InvalidAction)?);
        capture.guards.push(pumpkin_cluster::canonical_action::Condition {
            target: living, expected: pumpkin_cluster::canonical_action::Expected::EntityBirthKind {
                birth: body.birth, owner: body.owner, kind: body.kind,
            },
        });
    }
    capture.client_origin = proposal.client_origin;
    let foreign = intent.bindings.iter().filter_map(|binding| match binding.target {
        Target::Player { lease: Some(target), inventory: Some(_), .. } if target != lease => Some(target),
        _ => None,
    }).collect::<BTreeSet<_>>();
    if !foreign.is_empty() {
        if foreign.len() != 1 { return Err(ApplyFailure::InvalidAction); }
        let target = *foreign.first().ok_or(ApplyFailure::InvalidAction)?;
        let session = staged.intent.screen_session.ok_or(ApplyFailure::InvalidAction)?;
        let screen = original.inventory_source_screen(session)
            .filter(|screen| screen.preview_lease == Some(target) && original.inventory_source_preview_trusted(screen))
            .ok_or(ApplyFailure::InvalidAction)?;
        let policy = original.inventory_source_permission()
            .ok_or(ApplyFailure::Capacity)?;
        let permission = super::cluster_permissions::capture_permission_for_action(
            lease, policy.issuer, tick, "minecraft:command.invsee.edit", policy,
        ).filter(|permission| permission.allowed).ok_or(ApplyFailure::InvalidAction)?;
        capture.edit_authority = Some(pumpkin_cluster::inventory_edit_authority::InventoryEditAuthority {
            issuer: permission.issuer, viewer: lease, target, tick,
            policy: pumpkin_cluster::inventory_edit_authority::InventoryEditPolicy::Privileged(
                pumpkin_cluster::inventory_edit_authority::InventoryPermissionWitness {
                    issuer: permission.issuer, viewer: permission.viewer, tick: permission.tick,
                    policy_revision: permission.policy_revision, decision_version: permission.decision_version,
                    node: permission.node, allowed: permission.allowed,
                }),
            screen_session: session.session,
            screen_install_generation: original.player_install_generation(lease)
                .filter(|generation| *generation != 0).ok_or(ApplyFailure::InvalidAction)?,
            preview_provider: screen.preview_authority.map(|authority| authority.provider),
            bindings: intent.bindings.clone(),
        });
    }
    Ok((intent, capture))
}

pub(super) fn capture_inventory_view_authority(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &ActionProposal,
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    target: LivePlayerLease,
) -> Result<pumpkin_cluster::inventory_edit_authority::InventoryEditAuthority, ApplyFailure> {
    let viewer = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if proposal.tick != original.original_tick() || proposal.id.actor != ActorId::Player(viewer.gid)
        || proposal.id.incarnation != viewer.incarnation || viewer == target
        || proposal.client_origin != pumpkin_cluster::canonical_action::ClientOrigin::None
        || screen.session.actor != viewer.gid || screen.preview_lease != Some(target)
        || !original.inventory_source_preview_trusted(screen) {
        return Err(ApplyFailure::InvalidAction);
    }
    let policy = original.inventory_source_permission().ok_or(ApplyFailure::Capacity)?;
    let permission = super::cluster_permissions::capture_permission_for_action(viewer, policy.issuer,
        proposal.tick, "minecraft:command.invsee", policy).filter(|permission| permission.allowed).ok_or(ApplyFailure::InvalidAction)?;
    Ok(pumpkin_cluster::inventory_edit_authority::InventoryEditAuthority {
        issuer: permission.issuer, viewer, target, tick: proposal.tick,
        policy: pumpkin_cluster::inventory_edit_authority::InventoryEditPolicy::PrivilegedView(
            pumpkin_cluster::inventory_edit_authority::InventoryPermissionWitness {
                issuer: permission.issuer, viewer: permission.viewer, tick: permission.tick,
                policy_revision: permission.policy_revision, decision_version: permission.decision_version,
                node: permission.node, allowed: permission.allowed,
            }),
        screen_session: screen.session.session,
        screen_install_generation: original.player_install_generation(viewer).filter(|generation| *generation != 0).ok_or(ApplyFailure::InvalidAction)?,
        preview_provider: screen.preview_authority.as_ref().map(|authority| authority.provider),
        bindings: Vec::new(),
    })
}

pub(super) fn reject_inventory_action(server: &Server, owner: &InventoryOwner, action: &Action) {
    let gestures = match &action.semantic_intent {
        Some(pumpkin_cluster::canonical_action::SemanticIntent::InventoryGestures(intent)) => {
            intent.gestures.clone()
        }
        Some(pumpkin_cluster::canonical_action::SemanticIntent::CreativeInventory(intent)) => {
            intent
                .bindings
                .iter()
                .map(
                    |binding| pumpkin_cluster::inventory::InventoryGesture::Expect {
                        slot: binding.slot,
                        cell: pumpkin_cluster::inventory::InventoryCell::empty(binding.slot.loc),
                    },
                )
                .collect()
        }
        Some(pumpkin_cluster::canonical_action::SemanticIntent::RecipeInventory(intent)) => intent
            .bindings()
            .iter()
            .map(
                |binding| pumpkin_cluster::inventory::InventoryGesture::Expect {
                    slot: binding.slot,
                    cell: pumpkin_cluster::inventory::InventoryCell::empty(binding.slot.loc),
                },
            )
            .collect(),
        _ => return,
    };
    let Some(actor_chunk) = action.actor_chunk else {
        return;
    };
    let session =
        super::cluster_inventory_protocol::canonical_inventory_session(action.client_origin)
            .and_then(|session| match action.id.actor {
                ActorId::Player(actor) => {
                    Some(pumpkin_cluster::inventory::InventoryScreenSession { actor, session })
                }
                _ => None,
            });
    reject_inventory_proposal(
        server,
        owner,
        &ActionProposal {
            command: None,
            id: action.id,
            tick: action.tick,
            lease: None,
            client_origin: action.client_origin,
            screen_generation: session.map_or(0, |session| session.session),
            claimed_targets: Vec::new(),
            claim: ClaimedAction::Inventory(
                pumpkin_cluster::canonical_inventory::InventoryGestureProposal {
                    gestures,
                    screen_session: session,
                    actor_chunk,
                },
            ),
        },
        action.tick,
    );
}

pub(super) fn reject_inventory_proposal(
    server: &Server,
    owner: &InventoryOwner,
    proposal: &ActionProposal,
    tick: TickStamp,
) {
    let (mut targets, session) = match &proposal.claim {
        ClaimedAction::Inventory(intent) => (
            intent
                .gestures
                .iter()
                .flat_map(|gesture| gesture.targets())
                .collect::<Vec<_>>(),
            super::cluster_inventory_protocol::canonical_inventory_session(proposal.client_origin),
        ),
        ClaimedAction::InventoryMenu(intent) => (
            super::cluster_invsee::inventory_screen_snapshot(server, intent.session)
                .map(|screen| pumpkin_cluster::inventory::inventory_screen_targets(&screen))
                .unwrap_or_else(|| vec![InventoryTarget::Player(intent.session.actor)]),
            Some(intent.session.session),
        ),
        _ => return,
    };
    targets.sort_unstable();
    targets.dedup();
    let authoritative = targets
        .iter()
        .filter_map(|target| owner.phase_snapshot(TruthView::Local, *target))
        .collect();
    if let ActorId::Player(gid) = proposal.id.actor {
        let _ = super::cluster_inventory_protocol::publish_authoritative_inventory_correction(
            server, gid, session, &targets,
        );
    }
    let _ =
        server
            .cluster_inventory_publication_tx
            .send(super::cluster_invsee::InventoryPublication {
                resolution: pumpkin_cluster::inventory::InventoryResolution {
                    action: pumpkin_cluster::completion::ActionKey::canonical_id(proposal.id),
                    tick,
                    result: Err(InventoryApplyError::Rejected),
                    authoritative,
                    correction: pumpkin_cluster::inventory::InventoryDelta {
                        revisions: Vec::new(),
                        operations: Vec::new(),
                    },
                    effects: Vec::new(),
                    menu_effects: Vec::new(),
                },
                origin: super::cluster_inventory_protocol::canonical_inventory_origin(
                    proposal.client_origin,
                ),
                session,
            });
}

pub(super) fn prepare_inventory_gesture_capture(
    server: &Server,
    owner: &InventoryOwner,
    action: &LocalInventoryAction,
    tick: TickStamp,
    id: ActionId,
) -> Result<(InventoryGestureReplayIntent, InventoryActionCapture), InventoryApplyError> {
    prepare_inventory_gesture_capture_in(server, owner, action, tick, id)
}

fn prepare_inventory_gesture_capture_in(
    server: &Server,
    owner: &impl InventoryCaptureSource,
    action: &LocalInventoryAction,
    tick: TickStamp,
    id: ActionId,
) -> Result<(InventoryGestureReplayIntent, InventoryActionCapture), InventoryApplyError> {
    let InventoryIntentPayload::Gestures(gestures) = &action.intent.payload else {
        return Err(InventoryApplyError::Rejected);
    };
    let renames = gestures
        .iter()
        .flat_map(|gesture| gesture.slots())
        .filter_map(|slot| {
            owner
                .current_inventory_address(slot.target)
                .filter(|target| *target != slot.target)
                .map(|target| (slot.target, target))
        })
        .collect::<Vec<_>>();
    let mut gestures = gestures.clone();
    for gesture in &mut gestures {
        gesture.rename_targets(&renames);
    }
    let screen = action
        .intent
        .screen_session
        .and_then(|session| {
            (ActorId::Player(session.actor) == action.intent.actor).then_some(session)
        })
        .and_then(|session| owner.source_screen(server, session));
    if action.intent.screen_session.is_some() && screen.is_none() {
        return Err(InventoryApplyError::TargetUnavailable);
    }
    let preview = screen
        .as_ref()
        .filter(|screen| owner.trusted_preview(server, screen));
    let lease_for = |gid| {
        owner
            .player_lease_for_gid(gid)
            .filter(|lease| owner.live_player_lease_matches(*lease))
            .or_else(|| {
                preview
                    .and_then(|screen| screen.preview_lease)
                    .filter(|lease| lease.gid == gid)
            })
    };
    let chunks_for = |target| {
        owner.local_target_chunks(target).or_else(|| {
            let screen = preview?;
            let gid = match target {
                InventoryTarget::Player(gid)
                | InventoryTarget::LeasedPlayer { player: gid, .. } => gid,
                _ => return None,
            };
            (screen.preview_lease?.gid == gid).then(|| screen.affected.clone())
        })
    };
    let mut slots = gestures
        .iter()
        .flat_map(|gesture| gesture.slots())
        .collect::<BTreeSet<_>>();
    let equipment_players = slots
        .iter()
        .filter_map(|slot| {
            let gid = match slot.target {
                InventoryTarget::Player(gid)
                | InventoryTarget::LeasedPlayer { player: gid, .. } => gid,
                _ => return None,
            };
            (slot.loc.inv == pumpkin_cluster::inventory::INV_ARMOR
                || slot.loc.inv == pumpkin_cluster::inventory::INV_OFFHAND
                || slot.loc.inv == pumpkin_cluster::inventory::INV_SELECTED
                || slot.loc.inv == pumpkin_cluster::inventory::INV_MAIN && slot.loc.slot < 9)
                .then_some(gid)
        })
        .collect::<BTreeSet<_>>();
    let mut equipment = Vec::new();
    for gid in equipment_players {
        let lease = lease_for(gid).ok_or(InventoryApplyError::TargetUnavailable)?;
        let chunks = chunks_for(InventoryTarget::Player(gid))
            .ok_or(InventoryApplyError::TargetUnavailable)?;
        if chunks.len() != 1 {
            return Err(InventoryApplyError::Rejected);
        }
        let target = |inv, slot| Target::Player {
            key: lease.key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv, slot }),
        };
        let binding = InventoryEquipmentBinding {
            lease,
            living: Target::Chunk {
                key: chunks[0],
                part: ChunkPart::Player(gid),
                inventory: None,
            },
            selected: target(pumpkin_cluster::inventory::INV_SELECTED, 0),
            hotbar: std::array::from_fn(|slot| {
                target(pumpkin_cluster::inventory::INV_MAIN, slot as u16)
            }),
            armor: std::array::from_fn(|slot| {
                target(pumpkin_cluster::inventory::INV_ARMOR, slot as u16)
            }),
            offhand: target(pumpkin_cluster::inventory::INV_OFFHAND, 0),
            attribute_ids: pumpkin_data::attributes::Attributes::ALL
                .iter()
                .map(|attribute| u16::from(attribute.id))
                .collect(),
        };
        for (inv, count) in [
            (pumpkin_cluster::inventory::INV_MAIN, 9),
            (pumpkin_cluster::inventory::INV_ARMOR, 4),
            (pumpkin_cluster::inventory::INV_OFFHAND, 1),
            (pumpkin_cluster::inventory::INV_SELECTED, 1),
        ] {
            for slot in 0..count {
                slots.insert(InventorySlotRef::new(
                    owner
                        .current_inventory_address(InventoryTarget::Player(gid))
                        .unwrap_or(InventoryTarget::Player(gid)),
                    pumpkin_cluster::inventory::InvLoc::new(inv, slot),
                ));
            }
        }
        equipment.push(binding);
    }
    let mut bindings = Vec::with_capacity(slots.len());
    let mut leases = BTreeMap::new();
    let mut player_chunks = BTreeMap::<PlayerKey, ChunkKey>::new();
    if let ActorId::Player(actor) = action.intent.actor {
        let lease = owner
            .player_lease_for_gid(actor)
            .filter(|lease| owner.live_player_lease_matches(*lease))
            .ok_or(InventoryApplyError::TargetUnavailable)?;
        if id.incarnation != lease.incarnation {
            return Err(InventoryApplyError::Rejected);
        }
        leases.insert(lease.key, lease);
        player_chunks.insert(lease.key, action.intent.actor_chunk);
    }
    let mut validated = BTreeSet::new();
    for slot in slots {
        let target =
            binding_target_in(owner, preview, slot).ok_or(InventoryApplyError::TargetUnavailable)?;
        if validated.insert(slot.target) {
            let snapshot = owner
                .phase_snapshot(TruthView::Local, slot.target)
                .or_else(|| {
                    preview
                        .and_then(|screen| screen.preview.clone())
                        .filter(|snapshot| snapshot.target == slot.target)
                })
                .ok_or(InventoryApplyError::TargetUnavailable)?;
            super::cluster_invsee::validate_imported_inventory_snapshots(&[snapshot])?;
        }
        let max_stack = screen
            .as_ref()
            .and_then(|screen| {
                screen
                    .slots
                    .iter()
                    .find(|entry| entry.slot == slot)
                    .map(|entry| entry.limit)
            })
            .unwrap_or(pumpkin_cluster::inventory::INV_MAX_STACK);
        if max_stack == 0 {
            return Err(InventoryApplyError::Rejected);
        }
        if let Target::Player {
            lease: Some(lease), ..
        } = target
        {
            leases.insert(lease.key, lease);
            let chunks = chunks_for(slot.target).ok_or(InventoryApplyError::TargetUnavailable)?;
            if chunks.len() != 1
                || player_chunks
                    .insert(lease.key, chunks[0])
                    .is_some_and(|prior| prior != chunks[0])
            {
                return Err(InventoryApplyError::Rejected);
            }
        }
        bindings.push(InventorySlotBinding {
            slot,
            target,
            max_stack,
        });
    }
    let intent = InventoryGestureReplayIntent {
        entity_equipment: Vec::new(),
        gestures: gestures.clone(),
        bindings,
        equipment,
    };
    let mode = Target::Chunk {
        key: action.intent.actor_chunk,
        part: ChunkPart::LivingProperty {
            actor: action.intent.actor,
            property: pumpkin_cluster::living_property::LivingProperty::GameMode,
        },
        inventory: None,
    };
    if !intent.is_valid()
        && pumpkin_cluster::creative_inventory::CreativeInventoryIntent::from_gestures(
            &intent, mode,
        )
        .is_none()
    {
        return Err(InventoryApplyError::Rejected);
    }
    Ok((
        intent,
        InventoryActionCapture {
            edit_authority: None,
            id,
            tick,
            actor_chunk: Some(action.intent.actor_chunk),
            player_leases: leases.into_values().collect(),
            player_chunks: player_chunks.into_iter().collect(),
            guards: Vec::new(),
            client_origin: super::cluster_inventory_protocol::inventory_client_origin(
                action.origin,
                action.intent.screen_session,
            ),
        },
    ))
}
