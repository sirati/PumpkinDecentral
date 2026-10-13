use super::cluster_world_callbacks::{
    CallbackCapture, CallbackCaptureError, append_resolution_light, build_callback_action,
    capture_effects, capture_normal_use,
};
use pumpkin_cluster::canonical_action::{ChunkPart, Target, TruthView, Value};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_data::BlockStateId;
use std::collections::BTreeMap;

pub(super) struct WorldUseContext {
    pub proposal: pumpkin_cluster::canonical_action::ActionProposal,
    pub tick: pumpkin_cluster::time::TickStamp,
    pub world: std::sync::Arc<crate::world::World>,
    pub world_id: pumpkin_cluster::protocol::WorldId,
    pub actor: pumpkin_cluster::canonical_action::Cell,
    pub actor_target: Target,
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub position: pumpkin_util::math::position::BlockPos,
    pub can_modify_world: bool,
    pub sneaking: bool,
    pub block_use_allowed: bool,
    pub effective: pumpkin_cluster::controller::EffectiveControllerState,
    pub facing: pumpkin_data::block_properties::HorizontalFacing,
    pub producer: pumpkin_cluster::realm_completion::PeerGeneration,
    pub source: pumpkin_cluster::canonical_inventory::InventorySlotBinding,
    pub source_cell: pumpkin_cluster::canonical_action::Cell,
    pub policy_facts: Vec<(Target, pumpkin_cluster::canonical_action::Cell)>,
    pub menu: Option<FrozenBlockMenuOpen>,
}
pub(super) struct FrozenBlockMenuOpen {
    pub intent: pumpkin_cluster::canonical_menu::MenuSessionIntent,
    pub statistic: u32,
    pub presentation: Option<super::cluster_invsee::CanonicalInventoryOpenPresentation>,
    pub permit:
        Option<tokio::sync::mpsc::OwnedPermit<super::cluster_invsee::LocalInventoryLifecycle>>,
}

pub(super) fn prepare_block_menu<S: pumpkin_cluster::canonical_action::ActionState>(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &S,
    inventory: &super::cluster_world_inventory::InventoryOwner,
    context: &mut WorldUseContext,
    full_source_slot: u64,
) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure, ClaimedAction};
    let target = Target::Chunk {
        key: ChunkKey::new(
            context.world_id,
            context.position.0.x.div_euclid(16),
            context.position.0.z.div_euclid(16),
        ),
        part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
            x: context.position.0.x,
            y: context.position.0.y,
            z: context.position.0.z,
        }),
        inventory: None,
    };
    let cell = view
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?;
    let Value::Block(state) = cell.value else {
        return Err(ApplyFailure::Type(target));
    };
    let state = BlockStateId::new(state).ok_or(ApplyFailure::Type(target))?;
    let statistic = match state.to_block().name {
        "hopper" => pumpkin_data::statistic::CustomStatistic::InspectHopper,
        "crafting_table" => pumpkin_data::statistic::CustomStatistic::InteractWithCraftingTable,
        "loom" => pumpkin_data::statistic::CustomStatistic::InteractWithLoom,
        "smithing_table" => pumpkin_data::statistic::CustomStatistic::InteractWithSmithingTable,
        "stonecutter" => pumpkin_data::statistic::CustomStatistic::InteractWithStonecutter,
        "cartography_table" => {
            pumpkin_data::statistic::CustomStatistic::InteractWithCartographyTable
        }
        _ => return Ok(()),
    } as u32;
    let ClaimedAction::ItemUse(claim) = &context.proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    if !context.block_use_allowed {
        return Ok(());
    }
    let viewer = super::cluster_inventory_menu::capture_menu_viewer(
        original,
        context.lease,
        claim.intent.actor_chunk,
        context.tick,
    )?;
    let reference = ActionRef {
        id: context.proposal.id,
        tick: context.tick,
    };
    let identity = pumpkin_cluster::canonical_menu_identity::MenuPresentationIdentity::from_source(
        reference,
        full_source_slot,
    )
    .ok_or(ApplyFailure::InvalidAction)?;
    let Target::Chunk {
        key,
        part: ChunkPart::Block(position),
        ..
    } = target
    else {
        unreachable!()
    };
    let (screen, presentation, intent) = if state.to_block() == &pumpkin_data::Block::HOPPER {
        let (viewer_inventory, descriptor) = super::cluster_inventory_menu::capture_block_menu(
            original,
            view,
            context.lease,
            key,
            position,
            state,
        )
        .map_err(|_| ApplyFailure::Condition(target))?;
        super::cluster_inventory_menu::block_menu_open(
            context.lease,
            identity.session,
            identity.sync_id,
            viewer,
            &viewer_inventory,
            descriptor,
        )
    } else {
        let inventory_target = pumpkin_cluster::inventory::InventoryTarget::LeasedPlayer {
            key: context.lease.key.0,
            player: context.lease.gid,
            incarnation: context.lease.incarnation,
        };
        let mut viewer_inventory = inventory
            .phase_snapshot(TruthView::Local, inventory_target)
            .ok_or(ApplyFailure::InvalidAction)?;
        for cell in &mut viewer_inventory.cells {
            let slot =
                pumpkin_cluster::inventory::InventorySlotRef::new(inventory_target, cell.loc);
            let binding =
                super::cluster_world_inventory_actions::binding_target(inventory, None, slot)
                    .ok_or(ApplyFailure::InvalidAction)?;
            let Some(pumpkin_cluster::canonical_action::Cell {
                value: Value::Inventory(original),
                ..
            }) = view.read(TruthView::Local, binding)
            else {
                return Err(ApplyFailure::Inventory(binding));
            };
            *cell = original;
        }
        super::cluster_inventory_menu::virtual_block_menu_open(
            context.lease,
            identity.session,
            identity.sync_id,
            viewer,
            pumpkin_cluster::canonical_menu::MenuBlockContact {
                chunk: key,
                position,
                state: state.as_u16(),
            },
            &viewer_inventory,
        )
    }
    .map_err(|_| ApplyFailure::InvalidAction)?;
    let pumpkin_cluster::canonical_menu::MenuSessionOperation::Open { state, .. } =
        &intent.operation
    else {
        unreachable!()
    };
    let source = state.source.clone();
    context.menu = Some(FrozenBlockMenuOpen {
        intent,
        statistic,
        presentation: Some(super::cluster_invsee::CanonicalInventoryOpenPresentation {
            reference,
            lease: context.lease,
            screen,
            source,
            menu: None,
            merchant: None,
            presentation: Some(presentation),
        }),
        permit: None,
    });
    Ok(())
}

fn capture_block_use_policy<S: pumpkin_cluster::canonical_action::ActionState>(
    view: &S,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    sneaking: bool,
    facts: &mut Vec<(Target, pumpkin_cluster::canonical_action::Cell)>,
) -> Result<bool, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, InventoryId};
    if !sneaking {
        return Ok(true);
    }
    let player_slot = |inv, slot| Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(InventoryId::Player { inv, slot }),
    };
    let selector = player_slot(pumpkin_cluster::inventory::INV_SELECTED, 0);
    let selected = view
        .read(TruthView::Local, selector)
        .ok_or(ApplyFailure::Inventory(selector))?;
    let Value::Inventory(selected_item) = &selected.value else {
        return Err(ApplyFailure::Inventory(selector));
    };
    if selected_item.count != 1
        || !selected_item.nbt.is_empty()
        || !(1..=9).contains(&selected_item.item)
    {
        return Err(ApplyFailure::Inventory(selector));
    }
    let main = player_slot(pumpkin_cluster::inventory::INV_MAIN, selected_item.item - 1);
    let offhand = player_slot(pumpkin_cluster::inventory::INV_OFFHAND, 0);
    let mut empty = true;
    facts.push((selector, selected));
    for target in [main, offhand] {
        let cell = view
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Inventory(target))?;
        let Value::Inventory(item) = &cell.value else {
            return Err(ApplyFailure::Inventory(target));
        };
        empty &= item.count == 0;
        facts.push((target, cell));
    }
    Ok(empty)
}

pub(super) fn normalize_use_on_block<S: pumpkin_cluster::canonical_action::ActionState>(
    server: &std::sync::Arc<super::Server>,
    view: &S,
    _inventory: &super::cluster_world_inventory::InventoryOwner,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<WorldUseContext, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure, ChunkPart, ClaimedAction};
    use pumpkin_cluster::item_action::ItemActionKind;
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let ItemActionKind::UseOnBlock { world, block, .. } = claim.intent.kind else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if !pumpkin_cluster::canonical_action::ActionState::validate_live_player_lease(original, lease)
        || claim.intent.actor_chunk.world != world
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let actor_target = Target::Chunk {
        key: claim.intent.actor_chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let actor = view
        .read(TruthView::Local, actor_target)
        .ok_or(ApplyFailure::Condition(actor_target))?;
    let Value::Living(living) = &actor.value else {
        return Err(ApplyFailure::Type(actor_target));
    };
    let avatar = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(actor_target))?;
    if living.combat.health_milli == 0 || avatar.game_mode == 3 {
        return Err(ApplyFailure::InvalidAction);
    }
    let reach_id = u16::from(pumpkin_data::attributes::Attributes::BLOCK_INTERACTION_RANGE.id);
    let reach_base = living
        .combat
        .effective_attributes
        .get(&reach_id)
        .copied()
        .map(f64::from_bits)
        .ok_or(ApplyFailure::InvalidAction)?;
    let reach = living.attribute(reach_id, reach_base);
    let projection = original
        .player_projection(lease.gid)
        .filter(|spatial| spatial.chunk == claim.intent.actor_chunk)
        .ok_or(ApplyFailure::Condition(actor_target))?;
    let effective = projection.frame.effective;
    let mut pose = super::cluster_living_metadata::player_pose(living);
    if projection.gliding {
        pose = pumpkin_data::entity::EntityPose::FallFlying;
    } else if projection.crawling || projection.swimming {
        pose = pumpkin_data::entity::EntityPose::Swimming;
    } else if projection.sneaking {
        pose = pumpkin_data::entity::EntityPose::Crouching;
    }
    let eye_height =
        f64::from(super::cluster_living_metadata::player_dimensions(living, pose).eye_height);
    let distance: f64 = [block.x, block.y, block.z]
        .into_iter()
        .enumerate()
        .map(|(index, coordinate)| {
            let position = effective.position[index] + if index == 1 { eye_height } else { 0.0 };
            let difference =
                position - position.clamp(f64::from(coordinate), f64::from(coordinate) + 1.0);
            difference * difference
        })
        .sum();
    if !reach.is_finite() || reach < 0.0 || !distance.is_finite() || distance > reach * reach {
        return Err(ApplyFailure::InvalidAction);
    }
    let worlds = server.worlds.load_full();
    let runtime_world = worlds
        .iter()
        .find(|candidate| {
            candidate.level.cluster_world_key == world.key
                && candidate.level.cluster_dimension == world.dimension
        })
        .ok_or(ApplyFailure::InvalidAction)?;
    let position = pumpkin_util::math::position::BlockPos(
        pumpkin_util::math::vector3::Vector3::new(block.x, block.y, block.z),
    );
    let sneaking = effective.input_flags
        & u64::from(pumpkin_protocol::java::server::play::SPlayerInput::SNEAK as u8)
        != 0;
    let facing = match ((effective.yaw / 90.0 + 0.5).floor() as i32).rem_euclid(4) {
        0 => pumpkin_data::block_properties::HorizontalFacing::South,
        1 => pumpkin_data::block_properties::HorizontalFacing::West,
        2 => pumpkin_data::block_properties::HorizontalFacing::North,
        _ => pumpkin_data::block_properties::HorizontalFacing::East,
    };
    let producer = pumpkin_cluster::realm_completion::PeerGeneration {
        server: lease.gid.server,
        generation: original
            .peer_incarnations()
            .get(&lease.gid.server)
            .copied()
            .ok_or(ApplyFailure::InvalidAction)?,
    };
    let can_modify_world = avatar.game_mode != 2;
    let (_, source, source_cell, _, _) =
        super::cluster_item_use::normalize_source(view, original, proposal)?;
    let mut policy_facts = Vec::new();
    let game_mode = Target::Chunk {
        key: claim.intent.actor_chunk,
        part: ChunkPart::LivingProperty {
            actor: proposal.id.actor,
            property: pumpkin_cluster::living_property::LivingProperty::GameMode,
        },
        inventory: None,
    };
    policy_facts.push((
        game_mode,
        view.read(TruthView::Local, game_mode)
            .ok_or(ApplyFailure::Condition(game_mode))?,
    ));
    for attribute in [
        reach_id,
        u16::from(pumpkin_data::attributes::Attributes::LUCK.id),
    ] {
        let target = Target::Chunk {
            key: claim.intent.actor_chunk,
            part: ChunkPart::LivingProperty {
                actor: proposal.id.actor,
                property: pumpkin_cluster::living_property::LivingProperty::Attribute(attribute),
            },
            inventory: None,
        };
        policy_facts.push((
            target,
            view.read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?,
        ));
    }
    let block_use_allowed = capture_block_use_policy(view, lease, sneaking, &mut policy_facts)?;
    Ok(WorldUseContext {
        proposal: proposal.clone(),
        tick,
        world: runtime_world.clone(),
        world_id: world,
        actor,
        actor_target,
        lease,
        position,
        can_modify_world,
        sneaking,
        block_use_allowed,
        effective,
        facing,
        producer,
        source,
        source_cell,
        policy_facts,
        menu: None,
    })
}

pub(super) fn materialize_use_on_block<S: pumpkin_cluster::canonical_action::ActionState>(
    server: &std::sync::Arc<super::Server>,
    view: &S,
    inventory: &super::cluster_world_inventory::InventoryOwner,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<
    pumpkin_cluster::canonical_action::Action,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    let context = normalize_use_on_block(server, view, inventory, original, proposal, tick)?;
    let mut snapshot = crate::block::neighbor::NeighborSnapshot::default();
    let mut facts = BTreeMap::new();
    for _ in 0..4096 {
        let (_, capture) = capture_use_context(view, &context, &mut snapshot, &mut facts)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        match capture {
            CallbackCapture::Ready(resolution) => {
                return complete_use_context(view, &context, &mut facts, &resolution);
            }
            CallbackCapture::Unsupported => return Err(ApplyFailure::InvalidAction),
            CallbackCapture::Policy(requests) => {
                for request in requests {
                    if let crate::block::neighbor::CallbackQuery::Physics(position, source) =
                        request.query
                    {
                        if server
                            .plugin_manager
                            .has_handlers::<crate::plugin::api::events::block::BlockPhysicsEvent>()
                        {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        snapshot.physics_allowed.insert((position, source), true);
                    } else {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
            }
        }
    }
    Err(ApplyFailure::InvalidAction)
}

fn callback_block_target(
    world: pumpkin_cluster::protocol::WorldId,
    position: pumpkin_util::math::position::BlockPos,
) -> Target {
    Target::Chunk {
        key: ChunkKey::new(
            world,
            position.0.x.div_euclid(16),
            position.0.z.div_euclid(16),
        ),
        part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        }),
        inventory: None,
    }
}

pub(super) fn capture_use_context<S: pumpkin_cluster::canonical_action::ActionState>(
    view: &S,
    context: &WorldUseContext,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
) -> Result<(crate::block::registry::BlockActionResult, CallbackCapture), CallbackCaptureError> {
    if !context.block_use_allowed {
        // Item-only fallback is selected by its typed source, never by calling a
        // legacy block interaction while sneaking with either hand occupied.
        return Ok((
            crate::block::registry::BlockActionResult::Pass,
            CallbackCapture::Unsupported,
        ));
    }
    if context.menu.is_some() {
        return Ok((
            crate::block::registry::BlockActionResult::Success,
            CallbackCapture::Ready(Default::default()),
        ));
    }
    if let Value::Inventory(item) = &context.source_cell.value
        && item.item == pumpkin_data::item::Item::GLOWSTONE.id
    {
        let target = Target::Chunk {
            key: ChunkKey::new(
                context.world_id,
                context.position.0.x.div_euclid(16),
                context.position.0.z.div_euclid(16),
            ),
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
                x: context.position.0.x,
                y: context.position.0.y,
                z: context.position.0.z,
            }),
            inventory: None,
        };
        let cell = view
            .read(TruthView::Local, target)
            .ok_or(CallbackCaptureError::InvalidWrite(target))?;
        if let Value::Block(id) = cell.value
            && let Some(state) = BlockStateId::new(id)
            && state.to_block() == &pumpkin_data::Block::RESPAWN_ANCHOR
        {
            let mut props =
                pumpkin_data::block_properties::RespawnAnchorLikeProperties::from_state_id(state);
            if props.charges < 4 {
                facts.insert(target, Some(cell));
                snapshot.blocks.insert(context.position, state);
                props.charges += 1;
                let effects = vec![
                    crate::block::neighbor::BlockCallbackEffect::SetBlock {
                        position: context.position,
                        state: props.to_state_id(state.to_block()),
                        flags: pumpkin_world::world::BlockFlags::NOTIFY_ALL,
                    },
                    crate::block::neighbor::BlockCallbackEffect::Sound {
                        position: context.position,
                        sound: pumpkin_data::sound::Sound::BlockRespawnAnchorCharge,
                        volume_milli: 1000,
                        pitch_milli: 1000,
                    },
                ];
                return capture_effects(
                    view,
                    context.world_id,
                    &context.world.block_registry,
                    snapshot,
                    facts,
                    &effects,
                    4096,
                )
                .map(|capture| (crate::block::registry::BlockActionResult::Success, capture));
            }
        }
    }
    if let Value::Inventory(item) = &context.source_cell.value
        && item.item == pumpkin_data::item::Item::ENDER_EYE.id
    {
        let target = callback_block_target(context.world_id, context.position);
        let cell = view
            .read(TruthView::Local, target)
            .ok_or(CallbackCaptureError::InvalidWrite(target))?;
        if let Value::Block(id) = cell.value
            && let Some(state) = BlockStateId::new(id)
            && state.to_block() == &pumpkin_data::Block::END_PORTAL_FRAME
        {
            let mut properties =
                pumpkin_data::block_properties::EndPortalFrameLikeProperties::from_state_id(state);
            if !properties.eye {
                properties.eye = true;
                let filled = properties.to_state_id(state.to_block());
                facts.insert(target, Some(cell));
                snapshot.blocks.insert(context.position, state);
                let interior = super::cluster_world_end_portal::completed_interior(
                    context.position,
                    |position| {
                        if position == context.position {
                            return Ok(filled);
                        }
                        let target = callback_block_target(context.world_id, position);
                        let cell = view
                            .read(TruthView::Local, target)
                            .ok_or(CallbackCaptureError::InvalidWrite(target))?;
                        let Value::Block(id) = cell.value else {
                            return Err(CallbackCaptureError::InvalidWrite(target));
                        };
                        let state = BlockStateId::new(id)
                            .ok_or(CallbackCaptureError::InvalidWrite(target))?;
                        facts.insert(target, Some(cell));
                        snapshot.blocks.insert(position, state);
                        Ok(state)
                    },
                )?;
                let mut effects = vec![
                    crate::block::neighbor::BlockCallbackEffect::SetBlock {
                        position: context.position,
                        state: filled,
                        flags: pumpkin_world::world::BlockFlags::NOTIFY_LISTENERS,
                    },
                    crate::block::neighbor::BlockCallbackEffect::Sound {
                        position: context.position,
                        sound: pumpkin_data::sound::Sound::BlockEndPortalFrameFill,
                        volume_milli: 1000,
                        pitch_milli: 1000,
                    },
                ];
                if let Some(interior) = interior {
                    for position in interior {
                        effects.push(crate::block::neighbor::BlockCallbackEffect::SetBlock {
                            position,
                            state: pumpkin_data::Block::END_PORTAL.default_state.id,
                            flags: pumpkin_world::world::BlockFlags::NOTIFY_LISTENERS,
                        });
                    }
                    effects.push(crate::block::neighbor::BlockCallbackEffect::Sound {
                        position: context.position,
                        sound: pumpkin_data::sound::Sound::BlockEndPortalSpawn,
                        volume_milli: 1000,
                        pitch_milli: 1000,
                    });
                }
                return capture_effects(
                    view,
                    context.world_id,
                    &context.world.block_registry,
                    snapshot,
                    facts,
                    &effects,
                    4096,
                )
                .map(|capture| (crate::block::registry::BlockActionResult::Success, capture));
            }
        }
    }
    if context.world.dimension.respawn_anchor_works {
        let target = callback_block_target(context.world_id, context.position);
        let cell = view
            .read(TruthView::Local, target)
            .ok_or(CallbackCaptureError::InvalidWrite(target))?;
        if let Value::Block(id) = cell.value
            && let Some(state) = BlockStateId::new(id)
            && state.to_block() == &pumpkin_data::Block::RESPAWN_ANCHOR
            && pumpkin_data::block_properties::RespawnAnchorLikeProperties::from_state_id(state)
                .charges
                > 0
        {
            facts.insert(target, Some(cell));
            snapshot.blocks.insert(context.position, state);
            let effects = [crate::block::neighbor::BlockCallbackEffect::Sound {
                position: context.position,
                sound: pumpkin_data::sound::Sound::BlockRespawnAnchorSetSpawn,
                volume_milli: 1000,
                pitch_milli: 1000,
            }];
            return capture_effects(
                view,
                context.world_id,
                &context.world.block_registry,
                snapshot,
                facts,
                &effects,
                4096,
            )
            .map(|capture| {
                (
                    crate::block::registry::BlockActionResult::SuccessServer,
                    capture,
                )
            });
        }
    }
    capture_normal_use(
        view,
        context.world_id,
        &context.world.block_registry,
        snapshot,
        facts,
        context.position,
        context.can_modify_world,
        context.sneaking,
        context.facing,
        4096,
    )
}

pub(super) fn complete_use_context<S: pumpkin_cluster::canonical_action::ActionState>(
    view: &S,
    context: &WorldUseContext,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    resolution: &crate::block::neighbor::BlockCallbackResolution,
) -> Result<
    pumpkin_cluster::canonical_action::Action,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure, ClaimedAction};
    let proposal = &context.proposal;
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    if let Some(menu) = &context.menu {
        use pumpkin_cluster::canonical_action::{ActionKind, Condition, Expected, SemanticIntent};
        let guards = context
            .policy_facts
            .iter()
            .map(|(target, cell)| Condition {
                target: *target,
                expected: Expected::Exact(cell.clone()),
            })
            .chain(std::iter::once(Condition {
                target: context.source.target,
                expected: Expected::Exact(context.source_cell.clone()),
            }))
            .collect();
        let mut action = menu.intent.capture_action(
            view,
            pumpkin_cluster::canonical_inventory::InventoryActionCapture {
                edit_authority: None,
                id: proposal.id,
                tick: context.tick,
                actor_chunk: Some(claim.intent.actor_chunk),
                player_leases: vec![context.lease],
                player_chunks: vec![(context.lease.key, claim.intent.actor_chunk)],
                guards,
                client_origin: proposal.client_origin.clone(),
            },
        )?;
        let statistic = Target::Chunk {
            key: claim.intent.actor_chunk,
            part: ChunkPart::LivingProperty {
                actor: proposal.id.actor,
                property: pumpkin_cluster::living_property::LivingProperty::Statistic {
                    category: 8,
                    key: menu.statistic,
                },
            },
            inventory: None,
        };
        let extra = vec![pumpkin_cluster::canonical_action::Effect::Add {
            target: statistic,
            amount: 1,
        }];
        let intent = pumpkin_cluster::world_action::WorldReplayIntent {
            steps: vec![
                pumpkin_cluster::world_action::WorldReplayStep::MenuSession(menu.intent.clone()),
                pumpkin_cluster::world_action::WorldReplayStep::RequiredEffects {
                    conditions: action.conditions.clone(),
                    effects: extra.clone(),
                },
            ],
        };
        action.effects.extend(extra);
        action.counterfactual_conditions = intent.read_targets().into_iter().collect();
        action.counterfactual_effects = intent.effect_targets().into_iter().collect();
        action.semantic_intent = Some(SemanticIntent::World(intent));
        action.kind = ActionKind::Block;
        action.validate()?;
        return Ok(action);
    }
    let actor = &context.actor;
    let Value::Living(living) = &actor.value else {
        return Err(ApplyFailure::Type(context.actor_target));
    };
    let avatar = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(context.actor_target))?;
    let actor_target = context.actor_target;
    let lease = context.lease;
    let world = context.world_id;
    let runtime_world = &context.world;
    let tick = context.tick;
    facts.insert(actor_target, Some(actor.clone()));
    facts.insert(context.source.target, Some(context.source_cell.clone()));
    facts.extend(
        context
            .policy_facts
            .iter()
            .map(|(target, cell)| (*target, Some(cell.clone()))),
    );
    let producer = context.producer;
    let mut action = build_callback_action(
        view,
        ActionRef {
            id: proposal.id,
            tick,
        },
        producer,
        Some(claim.intent.actor_chunk),
        Some(lease),
        world,
        &facts,
        &resolution,
    )?;
    let spatial_epoch = avatar.spatial_epoch;
    if resolution.presentation.iter().any(|effect| {
        matches!(
            effect,
            crate::block::neighbor::BlockCallbackEffect::Sound {
                sound: pumpkin_data::sound::Sound::BlockRespawnAnchorSetSpawn,
                ..
            }
        )
    }) {
        action.effects.push(
            pumpkin_cluster::canonical_action::Effect::SetPlayerRespawn {
                target: actor_target,
                respawn: Some(pumpkin_cluster::item_action::PlayerRespawnState {
                    world,
                    position: pumpkin_cluster::protocol::BlockPos {
                        x: context.position.0.x,
                        y: context.position.0.y,
                        z: context.position.0.z,
                    },
                    yaw: context.effective.yaw.to_bits(),
                    forced: false,
                }),
            },
        );
    }
    let actor_guard = action
        .conditions
        .iter_mut()
        .find(|condition| condition.target == actor_target)
        .ok_or(ApplyFailure::Condition(actor_target))?;
    actor_guard.expected =
        pumpkin_cluster::canonical_action::Expected::SpatialEpochEquals(spatial_epoch);
    action
        .conditions
        .push(pumpkin_cluster::canonical_action::Condition {
            target: Target::Chunk {
                key: claim.intent.actor_chunk,
                part: ChunkPart::LivingProperty {
                    actor: proposal.id.actor,
                    property: pumpkin_cluster::living_property::LivingProperty::Health,
                },
                inventory: None,
            },
            expected: pumpkin_cluster::canonical_action::Expected::LivingUnsignedAtLeast(1),
        });
    if avatar.game_mode != 1
        && let Value::Inventory(item) = &context.source_cell.value
        && ((item.item == pumpkin_data::item::Item::GLOWSTONE.id
        && resolution.writes.iter().any(|(position, before, after)| {
            *position == context.position
                && before.to_block() == &pumpkin_data::Block::RESPAWN_ANCHOR
                && pumpkin_data::block_properties::RespawnAnchorLikeProperties::from_state_id(
                    *after,
                )
                .charges
                    > pumpkin_data::block_properties::RespawnAnchorLikeProperties::from_state_id(
                        *before,
                    )
                    .charges
        })) || (item.item == pumpkin_data::item::Item::ENDER_EYE.id
            && resolution.writes.iter().any(|(position, before, after)| *position == context.position
                && before.to_block() == &pumpkin_data::Block::END_PORTAL_FRAME
                && !pumpkin_data::block_properties::EndPortalFrameLikeProperties::from_state_id(*before).eye
                && pumpkin_data::block_properties::EndPortalFrameLikeProperties::from_state_id(*after).eye)))
    {
        action
            .effects
            .push(pumpkin_cluster::canonical_action::Effect::TransformItems {
                intent: pumpkin_cluster::canonical_inventory::ItemTransformIntent {
                    source: context.source.clone(),
                    item: item.item,
                    components: item.nbt.clone(),
                    operation:
                        pumpkin_cluster::canonical_inventory::ItemTransformOperation::Consume {
                            count: 1,
                        },
                    remainder_destinations: Vec::new(),
                },
            });
    }
    append_resolution_light(view, runtime_world, world, &mut action, resolution)?;
    action.client_origin = proposal.client_origin;
    action.validate()?;
    Ok(action)
}
