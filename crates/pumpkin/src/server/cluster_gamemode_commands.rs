use std::sync::Arc;

use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::living_property::{LivingProperty, LivingPropertyValue};

pub(crate) fn submit(
    server: &super::Server,
    source: &crate::command::CommandSource,
    lease: LivePlayerLease,
    mode: u8,
    success: Option<pumpkin_util::text::TextComponent>,
) -> Result<[u8; 16], super::cluster_player_commands::PlayerCommandSubmitError> {
    use super::cluster_player_commands::PlayerCommandSubmitError;
    if mode > 3
        || server
            .cluster_actor_statistics
            .load()
            .get(&lease.gid)
            .is_none_or(|state| state.incarnation != lease.incarnation)
    {
        return Err(PlayerCommandSubmitError::NoTarget);
    }
    let living = server.cluster_actor_living.load();
    let previous = living
        .get(&lease.gid.into())
        .and_then(|state| state.avatar.as_ref())
        .ok_or(PlayerCommandSubmitError::NoTarget)?
        .game_mode;
    super::cluster_player_commands::submit_with_feedback(
        server,
        source,
        lease.gid,
        lease.key.0.into_bytes(),
        pumpkin_cluster::player_command_control::PlayerCommandMutation::GameMode { mode, previous },
        success,
    )
}

pub(super) fn materialize(
    view: &impl ActionState,
    metadata: super::cluster_world_owner::PlayerAdministrationMaterialization,
    tick: pumpkin_cluster::time::TickStamp,
    mode: u8,
    previous: u8,
    detach: Option<pumpkin_cluster::actor_relation::PlayerDetachPlan>,
) -> Result<Action, ApplyFailure> {
    if mode > 3 || previous > 3 || mode != 3 && detach.is_some() {
        return Err(ApplyFailure::InvalidAction);
    }
    let body = Target::Chunk {
        key: metadata.association,
        part: ChunkPart::Player(metadata.lease.gid),
        inventory: None,
    };
    let target = Target::Chunk {
        key: metadata.association,
        part: ChunkPart::LivingProperty {
            actor: metadata.lease.gid.into(),
            property: LivingProperty::GameMode,
        },
        inventory: None,
    };
    let before = view
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?;
    if before.value != Value::LivingProperty(LivingPropertyValue::Unsigned(u32::from(previous))) {
        return Err(ApplyFailure::Condition(target));
    }
    let mut conditions = vec![
        Condition {
            target,
            expected: Expected::Exact(before),
        },
        Condition {
            target: body,
            expected: Expected::SpatialEpochEquals(metadata.epoch),
        },
    ];
    let mut effects = vec![Effect::Put {
        target,
        value: Value::LivingProperty(LivingPropertyValue::Unsigned(u32::from(mode))),
    }];
    let mut chunks = vec![metadata.association];
    if mode != previous && (mode == 3 || previous == 3) {
        effects.push(Effect::SetPlayerCamera {
            target: body,
            camera: None,
        });
    }
    if let Some(detach) = detach {
        conditions.extend(detach.conditions);
        effects.extend(detach.effects);
        chunks.extend(detach.chunks);
    }
    chunks.sort_unstable();
    chunks.dedup();
    let mut dependencies = conditions
        .iter()
        .filter_map(|guard| match &guard.expected {
            Expected::SpatialEpochEquals(SpatialEpoch::Action(reference)) => Some(*reference),
            _ => None,
        })
        .collect::<Vec<_>>();
    dependencies.sort_unstable();
    dependencies.dedup();
    let action = Action {
        id: metadata.id,
        tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        edit_authority: None,
        actor_chunk: Some(metadata.association),
        player_leases: vec![metadata.lease],
        player_chunks: Vec::new(),
        stakeholders: chunks.into_iter().map(Stakeholder::Chunk).collect(),
        dependencies,
        conditions,
        effects,
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

pub(super) struct PolicyResult {
    pub command: super::cluster_player_commands::AuthenticatedPlayerCommand,
    pub lease: LivePlayerLease,
    pub generation: u64,
    pub epoch: SpatialEpoch,
    pub mode: Option<u8>,
}

pub(super) fn begin_policy(
    server: &Arc<super::Server>,
    player: &Arc<crate::entity::player::Player>,
    command: super::cluster_player_commands::AuthenticatedPlayerCommand,
    results: Arc<crossbeam::queue::ArrayQueue<PolicyResult>>,
) -> Result<(), super::cluster_player_commands::AuthenticatedPlayerCommand> {
    let pumpkin_cluster::player_command_control::PlayerCommandMutation::GameMode { mode, previous } =
        &command.request.mutation
    else {
        return Err(command);
    };
    let (mode, previous) = (*mode, *previous);
    let Some(lease) = super::cluster_world_owner::player_action_lease(server, player) else {
        return Err(command);
    };
    let living = server.cluster_actor_living.load();
    let Some(avatar) = living
        .get(&lease.gid.into())
        .and_then(|state| state.avatar.as_ref())
    else {
        return Err(command);
    };
    if avatar.game_mode != previous {
        return Err(command);
    }
    let Some(world) = server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == avatar.world)
        .cloned()
    else {
        return Err(command);
    };
    let position = avatar.position.map(f64::from_bits);
    let source = crate::command::CommandSource::new(
        crate::command::CommandSender::Player(player.clone()),
        world,
        Some(player.clone()),
        pumpkin_util::math::vector3::Vector3::new(position[0], position[1], position[2]),
        pumpkin_util::math::vector2::Vector2::new(
            f32::from_bits(avatar.yaw),
            f32::from_bits(avatar.pitch),
        ),
        player.gameprofile.name.clone(),
        pumpkin_util::text::TextComponent::text(player.gameprofile.name.clone()),
        server.clone(),
    );
    let generation = player
        .cluster_owner_install_generation
        .load(std::sync::atomic::Ordering::Acquire);
    let epoch = avatar.spatial_epoch;
    let server = server.clone();
    let worker = server.clone();
    let player = player.clone();
    server.spawn_task(async move {
        let mut event =
            crate::plugin::api::events::player::player_gamemode_change::PlayerGamemodeChangeEvent {
                player,
                new_gamemode: pumpkin_util::GameMode::try_from(i32::from(mode)).unwrap(),
                previous_gamemode: pumpkin_util::GameMode::try_from(i32::from(previous)).unwrap(),
                cancelled: false,
            };
        let completed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::cluster_permissions::with_source(
                &worker,
                &source,
                worker.plugin_manager.fire(&worker, &mut event),
            ),
        )
        .await
        .is_ok();
        let mode = (completed && !event.cancelled).then_some(event.new_gamemode as u8);
        assert!(
            results
                .push(PolicyResult {
                    command,
                    lease,
                    generation,
                    epoch,
                    mode
                })
                .is_ok()
        );
    });
    Ok(())
}
