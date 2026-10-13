use pumpkin_cluster::ntp::shared_offset_millis;
use pumpkin_cluster::time::TickStamp;
use pumpkin_util::PermissionLvl;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;
use pumpkin_world::level::Level;

use crate::command::argument_builder::{ArgumentBuilder, argument, command, literal};
use crate::command::argument_types::core::integer::IntegerArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};

const DESCRIPTION: &str =
    "Shows cluster sync stats: pending actions per chunk and ticks behind ground truth.";
const PERMISSION: &str = "minecraft:command.syncstats";

const ARG_X: &str = "x";
const ARG_Z: &str = "z";

const MAX_LISTED_CHUNKS: usize = 32;

struct PendingTotals {
    tracked_chunks: usize,
    pending_actions: usize,
    pending_chunks: usize,
}

struct ChunkPending {
    position: Vector2<i32>,
    actions: usize,
    tracked: bool,
}

fn collect_pending_totals(level: &Level) -> PendingTotals {
    let mut totals = PendingTotals {
        tracked_chunks: level.cluster_tracked_len(),
        pending_actions: 0,
        pending_chunks: 0,
    };
    for entry in level.cluster_dual.pending.iter() {
        let actions = entry.value().len();
        totals.pending_actions += actions;
        if actions > 0 {
            totals.pending_chunks += 1;
        }
    }
    totals
}

fn collect_pending_chunks(level: &Level) -> Vec<ChunkPending> {
    let mut listed = Vec::new();
    for entry in level.cluster_dual.pending.iter() {
        let actions = entry.value().len();
        if actions > 0 {
            listed.push(ChunkPending {
                position: *entry.key(),
                actions,
                tracked: level.cluster_dual.chunks.contains_key(entry.key()),
            });
        }
    }
    listed.sort_by_key(|item| (item.position.x, item.position.y));
    listed
}

fn pending_for_chunk(level: &Level, position: Vector2<i32>) -> ChunkPending {
    ChunkPending {
        actions: level
            .cluster_dual
            .pending
            .get(&position)
            .map(|found| found.len())
            .unwrap_or(0),
        tracked: level.cluster_dual.chunks.contains_key(&position),
        position,
    }
}

fn unix_millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as i64)
        .unwrap_or(0)
}

fn bucket_behind(level: &Level) -> (Option<u16>, Option<u16>, usize) {
    let ground = level.cluster_ground_tick();
    let applied = level.cluster_applied_tick();
    let behind = match (applied, ground) {
        (Some(applied), Some(ground)) => {
            TickStamp(ground).distance_since(TickStamp(applied)) as usize
        }
        _ => 0,
    };
    (ground, applied, behind)
}

fn tick_label(value: Option<u16>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "none".to_owned(),
    }
}

fn sender_chunk(context: &CommandContext) -> Vector2<i32> {
    let block = BlockPos::floored_v(context.source.position);
    Vector2::new(block.0.x >> 4, block.0.z >> 4)
}

fn tracked_label(tracked: bool) -> &'static str {
    if tracked { "tracked" } else { "untracked" }
}

fn saturate_count(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

fn send_overall(context: &CommandContext, totals: &PendingTotals) {
    context.source.send_feedback(
        TextComponent::text(format!(
            "Sync stats: {} tracked chunks, {} pending actions in {} chunks",
            totals.tracked_chunks, totals.pending_actions, totals.pending_chunks
        )),
        false,
    );
}

fn send_chunk_line(context: &CommandContext, item: &ChunkPending) {
    context.source.send_feedback(
        TextComponent::text(format!(
            "Chunk [{}, {}]: {} pending actions ({})",
            item.position.x,
            item.position.y,
            item.actions,
            tracked_label(item.tracked)
        )),
        false,
    );
}

fn send_ticks(context: &CommandContext) {
    let level = &context.source.world().level;
    let (ground, applied, behind) = bucket_behind(level);
    context.source.send_feedback(
        TextComponent::text(format!(
            "Disciplined global tick {}, ground tick {}, applied tick {}, {} ticks behind ground truth",
            TickStamp::now().0,
            tick_label(ground),
            tick_label(applied),
            behind
        )),
        false,
    );
}

fn send_world_time(context: &CommandContext) {
    let snapshot = if context.server().advanced_config.cluster.enabled {
        let view = context.source.world().cluster_environment_view.load_full();
        Some((view.time.time_of_day, view.time.world_age))
    } else {
        context
            .source
            .world()
            .level_time
            .try_lock()
            .ok()
            .map(|time| (time.time_of_day, time.world_age))
    };
    let line = match snapshot {
        Some((time_of_day, _)) => format!(
            "World time {}d+{} ticks",
            time_of_day.div_euclid(24000),
            time_of_day.rem_euclid(24000)
        ),
        None => "World time unavailable".to_owned(),
    };
    context
        .source
        .send_feedback(TextComponent::text(line), false);
}

fn send_ntp_time(context: &CommandContext) {
    let offset = shared_offset_millis();
    let wall = unix_millis_now().saturating_add(offset.unwrap_or(0));
    let offset_text = match offset {
        Some(offset) => format!("{offset}ms"),
        None => "none".to_owned(),
    };
    context.source.send_feedback(
        TextComponent::text(format!(
            "NTP time: disciplined wall {}ms (offset {}, disciplined: {}, fallbacks: {})",
            wall,
            offset_text,
            crate::server::cluster::ntp_disciplined(),
            crate::server::cluster::ntp_undisciplined_fallbacks()
        )),
        false,
    );
}

fn send_pending_list(context: &CommandContext, level: &Level) {
    let listed = collect_pending_chunks(level);
    for item in listed.iter().take(MAX_LISTED_CHUNKS) {
        send_chunk_line(context, item);
    }
    if listed.len() > MAX_LISTED_CHUNKS {
        context.source.send_feedback(
            TextComponent::text(format!(
                "... and {} more chunks with pending actions",
                listed.len() - MAX_LISTED_CHUNKS
            )),
            false,
        );
    }
}

struct SyncstatsAllExecutor;

impl CommandExecutor for SyncstatsAllExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        if context.server().advanced_config.cluster.enabled {
            return execute_cluster(context, None);
        }
        let level = &context.source.world().level;
        let totals = collect_pending_totals(level);
        send_overall(context, &totals);
        send_pending_list(context, level);
        send_ticks(context);
        send_world_time(context);
        send_ntp_time(context);
        Ok(saturate_count(totals.pending_actions))
    }
}

struct SyncstatsChunkSelfExecutor;

impl CommandExecutor for SyncstatsChunkSelfExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        context.source.entity_or_err()?;
        execute_single_chunk(context, sender_chunk(context))
    }
}

struct SyncstatsChunkAtExecutor;

impl CommandExecutor for SyncstatsChunkAtExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let position = Vector2::new(
            IntegerArgumentType::get(context, ARG_X)?,
            IntegerArgumentType::get(context, ARG_Z)?,
        );
        execute_single_chunk(context, position)
    }
}

fn execute_single_chunk(context: &CommandContext, position: Vector2<i32>) -> CommandExecutorResult {
    if context.server().advanced_config.cluster.enabled {
        return execute_cluster(context, Some(position));
    }
    let level = &context.source.world().level;
    let item = pending_for_chunk(level, position);
    let totals = collect_pending_totals(level);
    send_chunk_line(context, &item);
    send_overall(context, &totals);
    send_ticks(context);
    send_world_time(context);
    send_ntp_time(context);
    Ok(saturate_count(item.actions))
}

fn execute_cluster(
    context: &CommandContext,
    position: Option<Vector2<i32>>,
) -> CommandExecutorResult {
    let server = context.server();
    let snapshot = server.cluster_sync_stats.load_full();
    let world = crate::server::cluster_world_delta::world_id(context.source.world());
    let mut chunks = snapshot
        .chunks
        .iter()
        .filter(|(key, _)| key.world == world)
        .collect::<Vec<_>>();
    chunks.sort_by_key(|(key, _)| (key.x(), key.z()));
    let totals = PendingTotals {
        tracked_chunks: chunks.iter().filter(|(_, stats)| stats.tracked).count(),
        pending_actions: snapshot.pending_by_world.get(&world).copied().unwrap_or(0),
        pending_chunks: chunks
            .iter()
            .filter(|(_, stats)| stats.pending_actions > 0)
            .count(),
    };
    let result = if let Some(position) = position {
        let key = pumpkin_cluster::protocol::ChunkKey::new(world, position.x, position.y);
        let stats = snapshot.chunks.get(&key);
        let item = ChunkPending {
            position,
            actions: stats.map_or(0, |stats| stats.pending_actions),
            tracked: stats.is_some_and(|stats| stats.tracked),
        };
        send_chunk_line(context, &item);
        item.actions
    } else {
        for (key, stats) in chunks
            .iter()
            .filter(|(_, stats)| stats.pending_actions > 0)
            .take(MAX_LISTED_CHUNKS)
        {
            send_chunk_line(
                context,
                &ChunkPending {
                    position: Vector2::new(key.x(), key.z()),
                    actions: stats.pending_actions,
                    tracked: stats.tracked,
                },
            );
        }
        if totals.pending_chunks > MAX_LISTED_CHUNKS {
            context.source.send_feedback(
                TextComponent::text(format!(
                    "... and {} more chunks with pending actions",
                    totals.pending_chunks - MAX_LISTED_CHUNKS
                )),
                false,
            );
        }
        totals.pending_actions
    };
    send_overall(context, &totals);
    let ground = chunks
        .iter()
        .filter(|(key, _)| {
            position.is_none_or(|position| key.x() == position.x && key.z() == position.y)
        })
        .filter_map(|(_, stats)| stats.ground_tick)
        .filter(|tick| snapshot.published_tick.distance_since(*tick) < 32768)
        .max_by_key(|tick| snapshot.published_tick.distance_since(*tick));
    let behind = ground.map(|ground| snapshot.published_tick.distance_since(ground));
    context.source.send_feedback(TextComponent::text(format!(
        "Disciplined global tick {}, ground tick {}, applied tick {}, {} ticks behind ground truth",
        TickStamp::now().0, tick_label(ground.map(|tick| tick.0)), snapshot.published_tick.0,
        behind.map_or_else(|| "unknown".to_owned(), |behind| behind.to_string()),
    )), false);
    send_world_time(context);
    send_ntp_time(context);
    Ok(saturate_count(result))
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    dispatcher.register(
        command("syncstats", DESCRIPTION)
            .requires(PERMISSION)
            .executes(SyncstatsAllExecutor)
            .then(literal("all").executes(SyncstatsAllExecutor))
            .then(literal("chunk").executes(SyncstatsChunkSelfExecutor).then(
                argument(ARG_X, IntegerArgumentType::any()).then(
                    argument(ARG_Z, IntegerArgumentType::any()).executes(SyncstatsChunkAtExecutor),
                ),
            )),
    );
}
