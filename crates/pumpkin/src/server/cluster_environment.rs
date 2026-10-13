use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Weak};

use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_time::{
    BorderMutation, BorderState, EnvironmentControl, EnvironmentMutation, EnvironmentSnapshot,
    MorningTransition, SpawnState, TimeUpdate, WeatherUpdate,
};
use tokio::sync::mpsc;

use crate::world::World;
use crate::world::time::LevelTime;
use crate::world::weather::Weather;

use super::Server;

#[derive(Clone)]
pub(crate) struct CertifiedConfigurationCut {
    pub evidence: Arc<super::cluster_configuration_authority::ConfigurationBaselineEvidence>,
    pub target: PeerGeneration,
    pub from_slot: u64,
    pub environments: BTreeMap<pumpkin_cluster::protocol::WorldId, Arc<EnvironmentSnapshot>>,
    pub gamerules: Arc<super::cluster_gamerules::GameruleSnapshot>,
    pub health: pumpkin_cluster::health_policy::HealthPolicyState,
    pub retirements: pumpkin_cluster::world_time::EnvironmentRetirements,
}

pub(crate) fn authority(
    server: &Server,
    target: PeerGeneration,
    slot: u64,
) -> Option<Arc<CertifiedConfigurationCut>> {
    server
        .cluster_configuration_authority
        .load_full()
        .filter(|cut| cut.target == target && cut.from_slot <= slot)
}

fn retained_roots(server: &Server) -> pumpkin_cluster::world_time::EnvironmentRoots {
    let mut roots = (**server.cluster_environment_roots.load()).clone();
    if let Some(cut) = server.cluster_configuration_authority.load_full() {
        roots.retirements = cut.retirements.clone();
    }
    roots
}

#[derive(Clone)]
pub struct GroundSleepView {
    pub world: pumpkin_cluster::protocol::WorldId,
    pub spectator: bool,
    pub health: f32,
    pub sleeping_since: Option<TickStamp>,
}

#[derive(Clone)]
pub struct EnvironmentView {
    evidence: Option<pumpkin_cluster::world_time::PinnedEnvironment>,
    pub time: LevelTime,
    pub weather: Weather,
    pub snapshot: Option<EnvironmentSnapshot>,
    pub morning: Option<MorningTransition>,
    pub border: BorderState,
    pub spawn: SpawnState,
}

impl Default for EnvironmentView {
    fn default() -> Self {
        Self {
            evidence: None,
            time: LevelTime::new(),
            weather: Weather::new(),
            snapshot: None,
            morning: None,
            border: BorderState {
                center_x: 0.0,
                center_z: 0.0,
                old_diameter: 59_999_968.0,
                new_diameter: 59_999_968.0,
                speed: 0,
                lerp_start_slot: 0,
                warning_blocks: 5,
                warning_time: 15,
                damage_per_block: 0.2,
                buffer: 5.0,
            },
            spawn: SpawnState {
                x: 0,
                y: 0,
                z: 0,
                yaw: 0.0,
                pitch: 0.0,
            },
        }
    }
}

impl EnvironmentView {
    pub fn from_level_info(info: &pumpkin_world::world_info::LevelData) -> Self {
        let mut view = Self::default();
        view.time.time_of_day = info.day_time;
        view.weather.clear_weather_time = info.clear_weather_time;
        view.border = BorderState {
            center_x: info.border_center_x,
            center_z: info.border_center_z,
            old_diameter: info.border_size,
            new_diameter: info.border_size_lerp_target,
            speed: info.border_size_lerp_time,
            lerp_start_slot: super::cluster_world_rules::disciplined_slot().unwrap_or(0),
            warning_blocks: info.border_warning_blocks as i32,
            warning_time: info.border_warning_time as i32,
            damage_per_block: info.border_damage_per_block as f32,
            buffer: info.border_safe_zone as f32,
        };
        view.spawn = SpawnState {
            x: info.spawn_x,
            y: info.spawn_y,
            z: info.spawn_z,
            yaw: info.spawn_yaw,
            pitch: info.spawn_pitch,
        };
        view
    }
}

pub fn sky_darkening(
    view: &EnvironmentView,
    dimension: &pumpkin_data::dimension::Dimension,
) -> i32 {
    use pumpkin_data::environment_attribute::{DayTimeline, sample_float_track};
    let multiplier = if dimension.has_skylight && !dimension.has_fixed_time {
        sample_float_track(
            DayTimeline::SKY_LIGHT_LEVEL_KEYFRAMES,
            DayTimeline::PERIOD_TICKS,
            view.time.time_of_day,
        )
    } else {
        1.0
    };
    let mut level = dimension.effective_sky_light_level() * multiplier;
    if dimension.has_skylight {
        let rain = (view.weather.rain_level - view.weather.thunder_level).max(0.0);
        if rain > 0.0 {
            level += rain * 0.3125 * (4.0 - level);
        }
        if view.weather.thunder_level > 0.0 {
            level += view.weather.thunder_level * 0.52734375 * (4.0 - level);
        }
    }
    (15.0 - level.clamp(0.0, 15.0)).clamp(0.0, 15.0) as i32
}

pub use pumpkin_cluster::world_time::EnvironmentMutation as TimeMutation;

#[derive(Clone)]
pub struct EnvironmentTickRules {
    advance_time: bool,
    advance_weather: bool,
    sleeping_percentage: i64,
}

#[derive(Clone)]
pub enum EnvironmentInput {
    Tick {
        world: Weak<World>,
        tick: TickStamp,
        slot: u64,
        rules: EnvironmentTickRules,
        sleepers: Arc<HashMap<pumpkin_cluster::actor::ActorId, GroundSleepView>>,
    },
    Time {
        world: Weak<World>,
        mutation: TimeMutation,
    },
    Weather {
        world: Weak<World>,
        clear: i32,
        rain: i32,
        raining: bool,
        thundering: bool,
    },
    Remote {
        peer: pumpkin_cluster::identity::ServerId,
        generation: u64,
        control: EnvironmentControl,
    },
    WeatherFlag {
        world: Weak<World>,
        rain: Option<bool>,
        thunder: Option<bool>,
    },
    Publish {
        world: Weak<World>,
        weather: bool,
    },
}

pub struct EnvironmentRuntime {
    tx: mpsc::Sender<EnvironmentInput>,
    hooks: mpsc::Sender<WeatherHook>,
}

pub struct WeatherHook {
    world: Weak<World>,
    rain: Option<bool>,
    thunder: Option<bool>,
}

fn mutation_source_available(world: &World) -> bool {
    world.server.upgrade().is_some_and(|server| {
        !server
            .cluster_global_shutdown
            .load(std::sync::atomic::Ordering::Acquire)
            && super::cluster_world_rules::primary(&server).is_some_and(|issuer| {
                world
                    .cluster_environment_view
                    .load()
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.issuer == issuer)
            })
            && super::cluster_world_rules::disciplined_slot().is_some()
    })
}

impl EnvironmentRuntime {
    pub fn new() -> (
        Self,
        mpsc::Receiver<EnvironmentInput>,
        mpsc::Receiver<WeatherHook>,
    ) {
        let (tx, rx) = mpsc::channel(1024);
        let (hooks, hook_rx) = mpsc::channel(256);
        (Self { tx, hooks }, rx, hook_rx)
    }

    pub fn tick(&self, world: &Arc<World>) -> bool {
        let Some(server) = world.server.upgrade() else {
            return false;
        };
        if server
            .cluster_global_shutdown
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return false;
        }
        let Some(slot) = super::cluster_world_rules::disciplined_slot() else {
            return false;
        };
        let Some(issuer) = super::cluster_world_rules::primary(&server) else {
            return false;
        };
        let Some(rules) = tick_rules_at(
            &server,
            super::cluster_world_delta::world_id(world),
            issuer,
            slot,
        ) else {
            return false;
        };
        self.tx
            .try_send(EnvironmentInput::Tick {
                world: Arc::downgrade(world),
                tick: TickStamp(slot as u16),
                slot,
                rules,
                sleepers: server.cluster_ground_sleep.load_full(),
            })
            .is_ok()
    }

    pub fn time(&self, world: &Arc<World>, mutation: TimeMutation) -> bool {
        if !mutation_source_available(world)
            || !pumpkin_cluster::world_time::valid_environment_mutation(&mutation)
        {
            return false;
        }
        self.tx
            .try_send(EnvironmentInput::Time {
                world: Arc::downgrade(world),
                mutation,
            })
            .is_ok()
    }

    pub fn weather(
        &self,
        world: &Arc<World>,
        clear: i32,
        rain: i32,
        raining: bool,
        thundering: bool,
    ) -> bool {
        if !mutation_source_available(world) || clear < 0 || rain < 0 {
            return false;
        }
        self.tx
            .try_send(EnvironmentInput::Weather {
                world: Arc::downgrade(world),
                clear,
                rain,
                raining,
                thundering,
            })
            .is_ok()
    }

    pub fn remote(
        &self,
        peer: pumpkin_cluster::identity::ServerId,
        generation: u64,
        control: EnvironmentControl,
    ) -> bool {
        self.tx
            .try_send(EnvironmentInput::Remote {
                peer,
                generation,
                control,
            })
            .is_ok()
    }

    pub fn morning(&self, world: &Arc<World>) -> bool {
        self.morning_at(world, TickStamp::now())
    }

    pub fn morning_at(&self, world: &Arc<World>, tick: TickStamp) -> bool {
        let Some(server) = world.server.upgrade() else {
            return false;
        };
        let local = pumpkin_cluster::identity::ServerId(server.advanced_config.cluster.server_id);
        if !super::cluster_world_rules::primary(&server)
            .is_some_and(|issuer| issuer.server == local)
        {
            return false;
        }
        let Some(slot) = super::cluster_world_rules::slot_for_tick(tick) else {
            return false;
        };
        self.time(world, TimeMutation::Morning { slot })
    }

    pub fn publish(&self, world: &Arc<World>, weather: bool) -> bool {
        if !mutation_source_available(world) {
            return false;
        }
        self.tx
            .try_send(EnvironmentInput::Publish {
                world: Arc::downgrade(world),
                weather,
            })
            .is_ok()
    }

    pub fn weather_flag(
        &self,
        world: &Arc<World>,
        rain: Option<bool>,
        thunder: Option<bool>,
    ) -> bool {
        if !mutation_source_available(world) {
            return false;
        }
        self.hooks
            .try_send(WeatherHook {
                world: Arc::downgrade(world),
                rain,
                thunder,
            })
            .is_ok()
    }
}

#[derive(Clone)]
struct OwnedEnvironment {
    view: EnvironmentView,
    last_tick: Option<TickStamp>,
    last_slot: Option<u64>,
    morning_latched: bool,
}

fn state_for<'a>(
    states: &'a mut HashMap<uuid::Uuid, OwnedEnvironment>,
    world: &World,
) -> &'a mut OwnedEnvironment {
    states
        .entry(world.uuid)
        .or_insert_with(|| OwnedEnvironment {
            view: {
                let mut view = world.cluster_environment_view.load().as_ref().clone();
                if view.snapshot.is_none() {
                    let info = world.level_info.load();
                    view.time.load_from(info.day_time, 0);
                    view.border = BorderState {
                        center_x: info.border_center_x,
                        center_z: info.border_center_z,
                        old_diameter: info.border_size,
                        new_diameter: info.border_size_lerp_target,
                        speed: info.border_size_lerp_time,
                        lerp_start_slot: super::cluster_world_rules::disciplined_slot()
                            .unwrap_or(0),
                        warning_blocks: info.border_warning_blocks as i32,
                        warning_time: info.border_warning_time as i32,
                        damage_per_block: info.border_damage_per_block as f32,
                        buffer: info.border_safe_zone as f32,
                    };
                    view.spawn = SpawnState {
                        x: info.spawn_x,
                        y: info.spawn_y,
                        z: info.spawn_z,
                        yaw: info.spawn_yaw,
                        pitch: info.spawn_pitch,
                    };
                }
                view
            },
            last_tick: None,
            last_slot: None,
            morning_latched: false,
        })
}

pub fn border_at(
    server: &Server,
    world: pumpkin_cluster::protocol::WorldId,
    tick: TickStamp,
) -> Option<pumpkin_cluster::world_time::WorldBorderWitness> {
    let issuer = super::cluster_world_rules::primary(server)?;
    let slot = super::cluster_world_rules::slot_for_tick(tick)?;
    server
        .cluster_world_borders
        .load()
        .current(world, issuer, slot)
        .map(|snapshot| snapshot.witness(slot))
}

pub fn spawn_at(
    server: &Server,
    world: pumpkin_cluster::protocol::WorldId,
    tick: TickStamp,
) -> Option<SpawnState> {
    let issuer = super::cluster_world_rules::primary(server)?;
    let slot = super::cluster_world_rules::slot_for_tick(tick)?;
    server
        .cluster_world_borders
        .load()
        .current(world, issuer, slot)
        .map(|snapshot| snapshot.spawn.clone())
}

pub fn environment_at_slot(
    server: &Server,
    world: pumpkin_cluster::protocol::WorldId,
    slot: u64,
) -> Option<Arc<EnvironmentView>> {
    let issuer = super::cluster_world_rules::primary(server)?;
    environment_at_source(server, world, issuer, slot)
}

pub fn environment_at_source(
    server: &Server,
    world: pumpkin_cluster::protocol::WorldId,
    issuer: PeerGeneration,
    slot: u64,
) -> Option<Arc<EnvironmentView>> {
    let evidence = server
        .cluster_environment_history
        .load()
        .get(world, issuer, slot)?;
    let mut view = EnvironmentView::default();
    restore_environment(&mut view, (*evidence.snapshot).clone());
    view.morning = evidence.snapshot.morning;
    view.snapshot = Some((*evidence.snapshot).clone());
    view.evidence = Some(evidence);
    Some(Arc::new(view))
}

fn retain_environment(
    server: &Server,
    world: &World,
    source: PeerGeneration,
    elected: PeerGeneration,
    value: &EnvironmentSnapshot,
) -> Result<(), pumpkin_cluster::world_time::EnvironmentEvidenceError> {
    let world_id = super::cluster_world_delta::world_id(world);
    let mut roots = retained_roots(server);
    server
        .cluster_environment_history
        .load()
        .retain_required_roots(&mut roots);
    let mut configs = (**server.cluster_world_borders.load()).clone();
    configs.try_insert(
        source,
        elected,
        pumpkin_cluster::world_time::WorldBorderSnapshot {
            world: world_id,
            issuer: value.issuer,
            revision: value.config_revision,
            from_slot: value.config_from_slot,
            border: value.border.clone(),
            spawn: value.spawn.clone(),
        },
        &roots,
    )?;
    let mut mornings = (**server.cluster_mornings.load()).clone();
    if let Some(morning) = value.morning {
        mornings.try_insert(source, elected, world_id, morning, &roots)?;
    }
    let mut environments = (**server.cluster_environment_history.load()).clone();
    environments.try_insert(source, elected, world_id, Arc::new(value.clone()), &roots)?;
    server.cluster_world_borders.store(Arc::new(configs));
    server.cluster_mornings.store(Arc::new(mornings));
    server
        .cluster_environment_history
        .store(Arc::new(environments));
    Ok(())
}

fn snapshot(
    world: &World,
    state: &OwnedEnvironment,
    issuer: PeerGeneration,
    revision: u64,
    slot: u64,
) -> EnvironmentSnapshot {
    let config = state
        .view
        .snapshot
        .as_ref()
        .filter(|previous| {
            previous.issuer == issuer
                && previous.border == state.view.border
                && previous.spawn == state.view.spawn
        })
        .map(|previous| (previous.config_revision, previous.config_from_slot))
        .unwrap_or((revision, slot));
    EnvironmentSnapshot {
        rain_level: state.view.weather.rain_level,
        old_rain_level: state.view.weather.old_rain_level,
        thunder_level: state.view.weather.thunder_level,
        old_thunder_level: state.view.weather.old_thunder_level,
        config_revision: config.0,
        config_from_slot: config.1,
        issuer,
        revision,
        slot,
        world_age: state.view.time.world_age,
        border: state.view.border.clone(),
        spawn: state.view.spawn.clone(),
        time: TimeUpdate::new(
            world.dimension.minecraft_name.to_owned(),
            state.view.time.time_of_day,
            state.view.time.partial_tick,
            state.view.time.rate,
            state.view.time.paused,
        ),
        weather: WeatherUpdate::new(
            world.dimension.minecraft_name.to_owned(),
            state.view.weather.clear_weather_time,
            state.view.weather.rain_time,
            state.view.weather.thunder_time,
            state.view.weather.raining,
            state.view.weather.thundering,
        ),
        morning: state
            .view
            .morning
            .filter(|morning| morning.issuer == issuer),
    }
}

fn set_weather(weather: &mut Weather, clear: i32, rain: i32, raining: bool, thundering: bool) {
    weather.clear_weather_time = clear;
    weather.rain_time = rain;
    weather.thunder_time = rain;
    weather.raining = raining;
    weather.thundering = thundering;
}

fn publish_weather(world: &World, old: &Weather, new: &Weather) {
    use pumpkin_protocol::java::client::play::{CGameEvent, GameEvent};
    let excluded = world
        .players
        .load()
        .iter()
        .filter(|player| player.is_in_cluster_lobby())
        .map(|player| player.gameprofile.id)
        .collect::<Vec<_>>();
    if old.raining != new.raining {
        world.broadcast_packet_except(
            &excluded,
            &CGameEvent::new(
                if new.raining {
                    GameEvent::BeginRaining
                } else {
                    GameEvent::EndRaining
                },
                0.0,
            ),
        );
    }
    if old.rain_level != new.rain_level {
        world.broadcast_packet_except(
            &excluded,
            &CGameEvent::new(GameEvent::RainLevelChange, new.rain_level),
        );
    }
    if old.thunder_level != new.thunder_level {
        world.broadcast_packet_except(
            &excluded,
            &CGameEvent::new(GameEvent::ThunderLevelChange, new.thunder_level),
        );
    }
}

fn commit_environment(
    server: &Server,
    world: &World,
    state: &mut OwnedEnvironment,
    mut next: OwnedEnvironment,
    source: PeerGeneration,
    elected: PeerGeneration,
    value: EnvironmentSnapshot,
) -> Result<(), pumpkin_cluster::world_time::EnvironmentEvidenceError> {
    retain_environment(server, world, source, elected, &value)?;
    let config_changed =
        state.view.border != next.view.border || state.view.spawn != next.view.spawn;
    publish_weather(world, &state.view.weather, &next.view.weather);
    next.view.snapshot = Some(value);
    *state = next;
    world
        .cluster_environment_view
        .store(Arc::new(state.view.clone()));
    if config_changed {
        publish_config(
            world,
            state,
            source == elected && elected.server.0 == server.advanced_config.cluster.server_id,
        );
    }
    Ok(())
}

fn apply_mutation(
    state: &mut OwnedEnvironment,
    mutation: EnvironmentMutation,
    issuer: PeerGeneration,
    revision: u64,
    slot: u64,
    advance_weather: bool,
) {
    match mutation {
        EnvironmentMutation::Border(mutation) => {
            let mut border = state.view.border.clone();
            match mutation {
                BorderMutation::Center(x, z) => {
                    border.center_x = x;
                    border.center_z = z;
                }
                BorderMutation::Diameter(value, speed) => {
                    border.old_diameter = border.current_diameter(slot);
                    border.new_diameter = value;
                    border.speed = speed.unwrap_or(0);
                    border.lerp_start_slot = slot;
                }
                BorderMutation::Add(value, speed) => {
                    border.old_diameter = border.current_diameter(slot);
                    border.new_diameter = border.old_diameter + value;
                    border.speed = speed.unwrap_or(0);
                    border.lerp_start_slot = slot;
                }
                BorderMutation::WarningDistance(value) => border.warning_blocks = value,
                BorderMutation::WarningTime(value) => border.warning_time = value,
                BorderMutation::Damage(value) => border.damage_per_block = value,
                BorderMutation::Buffer(value) => border.buffer = value,
                BorderMutation::Reset => border = EnvironmentView::default().border,
            }
            if pumpkin_cluster::world_time::valid_border(&border) {
                state.view.border = border;
            }
        }
        EnvironmentMutation::Spawn(value) => state.view.spawn = value,
        EnvironmentMutation::Set(value) => state.view.time.set_time(value),
        EnvironmentMutation::Add(value) => {
            state.view.time.time_of_day = state.view.time.time_of_day.saturating_add(value).max(0)
        }
        EnvironmentMutation::Pause(value) => state.view.time.set_paused(value),
        EnvironmentMutation::Rate(value) => state.view.time.set_rate(value),
        EnvironmentMutation::Morning { slot: source_slot } => {
            if state.morning_latched {
                return;
            }
            if source_slot > slot {
                return;
            }
            let day = state.view.time.time_of_day.div_euclid(24000);
            let Some(next) = day.checked_add(1).and_then(|day| day.checked_mul(24000)) else {
                return;
            };
            state.view.time.set_time(next);
            state.morning_latched = true;
            state.view.morning = Some(MorningTransition {
                issuer,
                revision,
                slot: source_slot,
            });
            if advance_weather {
                set_weather(&mut state.view.weather, 0, 0, false, false);
            }
        }
        EnvironmentMutation::Weather {
            clear,
            rain,
            raining,
            thundering,
        } => set_weather(&mut state.view.weather, clear, rain, raining, thundering),
        EnvironmentMutation::WeatherFlag { rain, thunder } => {
            let raining = rain.unwrap_or(state.view.weather.raining);
            let thundering = thunder.unwrap_or(state.view.weather.thundering);
            set_weather(&mut state.view.weather, 0, 0, raining, thundering);
        }
        EnvironmentMutation::Publish => {}
    }
}

fn publish_config(world: &World, state: &OwnedEnvironment, persist: bool) {
    let border = &state.view.border;
    let slot = super::cluster_world_rules::disciplined_slot().unwrap_or(border.lerp_start_slot);
    let excluded = world
        .players
        .load()
        .iter()
        .filter(|player| player.is_in_cluster_lobby())
        .map(|player| player.gameprofile.id)
        .collect::<Vec<_>>();
    world.broadcast_packet_except(
        &excluded,
        &pumpkin_protocol::java::client::play::CInitializeWorldBorder::new(
            border.center_x,
            border.center_z,
            border.current_diameter(slot),
            border.new_diameter,
            border.remaining_millis(slot).into(),
            29_999_984.into(),
            border.warning_blocks.into(),
            border.warning_time.into(),
        ),
    );
    let spawn = pumpkin_util::math::position::BlockPos::new(
        state.view.spawn.x,
        state.view.spawn.y,
        state.view.spawn.z,
    );
    world.broadcast_packet_except_editioned(
        &excluded,
        &pumpkin_protocol::java::client::play::CPlayerSpawnPosition::new(
            spawn,
            state.view.spawn.yaw,
            state.view.spawn.pitch,
            world.dimension.minecraft_name.to_owned(),
        ),
        &pumpkin_protocol::bedrock::client::CSetSpawnPosition {
            spawn_position_type: pumpkin_protocol::bedrock::client::SpawnPositionType::WorldRespawn,
            block_position: spawn,
            spawn_block_pos: spawn,
            dimension_type: match world.dimension.minecraft_name {
                "minecraft:the_nether" => 1.into(),
                "minecraft:the_end" => 2.into(),
                _ => 0.into(),
            },
        },
    );
    if persist {
        world
            .level
            .should_save
            .store(true, std::sync::atomic::Ordering::Relaxed);
        world.level.level_channel.notify();
    }
}

pub(super) async fn save_configurations<T: Send>(
    rx: mpsc::Receiver<(std::path::PathBuf, T)>,
    encode: impl Fn(&T) -> Option<Vec<u8>> + Send,
) {
    drain_configurations(rx, encode, |path, bytes| async move {
        save_environment(&path, &bytes).await
    })
    .await;
}

async fn drain_configurations<
    T: Send,
    F: std::future::Future<Output = std::io::Result<()>> + Send,
>(
    mut rx: mpsc::Receiver<(std::path::PathBuf, T)>,
    encode: impl Fn(&T) -> Option<Vec<u8>> + Send,
    mut write: impl FnMut(std::path::PathBuf, Vec<u8>) -> F + Send,
) {
    let mut pending = BTreeMap::new();
    let mut failures = BTreeMap::<std::path::PathBuf, (std::time::Instant, u64)>::new();
    let mut retry = tokio::time::interval(std::time::Duration::from_millis(250));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            value = rx.recv(), if pending.len() < 32 && !rx.is_closed() || pending.len() < 32 && !rx.is_empty() => {
                if let Some((path, value)) = value { pending.insert(path, value); }
            }
            _ = retry.tick() => {
                let paths = pending.keys().cloned().collect::<Vec<_>>();
                for path in paths {
                    let result = match encode(&pending[&path]) {
                        Some(bytes) => write(path.clone(), bytes).await,
                        None => Err(std::io::Error::other("configuration encoding failed")),
                    };
                    match result {
                        Ok(()) => { pending.remove(&path); failures.remove(&path); }
                        Err(error) => {
                            let now = std::time::Instant::now();
                            let failure = failures.entry(path.clone()).or_insert((now.checked_sub(std::time::Duration::from_secs(300)).unwrap_or(now), 0));
                            if now.duration_since(failure.0) >= std::time::Duration::from_secs(300) {
                                tracing::warn!(domain="configuration_persistence", stream="Control", path=%path.display(), %error, suppressed=failure.1, "accepted configuration retained for disk retry");
                                *failure = (now, 0);
                            } else { failure.1 = failure.1.saturating_add(1); }
                        }
                    }
                }
            }
        }
        if rx.is_closed() && rx.is_empty() && pending.is_empty() {
            break;
        }
    }
}

pub(super) async fn save_environment(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension("tmp");
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(bytes).await?;
    file.sync_all().await?;
    tokio::fs::rename(&temporary, path).await?;
    if let Some(parent) = path.parent() {
        tokio::fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(())
}

fn restore_environment(view: &mut EnvironmentView, value: EnvironmentSnapshot) {
    view.time.world_age = value.world_age;
    view.time.time_of_day = value.time.time_of_day;
    view.time.partial_tick = value.time.partial_tick;
    view.time.rate = value.time.rate;
    view.time.paused = value.time.paused;
    view.weather.clear_weather_time = value.weather.clear_weather_time;
    view.weather.rain_time = value.weather.rain_time;
    view.weather.thunder_time = value.weather.thunder_time;
    view.weather.raining = value.weather.raining;
    view.weather.thundering = value.weather.thundering;
    view.weather.rain_level = value.rain_level;
    view.weather.old_rain_level = value.old_rain_level;
    view.weather.thunder_level = value.thunder_level;
    view.weather.old_thunder_level = value.old_thunder_level;
    view.border = value.border;
    view.spawn = value.spawn;
    view.morning = None;
    view.snapshot = None;
    view.evidence = None;
}

fn enough_sleepers<'a>(
    players: impl Iterator<Item = &'a GroundSleepView>,
    world: pumpkin_cluster::protocol::WorldId,
    tick: TickStamp,
    percentage: i64,
) -> bool {
    let mut count = 0usize;
    let mut ready = 0usize;
    for player in
        players.filter(|player| player.world == world && !player.spectator && player.health > 0.0)
    {
        count += 1;
        if player.sleeping_since.is_some_and(|since| {
            let age = tick.distance_since(since);
            age >= 100 && age < 0x8000
        }) {
            ready += 1;
        }
    }
    count > 0
        && ready
            >= count
                .saturating_mul(percentage.clamp(0, 100) as usize)
                .div_ceil(100)
                .max(1)
}

pub fn spawn(
    server: &Arc<Server>,
    mut rx: mpsc::Receiver<EnvironmentInput>,
    mut hooks: mpsc::Receiver<WeatherHook>,
) {
    if !server.advanced_config.cluster.enabled {
        return;
    }
    let hook_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut pending = None;
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            if hook_server.cluster_global_shutdown.load(std::sync::atomic::Ordering::Acquire) {
                hooks.close();
                if hooks.is_empty() && pending.is_none() { break; }
            }
            let hook = tokio::select! {
                hook = hooks.recv(), if pending.is_none() => {
                    let Some(hook) = hook else { break };
                    Some(hook)
                }
                _ = flush.tick() => None,
            };
            if let Some(hook) = hook {
                if let Some(world) = hook.world.upgrade() {
                    let allowed = if let Some(rain) = hook.rain {
                        let mut event = crate::plugin::api::events::world::weather_change::WeatherChangeEvent::new(world.clone(), rain);
                        tokio::time::timeout(std::time::Duration::from_secs(5), hook_server.plugin_manager.fire(&hook_server, &mut event)).await.is_ok() && !event.cancelled
                    } else if let Some(thunder) = hook.thunder {
                        let mut event = crate::plugin::api::events::world::weather_change::ThunderChangeEvent::new(world.clone(), thunder);
                        tokio::time::timeout(std::time::Duration::from_secs(5), hook_server.plugin_manager.fire(&hook_server, &mut event)).await.is_ok() && !event.cancelled
                    } else { false };
                    if allowed {
                        pending = Some(EnvironmentInput::WeatherFlag { world: hook.world, rain: hook.rain, thunder: hook.thunder });
                    }
                }
            }
            if let Some(input) = pending.take() {
                match hook_server.cluster_environment.tx.try_send(input) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(input)) => pending = Some(input),
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
        }
    });
    let (disk_tx, disk_rx) = mpsc::channel::<(std::path::PathBuf, EnvironmentSnapshot)>(32);
    server.spawn_task(save_configurations(disk_rx, |value| {
        pumpkin_cluster::world_time::encode_environment(&EnvironmentControl::Snapshot(
            value.clone(),
        ))
        .ok()
    }));
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut states = HashMap::<uuid::Uuid, OwnedEnvironment>::new();
        let mut outbound = VecDeque::<(Vec<pumpkin_cluster::identity::ServerId>, EnvironmentControl)>::new();
        let mut persistence = BTreeMap::<String, EnvironmentSnapshot>::new();
        let mut peers = HashMap::new();
        let mut replay = HashMap::<pumpkin_cluster::identity::ServerId, ReplayCursor>::new();
        let mut diagnostics=super::cluster_control_registers::Diagnostics::default();
        let mut revision = 0_u64;
        let mut pending_input = None;
        let mut unknown = VecDeque::new();
        let mut pending_frames = VecDeque::new();
        let mut adopted = BTreeMap::new();
        let mut final_saved = false;
        let mut shutdown_saved = false;
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            flush_environment_outbound(&task_server, &mut outbound);
            let new_worlds=task_server.worlds.load().iter().filter(|world| !states.contains_key(&world.uuid)).cloned().collect::<Vec<_>>();
            for world in &new_worlds {
                let state=state_for(&mut states,world);
                if matches!(task_server.advanced_config.cluster.role,pumpkin_config::ClusterRole::Primary) {
                    let path=world.level.level_folder.dim_folder.join("cluster_environment.dat");
                    if let Ok(bytes)=tokio::fs::read(path).await
                        && let Ok(EnvironmentControl::Snapshot(value))=pumpkin_cluster::world_time::decode_environment(&bytes)
                        && value.time.dimension==world.dimension.minecraft_name {
                        restore_environment(&mut state.view,value);
                        world.cluster_environment_view.store(Arc::new(state.view.clone()));
                        publish_config(world,state,false);
                    }
                }
            }
            let shutting_down=task_server.cluster_global_shutdown.load(std::sync::atomic::Ordering::Acquire);
            if shutting_down {
                rx.close(); replay.clear();
                let drained=rx.is_empty() && pending_input.is_none() && unknown.is_empty() && pending_frames.is_empty() && outbound.is_empty();
                if !shutdown_saved || drained && !final_saved {
                    for state in states.values() {
                        if let Some(value)=&state.view.snapshot
                            && value.issuer.server.0==task_server.advanced_config.cluster.server_id
                            && matches!(task_server.advanced_config.cluster.role,pumpkin_config::ClusterRole::Primary) {
                            persistence.insert(value.time.dimension.clone(),value.clone());
                        }
                    }
                    shutdown_saved=true;
                    if drained {final_saved=true;}
                }
                flush_persistence(&task_server,&mut persistence,&disk_tx);
                if drained && final_saved && persistence.is_empty() {break;}
            }
            if pending_input.is_none()
                && super::cluster_world_rules::primary(&task_server).is_some_and(|issuer|issuer.server.0==task_server.advanced_config.cluster.server_id)
                && let Some((_,EnvironmentControl::Request {dimension,mutation}))=outbound.front()
                && let Some(world)=task_server.worlds.load().iter().find(|world|world.dimension.minecraft_name==dimension).cloned() {
                pending_input=Some(EnvironmentInput::Time {world:Arc::downgrade(&world),mutation:mutation.clone()});
                outbound.pop_front();
            }
            let (input, flush_outbound)=if pending_input.is_none() && unknown.len()<128 && !pending_frames.is_empty() { (pending_frames.pop_front(),false) } else { tokio::select! {
                input=rx.recv(), if pending_input.is_none() && unknown.len()<128 && outbound.len()<4096 && (!rx.is_closed() || !rx.is_empty()) => (input,false),
                _=flush.tick() => (None,true),
            }};
            let input=pending_input.take().or(input).or_else(|| if flush_outbound {unknown.pop_front()} else {None});
            if let Some(EnvironmentInput::Remote {peer,generation,control:EnvironmentControl::Frames(frames)})=input {
                for value in frames { pending_frames.push_back(EnvironmentInput::Remote {peer,generation,control:EnvironmentControl::Snapshot(value)}); }
                continue;
            }
            let Some(elected)=super::cluster_world_rules::primary(&task_server) else { pending_input=input; continue; };
            let Some(current_slot)=super::cluster_world_rules::disciplined_slot() else { pending_input=input; continue; };
            let local=pumpkin_cluster::identity::ServerId(task_server.advanced_config.cluster.server_id);
            let primary=elected.server==local;
            if primary {
                let Some(cut)=authority(&task_server,elected,current_slot) else {pending_input=input;continue;};
                let worlds=task_server.worlds.load();
                if worlds.iter().any(|world| !cut.environments.contains_key(&super::cluster_world_delta::world_id(world))) {pending_input=input;continue;}
                for world in worlds.iter() {
                    let world_id=super::cluster_world_delta::world_id(world);
                    if adopted.get(&world_id)!=Some(&(elected,cut.from_slot)) {
                        let state=state_for(&mut states,world);
                        restore_environment(&mut state.view,(*cut.environments[&world_id]).clone());
                        state.last_slot=Some(cut.from_slot.saturating_sub(1));state.last_tick=None;state.morning_latched=false;
                        adopted.insert(world_id,(elected,cut.from_slot));
                    }
                }
            }
            let retry=input.clone();
            let mut mutation=None;
            let mut tick_input=None;
            match input {
                Some(EnvironmentInput::Remote {peer,generation,control}) => {
                    let known_generation=task_server.cluster_peer_generations.load().get(&peer).copied();
                    if known_generation.is_some_and(|known|known>generation) {diagnostics.rejected("environment",peer.0,"stale transport");continue;}
                    if known_generation!=Some(generation) || !task_server.cluster_connected_peers.load().contains(&peer) {unknown.push_back(retry.clone().unwrap());continue;}
                    let Some(boot)=task_server.cluster_peer_incarnations.load().get(&peer).copied().filter(|boot| *boot!=0) else {unknown.push_back(retry.clone().unwrap());continue;};
                    let source=PeerGeneration {server:peer,generation:boot};
                    match control {
                        EnvironmentControl::Config(value) => {
                            let mut history=(**task_server.cluster_world_borders.load()).clone();
                            let mut roots=retained_roots(&task_server);
                            task_server.cluster_environment_history.load().retain_required_roots(&mut roots);
                            match history.try_insert(source,elected,value,&roots) {
                                Ok(_) => task_server.cluster_world_borders.store(Arc::new(history)),
                                Err(pumpkin_cluster::world_time::EnvironmentEvidenceError::Capacity) => {diagnostics.deferred("environment",local.0,"evidence archive capacity");pending_input=retry.clone();},
                                Err(_) => diagnostics.rejected("environment",peer.0,"invalid retained evidence"),
                            }
                        }
                        EnvironmentControl::Morning {world,transition} => {
                            let mut history=(**task_server.cluster_mornings.load()).clone();
                            let mut roots=retained_roots(&task_server);
                            task_server.cluster_environment_history.load().retain_required_roots(&mut roots);
                            match history.try_insert(source,elected,world,transition,&roots) {
                                Ok(_) => task_server.cluster_mornings.store(Arc::new(history)),
                                Err(pumpkin_cluster::world_time::EnvironmentEvidenceError::Capacity) => {diagnostics.deferred("environment",local.0,"evidence archive capacity");pending_input=retry.clone();},
                                Err(_) => diagnostics.rejected("environment",peer.0,"invalid retained evidence"),
                            }
                        }
                        EnvironmentControl::Snapshot(value) => {
                            if source!=elected || value.issuer!=elected {diagnostics.rejected("environment",peer.0,"snapshot source is not elected primary");continue;}
                            if let Some(world)=task_server.worlds.load().iter().find(|world|world.dimension.minecraft_name==value.time.dimension) {
                                let state=state_for(&mut states,world);
                                let valid_view=pumpkin_cluster::world_time::accept_environment_snapshot(source,elected,state.view.snapshot.as_ref(),&value);
                                let result=if valid_view {
                                    let mut next=state.clone();
                                    restore_environment(&mut next.view,value.clone());
                                    next.view.morning=value.morning;
                                    next.last_tick=Some(TickStamp(value.slot as u16)); next.last_slot=Some(value.slot);
                                    commit_environment(&task_server,world,state,next,source,elected,value.clone())
                                } else { retain_environment(&task_server,world,source,elected,&value) };
                                match result {
                                    Ok(()) => if valid_view { state.view.time.send_time(world); },
                                    Err(pumpkin_cluster::world_time::EnvironmentEvidenceError::Capacity) => {diagnostics.deferred("environment",local.0,"evidence archive capacity");pending_input=retry.clone();},
                                    Err(_) => diagnostics.rejected("environment",peer.0,"invalid retained evidence"),
                                }
                            } else {pending_input=retry.clone();}
                        }
                        EnvironmentControl::Request {dimension,mutation:request} if !matches!(request,EnvironmentMutation::Morning{..}) => {
                            if let Some(world)=task_server.worlds.load().iter().find(|world|world.dimension.minecraft_name==dimension) { mutation=Some((world.clone(),request)); }
                        }
                        _ => diagnostics.rejected("environment",peer.0,"invalid remote control"),
                    }
                }
                Some(EnvironmentInput::Tick {world,tick,slot,rules,sleepers}) => tick_input=world.upgrade().map(|world|(world,tick,slot,rules,sleepers)),
                Some(EnvironmentInput::Time {world,mutation:request}) => mutation=world.upgrade().map(|world|(world,request)),
                Some(EnvironmentInput::Weather {world,clear,rain,raining,thundering}) => mutation=world.upgrade().map(|world|(world,EnvironmentMutation::Weather{clear,rain,raining,thundering})),
                Some(EnvironmentInput::WeatherFlag {world,rain,thunder}) => mutation=world.upgrade().map(|world|(world,EnvironmentMutation::WeatherFlag{rain,thunder})),
                Some(EnvironmentInput::Publish {world,weather:_}) => mutation=world.upgrade().map(|world|(world,EnvironmentMutation::Publish)),
                None => {},
            }
            if let Some((world,request))=mutation {
                if !primary { outbound.push_back((vec![elected.server],EnvironmentControl::Request{dimension:world.dimension.minecraft_name.to_owned(),mutation:request})); }
                else {
                    let state=state_for(&mut states,&world);
                    let mut last=state.last_slot.unwrap_or(current_slot);
                    for _ in 0..64 {
                        if last>=current_slot || outbound.len()>=4096 {break;}
                        let source_slot=last+1;
                        let Some(next_revision)=revision.checked_add(1) else {break;};
                        let Some(rules)=tick_rules_at(&task_server,super::cluster_world_delta::world_id(&world),elected,source_slot) else {break;};
                        let mut next=state.clone();
                        advance_environment(&mut next.view,rules.advance_time,rules.advance_weather);
                        next.last_slot=Some(source_slot);next.last_tick=Some(TickStamp(source_slot as u16));
                        let value=snapshot(&world,&next,elected,next_revision,source_slot);
                        match commit_environment(&task_server,&world,state,next,elected,elected,value.clone()) {
                            Ok(()) => {
                                revision=next_revision;last=source_slot;
                                if state.view.time.world_age%20==0 {persistence.insert(value.time.dimension.clone(),value.clone());}
                                outbound.push_back((task_server.cluster_connected_peers.load().iter().copied().collect(),EnvironmentControl::Snapshot(value)));
                            }
                            Err(_) => {diagnostics.deferred("environment",local.0,"clock evidence unavailable or invalid");break;},
                        }
                    }
                    if last!=current_slot || outbound.len()>=4096 {pending_input=retry.clone();}
                    else if let Some(next_revision)=revision.checked_add(1) {
                        let advance_weather=if let EnvironmentMutation::Morning {slot}= &request {
                            let Some(rules)=tick_rules_at(&task_server,super::cluster_world_delta::world_id(&world),elected,*slot) else {pending_input=retry.clone();continue;};
                            rules.advance_weather
                        } else {false};
                        let mut next=state.clone();
                        next.last_slot=Some(current_slot);next.last_tick=Some(TickStamp(current_slot as u16));
                        apply_mutation(&mut next,request,elected,next_revision,current_slot,advance_weather);
                        let value=snapshot(&world,&next,elected,next_revision,current_slot);
                        match commit_environment(&task_server,&world,state,next,elected,elected,value.clone()) {
                            Ok(()) => {
                                revision=next_revision;
                                state.view.time.send_time(&world);
                                persistence.insert(value.time.dimension.clone(),value.clone());
                                outbound.push_back((task_server.cluster_connected_peers.load().iter().copied().collect(),EnvironmentControl::Snapshot(value)));
                            }
                            Err(pumpkin_cluster::world_time::EnvironmentEvidenceError::Capacity) => {diagnostics.deferred("environment",local.0,"evidence archive capacity");pending_input=retry.clone();},
                            Err(_) => diagnostics.rejected("environment",local.0,"invalid local environment mutation"),                        }
                    }
                }
            }
            if let Some((world,tick,slot,rules,sleepers))=tick_input {
                let state=state_for(&mut states,&world);
                if primary {
                    let mut last=state.last_slot.unwrap_or(slot.saturating_sub(1));
                    for _ in 0..64 {
                        if last>=slot { break; }
                        if outbound.len()>=4096 { pending_input=retry.clone(); break; }
                        let source_slot=last+1;
                        let Some(next_revision)=revision.checked_add(1) else {break;};
                        let Some(source_rules)=tick_rules_at(&task_server,super::cluster_world_delta::world_id(&world),elected,source_slot) else {pending_input=retry.clone();break;};
                        let mut next=state.clone();
                        advance_environment(&mut next.view,source_rules.advance_time,source_rules.advance_weather);
                        let ready=source_slot==slot && enough_sleepers(sleepers.values(),super::cluster_world_delta::world_id(&world),tick,source_rules.sleeping_percentage);
                        if source_slot==slot && !ready {next.morning_latched=false;}
                        let morning=ready && !next.morning_latched && (next.view.time.is_night() || next.view.weather.thundering);
                        if morning { apply_mutation(&mut next,EnvironmentMutation::Morning{slot:source_slot},elected,next_revision,source_slot,source_rules.advance_weather); }
                        next.last_slot=Some(source_slot); next.last_tick=Some(TickStamp(source_slot as u16));
                        let value=snapshot(&world,&next,elected,next_revision,source_slot);
                        match commit_environment(&task_server,&world,state,next,elected,elected,value.clone()) {
                            Ok(()) => {
                                revision=next_revision; last=source_slot;
                                if morning || state.view.time.world_age%20==0 { persistence.insert(value.time.dimension.clone(),value.clone()); state.view.time.send_time(&world); }
                                outbound.push_back((task_server.cluster_connected_peers.load().iter().copied().collect(),EnvironmentControl::Snapshot(value)));
                            }
                            Err(pumpkin_cluster::world_time::EnvironmentEvidenceError::Capacity) => {diagnostics.deferred("environment",local.0,"evidence archive capacity");pending_input=retry.clone();break;},
                            Err(_) => {diagnostics.rejected("environment",local.0,"invalid primary environment tick");break;},
                        }
                    }
                    if last<slot && pending_input.is_none() {pending_input=retry.clone();}
                } else {
                    let ticks=state.last_tick.map_or(1,|last|tick.distance_since(last));
                    if ticks<32768 {
                        let old=state.view.weather.clone();
                        for _ in 0..ticks {advance_environment(&mut state.view,rules.advance_time,false);}
                        state.last_tick=Some(tick);
                        publish_weather(&world,&old,&state.view.weather);
                        world.cluster_environment_view.store(Arc::new(state.view.clone()));
                        if state.view.time.world_age%20==0 {state.view.time.send_time(&world);}
                    }
                }
            }
            if primary && !shutting_down {
                let connected=task_server.cluster_connected_peers.load();
                let generations=task_server.cluster_peer_generations.load();
                for peer in connected.iter() {
                    let generation=generations.get(peer).copied();
                    if peers.get(peer)!=Some(&generation) {replay.insert(*peer,ReplayCursor::Config(None));peers.insert(*peer,generation);}
                    if let Some(cursor)=replay.get_mut(peer) {fill_replay(&task_server,elected,*peer,cursor,&mut outbound);}
                }
                peers.retain(|peer,_|connected.contains(peer)); replay.retain(|peer,_|connected.contains(peer));
            }
            if flush_outbound { flush_environment_outbound(&task_server,&mut outbound); }
            if primary {flush_persistence(&task_server,&mut persistence,&disk_tx);}
        }
    });
}

fn tick_rules_at(
    server: &Server,
    world: pumpkin_cluster::protocol::WorldId,
    issuer: PeerGeneration,
    slot: u64,
) -> Option<EnvironmentTickRules> {
    let history = server.cluster_world_rules.load();
    history
        .snapshots()
        .filter(|snapshot| snapshot.rules.world == world && snapshot.rules.issuer == issuer)
        .filter(|snapshot| {
            history
                .resolve(
                    pumpkin_cluster::world_rules::WorldRulesWitness {
                        rules: snapshot.rules,
                        slot,
                    },
                    TickStamp(slot as u16),
                )
                .is_ok()
        })
        .max_by_key(|snapshot| snapshot.rules.epoch)
        .map(|snapshot| EnvironmentTickRules {
            advance_time: snapshot.advance_time,
            advance_weather: snapshot.advance_weather,
            sleeping_percentage: snapshot.players_sleeping_percentage,
        })
}

fn advance_environment(view: &mut EnvironmentView, advance_time: bool, advance_weather: bool) {
    view.time.world_age = view.time.world_age.saturating_add(1);
    if advance_time && !view.time.paused {
        view.time.partial_tick += view.time.rate;
        let full = view.time.partial_tick.floor() as i64;
        view.time.partial_tick -= full as f32;
        view.time.time_of_day = view.time.time_of_day.saturating_add(full);
    }
    view.weather.advance_weather(advance_weather);
}

fn flush_persistence(
    server: &Server,
    pending: &mut BTreeMap<String, EnvironmentSnapshot>,
    tx: &mpsc::Sender<(std::path::PathBuf, EnvironmentSnapshot)>,
) {
    if let Some((dimension, value)) = pending.first_key_value()
        && let Some(world) = server
            .worlds
            .load()
            .iter()
            .find(|world| world.dimension.minecraft_name == dimension)
        && tx
            .try_send((
                world
                    .level
                    .level_folder
                    .dim_folder
                    .join("cluster_environment.dat"),
                value.clone(),
            ))
            .is_ok()
    {
        let dimension = dimension.clone();
        pending.remove(&dimension);
    }
}

#[derive(Clone)]
enum ReplayCursor {
    Config(Option<(pumpkin_cluster::protocol::WorldId, PeerGeneration, u64)>),
    Morning(Option<(pumpkin_cluster::protocol::WorldId, PeerGeneration, u64)>),
    Frame(Option<(pumpkin_cluster::protocol::WorldId, PeerGeneration, u64, u64)>),
    Done,
}

fn fill_replay(
    server: &Server,
    issuer: PeerGeneration,
    peer: pumpkin_cluster::identity::ServerId,
    cursor: &mut ReplayCursor,
    outbound: &mut VecDeque<(Vec<pumpkin_cluster::identity::ServerId>, EnvironmentControl)>,
) {
    for _ in 0..16 {
        if outbound.len() >= 4096 {
            break;
        }
        match cursor {
            ReplayCursor::Config(after) => {
                let history = server.cluster_world_borders.load();
                if let Some(value) = history.snapshots().find(|value| {
                    value.issuer == issuer
                        && after.is_none_or(|key| (value.world, value.issuer, value.revision) > key)
                }) {
                    *after = Some((value.world, value.issuer, value.revision));
                    outbound.push_back((vec![peer], EnvironmentControl::Config(value.clone())));
                } else {
                    *cursor = ReplayCursor::Morning(None);
                }
            }
            ReplayCursor::Morning(after) => {
                let history = server.cluster_mornings.load();
                if let Some((world, value)) = history.records().find(|(world, value)| {
                    value.issuer == issuer
                        && after.is_none_or(|key| (*world, value.issuer, value.revision) > key)
                }) {
                    *after = Some((world, value.issuer, value.revision));
                    outbound.push_back((
                        vec![peer],
                        EnvironmentControl::Morning {
                            world,
                            transition: value,
                        },
                    ));
                } else {
                    *cursor = ReplayCursor::Frame(None);
                }
            }
            ReplayCursor::Frame(after) => {
                let history = server.cluster_environment_history.load();
                if let Some((world, value)) = history.records().find(|(world, value)| {
                    value.issuer == issuer
                        && after.is_none_or(|key| {
                            (*world, value.issuer, value.slot, value.revision) > key
                        })
                }) {
                    *after = Some((world, value.issuer, value.slot, value.revision));
                    outbound
                        .push_back((vec![peer], EnvironmentControl::Snapshot((**value).clone())));
                } else {
                    *cursor = ReplayCursor::Done;
                }
            }
            ReplayCursor::Done => break,
        }
    }
}

fn flush_environment_outbound(
    server: &Server,
    outbound: &mut VecDeque<(Vec<pumpkin_cluster::identity::ServerId>, EnvironmentControl)>,
) {
    let Some((peers, first)) = outbound.front() else {
        return;
    };
    let targets = if matches!(first, EnvironmentControl::Request { .. }) {
        let Some(primary) = super::cluster_world_rules::primary(server) else {
            return;
        };
        if primary.server.0 == server.advanced_config.cluster.server_id {
            return;
        }
        vec![primary.server]
    } else {
        let connected = server.cluster_connected_peers.load();
        peers
            .iter()
            .copied()
            .filter(|peer| connected.contains(peer))
            .collect()
    };
    let mut frames = Vec::new();
    if matches!(first, EnvironmentControl::Snapshot(_)) {
        for (targets, control) in outbound.iter().take(8) {
            if targets != peers {
                break;
            }
            if let EnvironmentControl::Snapshot(value) = control {
                frames.push(value.clone());
            } else {
                break;
            }
        }
    }
    let count = frames.len().max(1);
    let control = if frames.len() > 1 {
        EnvironmentControl::Frames(frames)
    } else {
        first.clone()
    };
    if let Ok(bytes) = pumpkin_cluster::world_time::encode_environment(&control)
        && server
            .cluster_global_outbound
            .try_event(server, targets, bytes)
    {
        for _ in 0..count {
            outbound.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn saved_environment() -> EnvironmentSnapshot {
        let issuer = PeerGeneration {
            server: pumpkin_cluster::identity::ServerId(1),
            generation: 9,
        };
        EnvironmentSnapshot {
            issuer,
            revision: 8,
            slot: 1000,
            world_age: 55,
            rain_level: 0.5,
            old_rain_level: 0.4,
            thunder_level: 0.25,
            old_thunder_level: 0.2,
            config_revision: 8,
            config_from_slot: 1000,
            time: TimeUpdate::new(String::from("minecraft:overworld"), 24_000, 0.5, 2.0, true),
            weather: WeatherUpdate::new(
                String::from("minecraft:overworld"),
                100,
                200,
                300,
                false,
                true,
            ),
            border: EnvironmentView::default().border,
            spawn: SpawnState {
                x: 10,
                y: 64,
                z: -20,
                yaw: 90.0,
                pitch: 10.0,
            },
            morning: Some(MorningTransition {
                issuer,
                revision: 8,
                slot: 1000,
            }),
        }
    }

    #[tokio::test]
    async fn failed_save_retains_head_and_coalesces_newer_configuration_until_drain() {
        let (tx, rx) = mpsc::channel(32);
        let (attempt_tx, mut attempts) = mpsc::channel(8);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut release = Some(release_rx);
        tx.send((std::path::PathBuf::from("configuration"), vec![1]))
            .await
            .unwrap();
        let saver = tokio::spawn(drain_configurations(
            rx,
            |bytes: &Vec<u8>| Some(bytes.clone()),
            move |_, bytes| {
                let release = release.take();
                let attempts = attempt_tx.clone();
                async move {
                    attempts.send(bytes).await.unwrap();
                    if let Some(release) = release {
                        release.await.unwrap();
                        Err(std::io::Error::other("injected fsync failure"))
                    } else {
                        Ok(())
                    }
                }
            },
        ));
        assert_eq!(attempts.recv().await.unwrap(), vec![1]);
        tx.send((std::path::PathBuf::from("configuration"), vec![2]))
            .await
            .unwrap();
        drop(tx);
        release_tx.send(()).unwrap();
        assert_eq!(attempts.recv().await.unwrap(), vec![2]);
        tokio::time::timeout(std::time::Duration::from_secs(2), saver)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn durable_replacement_restores_configuration_without_old_boot_wake() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join("dimension")
            .join("cluster_environment.dat");
        let first = saved_environment();
        let bytes = pumpkin_cluster::world_time::encode_environment(&EnvironmentControl::Snapshot(
            first.clone(),
        ))
        .unwrap();
        save_environment(&path, &bytes).await.unwrap();
        let mut final_state = first;
        final_state.revision += 1;
        final_state.spawn.x = -200;
        final_state.border.new_diameter = 500.0;
        let bytes = pumpkin_cluster::world_time::encode_environment(&EnvironmentControl::Snapshot(
            final_state.clone(),
        ))
        .unwrap();
        save_environment(&path, &bytes).await.unwrap();
        let loaded = tokio::fs::read(&path).await.unwrap();
        let EnvironmentControl::Snapshot(restored) =
            pumpkin_cluster::world_time::decode_environment(&loaded).unwrap()
        else {
            panic!()
        };
        let mut view = EnvironmentView::default();
        restore_environment(&mut view, restored);
        assert_eq!(view.spawn.x, -200);
        assert_eq!(view.weather.rain_level, 0.5);
        assert_eq!(view.weather.old_rain_level, 0.4);
        assert_eq!(view.weather.thunder_level, 0.25);
        assert_eq!(view.weather.old_thunder_level, 0.2);
        assert_eq!(view.border.new_diameter, 500.0);
        assert_eq!(view.time.time_of_day, 24_000);
        assert_eq!(view.time.partial_tick, 0.5);
        assert_eq!(view.weather.thunder_time, 300);
        assert!(view.time.paused);
        assert!(view.morning.is_none());
        assert!(view.snapshot.is_none());
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn certified_clock_and_weather_rules_control_distinct_transitions() {
        let mut view = EnvironmentView::default();
        view.time.time_of_day = 100;
        view.time.partial_tick = 0.25;
        view.time.rate = 2.0;
        view.time.paused = false;
        view.weather.raining = true;
        view.weather.rain_time = 2;
        view.weather.thunder_time = 100;
        view.weather.clear_weather_time = 0;
        advance_environment(&mut view, false, false);
        assert_eq!(view.time.world_age, 1);
        assert_eq!(view.time.time_of_day, 100);
        assert_eq!(view.weather.rain_time, 2);
        assert!(view.weather.rain_level > 0.0);
        advance_environment(&mut view, true, true);
        assert_eq!(view.time.time_of_day, 102);
        assert_eq!(view.time.partial_tick, 0.25);
        assert_eq!(view.weather.rain_time, 1);
        assert!(view.weather.raining);
        view.time.paused = true;
        advance_environment(&mut view, true, true);
        assert_eq!(view.time.world_age, 3);
        assert_eq!(view.time.time_of_day, 102);
        assert!(!view.weather.raining);
        assert_eq!(view.weather.rain_time, 0);
        assert!(view.weather.rain_level < view.weather.old_rain_level);
    }

    #[test]
    fn morning_threshold_counts_only_eligible_ground_sleepers_and_original_ticks() {
        let world = pumpkin_cluster::protocol::WorldId::new(0, 1);
        let mut players = vec![
            GroundSleepView {
                world,
                health: 20.0,
                spectator: false,
                sleeping_since: Some(TickStamp(900)),
            },
            GroundSleepView {
                world,
                health: 20.0,
                spectator: false,
                sleeping_since: Some(TickStamp(901)),
            },
            GroundSleepView {
                world,
                health: 20.0,
                spectator: true,
                sleeping_since: None,
            },
            GroundSleepView {
                world,
                health: 0.0,
                spectator: false,
                sleeping_since: None,
            },
        ];
        assert!(enough_sleepers(players.iter(), world, TickStamp(1000), 50));
        assert!(!enough_sleepers(
            players.iter(),
            world,
            TickStamp(1000),
            100
        ));
        assert!(enough_sleepers(players.iter(), world, TickStamp(1001), 100));
        players[0].sleeping_since = Some(TickStamp(1002));
        assert!(!enough_sleepers(
            players.iter(),
            world,
            TickStamp(1001),
            100
        ));
        assert!(!enough_sleepers(
            std::iter::empty(),
            world,
            TickStamp(1000),
            0
        ));
        players.truncate(1);
        players[0].sleeping_since = Some(TickStamp(65_435));
        assert!(enough_sleepers(players.iter(), world, TickStamp(10), 100));
    }
    #[test]
    fn morning_source_advances_once_when_weather_cycle_is_disabled() {
        let issuer = PeerGeneration {
            server: pumpkin_cluster::identity::ServerId(1),
            generation: 9,
        };
        let mut state = OwnedEnvironment {
            view: EnvironmentView::default(),
            last_tick: None,
            last_slot: None,
            morning_latched: false,
        };
        state.view.time.time_of_day = 13_000;
        state.view.weather.thundering = true;
        apply_mutation(
            &mut state,
            EnvironmentMutation::Morning { slot: 1000 },
            issuer,
            1,
            1000,
            false,
        );
        assert_eq!(state.view.time.time_of_day, 24_000);
        assert_eq!(
            state.view.morning,
            Some(MorningTransition {
                issuer,
                revision: 1,
                slot: 1000
            })
        );
        assert!(state.view.weather.thundering);
        advance_environment(&mut state.view, true, false);
        apply_mutation(
            &mut state,
            EnvironmentMutation::Morning { slot: 1001 },
            issuer,
            2,
            1001,
            false,
        );
        assert_eq!(state.view.time.time_of_day, 24_001);
        assert_eq!(state.view.morning.unwrap().revision, 1);
    }
}
