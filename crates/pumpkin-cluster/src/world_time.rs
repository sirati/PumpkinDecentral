use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::primary::{AcceptedTick, PrimarySaveHandle};
use crate::protocol::StreamKind;
use crate::time::TickStamp;

pub const WORLD_TIME_MAX_DIMENSION_LEN: usize = 64;
pub const WORLD_TIME_MIN_RATE: f32 = 1.0e-5;
pub const WORLD_TIME_MAX_RATE: f32 = 1000.0;

#[derive(Debug, Clone, PartialEq)]
pub struct WorldTimeError {
    pub message: String,
}

impl core::fmt::Display for WorldTimeError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WorldTimeError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeUpdate {
    pub dimension: String,
    pub time_of_day: i64,
    pub partial_tick: f32,
    pub rate: f32,
    pub paused: bool,
}

impl TimeUpdate {
    #[must_use]
    pub fn new(
        dimension: String,
        time_of_day: i64,
        partial_tick: f32,
        rate: f32,
        paused: bool,
    ) -> Self {
        Self {
            dimension,
            time_of_day,
            partial_tick,
            rate,
            paused,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeatherUpdate {
    pub dimension: String,
    pub clear_weather_time: i32,
    pub rain_time: i32,
    pub thunder_time: i32,
    pub raining: bool,
    pub thundering: bool,
}

impl WeatherUpdate {
    #[must_use]
    pub fn new(
        dimension: String,
        clear_weather_time: i32,
        rain_time: i32,
        thunder_time: i32,
        raining: bool,
        thundering: bool,
    ) -> Self {
        Self {
            dimension,
            clear_weather_time,
            rain_time,
            thunder_time,
            raining,
            thundering,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorldTimeControl {
    Time(TimeUpdate),
    Weather(WeatherUpdate),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MorningTransition {
    pub issuer: crate::realm_completion::PeerGeneration,
    pub revision: u64,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BorderState {
    pub center_x: f64,
    pub center_z: f64,
    pub old_diameter: f64,
    pub new_diameter: f64,
    pub speed: i64,
    pub lerp_start_slot: u64,
    pub warning_blocks: i32,
    pub warning_time: i32,
    pub damage_per_block: f32,
    pub buffer: f32,
}

impl BorderState {
    pub fn current_diameter(&self, slot: u64) -> f64 {
        let elapsed = slot.saturating_sub(self.lerp_start_slot).saturating_mul(50);
        if self.speed <= 0 || elapsed >= self.speed as u64 {
            return self.new_diameter;
        }
        self.old_diameter
            + (self.new_diameter - self.old_diameter) * (elapsed as f64 / self.speed as f64)
    }

    pub fn remaining_millis(&self, slot: u64) -> i64 {
        self.speed
            .saturating_sub(
                i64::try_from(slot.saturating_sub(self.lerp_start_slot).saturating_mul(50))
                    .unwrap_or(i64::MAX),
            )
            .max(0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnState {
    pub x: i32,
    pub y: i32,
    pub z: i32,
    pub yaw: f32,
    pub pitch: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BorderMutation {
    Center(f64, f64),
    Diameter(f64, Option<i64>),
    Add(f64, Option<i64>),
    WarningDistance(i32),
    WarningTime(i32),
    Damage(f32),
    Buffer(f32),
    Reset,
}

pub fn valid_border(value: &BorderState) -> bool {
    value.center_x.is_finite()
        && value.center_z.is_finite()
        && value.center_x.abs() <= 29_999_984.0
        && value.center_z.abs() <= 29_999_984.0
        && value.old_diameter.is_finite()
        && value.new_diameter.is_finite()
        && (0.0..=60_000_000.0).contains(&value.old_diameter)
        && (0.0..=60_000_000.0).contains(&value.new_diameter)
        && value.speed >= 0
        && value.warning_blocks >= 0
        && value.warning_time >= 0
        && value.damage_per_block.is_finite()
        && value.damage_per_block >= 0.0
        && value.buffer.is_finite()
        && value.buffer >= 0.0
}

pub fn valid_border_mutation(value: &BorderMutation) -> bool {
    match value {
        BorderMutation::Center(x, z) => {
            x.is_finite() && z.is_finite() && x.abs() <= 29_999_984.0 && z.abs() <= 29_999_984.0
        }
        BorderMutation::Diameter(value, speed) => {
            value.is_finite()
                && (0.0..=60_000_000.0).contains(value)
                && speed.is_none_or(|speed| speed >= 0)
        }
        BorderMutation::Add(value, speed) => {
            value.is_finite() && speed.is_none_or(|speed| speed >= 0)
        }
        BorderMutation::WarningDistance(value) | BorderMutation::WarningTime(value) => *value >= 0,
        BorderMutation::Damage(value) | BorderMutation::Buffer(value) => {
            value.is_finite() && *value >= 0.0
        }
        BorderMutation::Reset => true,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldBorderWitness {
    pub world: crate::protocol::WorldId,
    pub issuer: crate::realm_completion::PeerGeneration,
    pub revision: u64,
    pub slot: u64,
    pub center: [u64; 2],
    pub diameter: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorldBorderSnapshot {
    pub world: crate::protocol::WorldId,
    pub issuer: crate::realm_completion::PeerGeneration,
    pub revision: u64,
    pub from_slot: u64,
    pub border: BorderState,
    pub spawn: SpawnState,
}

impl WorldBorderSnapshot {
    pub fn witness(&self, slot: u64) -> WorldBorderWitness {
        WorldBorderWitness {
            world: self.world,
            issuer: self.issuer,
            revision: self.revision,
            slot,
            center: [
                self.border.center_x.to_bits(),
                self.border.center_z.to_bits(),
            ],
            diameter: self.border.current_diameter(slot).to_bits(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorldBorderHistory {
    capacity: usize,
    revision: u64,
    snapshots: std::collections::BTreeMap<
        (
            crate::protocol::WorldId,
            crate::realm_completion::PeerGeneration,
            u64,
        ),
        WorldBorderSnapshot,
    >,
}

impl Default for WorldBorderHistory {
    fn default() -> Self {
        Self::new(4096)
    }
}

impl WorldBorderHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            revision: 0,
            snapshots: Default::default(),
        }
    }

    pub fn try_insert(
        &mut self,
        source: crate::realm_completion::PeerGeneration,
        elected: crate::realm_completion::PeerGeneration,
        snapshot: WorldBorderSnapshot,
        roots: &EnvironmentRoots,
    ) -> Result<bool, EnvironmentEvidenceError> {
        if source != elected
            || snapshot.issuer != elected
            || snapshot.revision == 0
            || !valid_border(&snapshot.border)
            || !snapshot.spawn.yaw.is_finite()
            || !snapshot.spawn.pitch.is_finite()
        {
            return Err(EnvironmentEvidenceError::Invalid);
        }
        let key = (snapshot.world, snapshot.issuer, snapshot.revision);
        if let Some(previous) = self.snapshots.get(&key) {
            return if previous == &snapshot {
                Ok(true)
            } else {
                Err(EnvironmentEvidenceError::Invalid)
            };
        }
        if self
            .snapshots
            .values()
            .filter(|old| old.world == snapshot.world && old.issuer == snapshot.issuer)
            .any(|old| {
                (old.revision < snapshot.revision && old.from_slot > snapshot.from_slot)
                    || (old.revision > snapshot.revision && old.from_slot < snapshot.from_slot)
            })
        {
            return Err(EnvironmentEvidenceError::Invalid);
        }
        if !self.snapshots.contains_key(&key) && self.snapshots.len() >= self.capacity {
            let removable = self.snapshots.keys().copied().find(|key| {
                !roots.borders.contains(key)
                    && roots
                        .published_through_slot
                        .is_some_and(|barrier| self.snapshots[key].from_slot < barrier)
                    && (roots.retirements.covers(
                        EnvironmentDomain::Borders,
                        key.0,
                        key.1,
                        self.snapshots[key].from_slot,
                    ) || self
                        .snapshots
                        .keys()
                        .any(|other| other.0 == key.0 && other.1 == key.1 && other.2 > key.2))
            });
            if let Some(key) = removable {
                self.snapshots.remove(&key);
            } else {
                return Err(EnvironmentEvidenceError::Capacity);
            }
        }
        if self.insert(source, elected, snapshot) {
            Ok(true)
        } else {
            Err(EnvironmentEvidenceError::Invalid)
        }
    }

    pub fn insert(
        &mut self,
        source: crate::realm_completion::PeerGeneration,
        elected: crate::realm_completion::PeerGeneration,
        snapshot: WorldBorderSnapshot,
    ) -> bool {
        if source != elected
            || snapshot.issuer != elected
            || snapshot.revision == 0
            || !valid_border(&snapshot.border)
            || !snapshot.spawn.yaw.is_finite()
            || !snapshot.spawn.pitch.is_finite()
        {
            return false;
        }
        let key = (snapshot.world, snapshot.issuer, snapshot.revision);
        if self.snapshots.len() >= self.capacity && !self.snapshots.contains_key(&key) {
            return false;
        }
        if let Some(previous) = self.snapshots.get(&key) {
            return previous == &snapshot;
        }
        if self
            .snapshots
            .values()
            .filter(|old| old.world == snapshot.world && old.issuer == snapshot.issuer)
            .any(|old| {
                (old.revision < snapshot.revision && old.from_slot > snapshot.from_slot)
                    || (old.revision > snapshot.revision && old.from_slot < snapshot.from_slot)
            })
        {
            return false;
        }
        self.snapshots.insert(key, snapshot);
        self.revision = self.revision.saturating_add(1);
        true
    }

    pub fn contains(&self, witness: WorldBorderWitness) -> bool {
        self.snapshots
            .contains_key(&(witness.world, witness.issuer, witness.revision))
    }

    pub fn get(
        &self,
        world: crate::protocol::WorldId,
        issuer: crate::realm_completion::PeerGeneration,
        revision: u64,
    ) -> Option<&WorldBorderSnapshot> {
        self.snapshots.get(&(world, issuer, revision))
    }

    pub fn resolve(
        &self,
        witness: WorldBorderWitness,
        tick: TickStamp,
    ) -> Option<&WorldBorderSnapshot> {
        if witness.slot as u16 != tick.0 {
            return None;
        }
        let snapshot = self
            .snapshots
            .get(&(witness.world, witness.issuer, witness.revision))?;
        if snapshot.from_slot > witness.slot || snapshot.witness(witness.slot) != witness {
            return None;
        }
        if self.snapshots.values().any(|next| {
            next.world == witness.world
                && next.issuer == witness.issuer
                && next.revision > witness.revision
                && next.from_slot <= witness.slot
        }) {
            return None;
        }
        Some(snapshot)
    }

    pub fn current(
        &self,
        world: crate::protocol::WorldId,
        issuer: crate::realm_completion::PeerGeneration,
        slot: u64,
    ) -> Option<&WorldBorderSnapshot> {
        self.snapshots
            .values()
            .filter(|value| {
                value.world == world && value.issuer == issuer && value.from_slot <= slot
            })
            .max_by_key(|value| value.revision)
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn snapshots(&self) -> impl Iterator<Item = &WorldBorderSnapshot> {
        self.snapshots.values()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EnvironmentDomain {
    Frames,
    Borders,
    Mornings,
}

#[derive(Debug, Clone, Default)]
pub struct EnvironmentRetirements {
    completed: std::collections::BTreeMap<
        (
            EnvironmentDomain,
            crate::protocol::WorldId,
            crate::realm_completion::PeerGeneration,
        ),
        u64,
    >,
}

impl EnvironmentRetirements {
    pub fn try_insert(
        &mut self,
        domain: EnvironmentDomain,
        world: crate::protocol::WorldId,
        issuer: crate::realm_completion::PeerGeneration,
        through_slot: u64,
    ) -> bool {
        let key = (domain, world, issuer);
        if issuer.generation == 0
            || self.completed.len() >= 16384 && !self.completed.contains_key(&key)
        {
            return false;
        }
        self.completed
            .entry(key)
            .and_modify(|slot| *slot = (*slot).max(through_slot))
            .or_insert(through_slot);
        true
    }

    fn covers(
        &self,
        domain: EnvironmentDomain,
        world: crate::protocol::WorldId,
        issuer: crate::realm_completion::PeerGeneration,
        slot: u64,
    ) -> bool {
        self.completed
            .get(&(domain, world, issuer))
            .is_some_and(|completed| slot <= *completed)
    }
}

#[derive(Debug, Clone, Default)]
pub struct EnvironmentRoots {
    pub retirements: EnvironmentRetirements,
    pub published_through_slot: Option<u64>,
    pub source_slots: std::collections::BTreeSet<u64>,
    pub borders: std::collections::BTreeSet<(
        crate::protocol::WorldId,
        crate::realm_completion::PeerGeneration,
        u64,
    )>,
    pub mornings: std::collections::BTreeSet<(
        crate::protocol::WorldId,
        crate::realm_completion::PeerGeneration,
        u64,
    )>,
}

impl EnvironmentRoots {
    pub fn pin_source(&mut self, slot: u64) {
        self.source_slots.insert(slot);
    }
    pub fn pin_border(&mut self, witness: WorldBorderWitness) {
        self.source_slots.insert(witness.slot);
        self.borders
            .insert((witness.world, witness.issuer, witness.revision));
    }
    pub fn pin_morning(&mut self, world: crate::protocol::WorldId, morning: MorningTransition) {
        self.mornings
            .insert((world, morning.issuer, morning.revision));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentEvidenceError {
    Invalid,
    Capacity,
}

#[derive(Debug, Clone)]
pub struct WorldMorningHistory {
    records: std::collections::BTreeMap<
        (
            crate::protocol::WorldId,
            crate::realm_completion::PeerGeneration,
            u64,
        ),
        MorningTransition,
    >,
    capacity: usize,
    revision: u64,
}

impl Default for WorldMorningHistory {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl WorldMorningHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            records: Default::default(),
            capacity,
            revision: 0,
        }
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn contains(&self, world: crate::protocol::WorldId, morning: MorningTransition) -> bool {
        self.records
            .contains_key(&(world, morning.issuer, morning.revision))
    }
    pub fn resolve(&self, world: crate::protocol::WorldId, morning: MorningTransition) -> bool {
        self.records.get(&(world, morning.issuer, morning.revision)) == Some(&morning)
    }
    pub fn records(
        &self,
    ) -> impl Iterator<Item = (crate::protocol::WorldId, MorningTransition)> + '_ {
        self.records
            .iter()
            .map(|((world, _, _), morning)| (*world, *morning))
    }
    pub fn try_insert(
        &mut self,
        source: crate::realm_completion::PeerGeneration,
        elected: crate::realm_completion::PeerGeneration,
        world: crate::protocol::WorldId,
        morning: MorningTransition,
        roots: &EnvironmentRoots,
    ) -> Result<bool, EnvironmentEvidenceError> {
        if source != elected
            || morning.issuer != elected
            || morning.issuer.generation == 0
            || morning.revision == 0
        {
            return Err(EnvironmentEvidenceError::Invalid);
        }
        let key = (world, morning.issuer, morning.revision);
        if let Some(old) = self.records.get(&key) {
            return if *old == morning {
                Ok(false)
            } else {
                Err(EnvironmentEvidenceError::Invalid)
            };
        }
        if self
            .records
            .iter()
            .filter(|((record_world, issuer, _), _)| {
                *record_world == world && *issuer == morning.issuer
            })
            .any(|(_, old)| {
                (old.revision < morning.revision && old.slot > morning.slot)
                    || (old.revision > morning.revision && old.slot < morning.slot)
            })
        {
            return Err(EnvironmentEvidenceError::Invalid);
        }
        if self.records.len() >= self.capacity {
            let removable = self.records.keys().copied().find(|key| {
                !roots.mornings.contains(key)
                    && roots
                        .published_through_slot
                        .is_some_and(|barrier| self.records[key].slot < barrier)
                    && (roots.retirements.covers(
                        EnvironmentDomain::Mornings,
                        key.0,
                        key.1,
                        self.records[key].slot,
                    ) || self
                        .records
                        .keys()
                        .any(|other| other.0 == key.0 && other.1 == key.1 && other.2 > key.2))
            });
            if let Some(key) = removable {
                self.records.remove(&key);
            } else {
                return Err(EnvironmentEvidenceError::Capacity);
            }
        }
        self.records.insert(key, morning);
        self.revision = self.revision.saturating_add(1);
        Ok(true)
    }
}

#[derive(Debug)]
struct EnvironmentRecord {
    snapshot: std::sync::Arc<EnvironmentSnapshot>,
    pin: std::sync::Arc<()>,
}

#[derive(Debug, Clone)]
pub struct PinnedEnvironment {
    pub snapshot: std::sync::Arc<EnvironmentSnapshot>,
    _pin: std::sync::Arc<()>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentHistory {
    records: std::collections::BTreeMap<
        (
            crate::protocol::WorldId,
            crate::realm_completion::PeerGeneration,
            u64,
            u64,
        ),
        std::sync::Arc<EnvironmentRecord>,
    >,
    capacity: usize,
    revision: u64,
}

impl Default for EnvironmentHistory {
    fn default() -> Self {
        Self::new(8192)
    }
}

impl EnvironmentHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            records: Default::default(),
            capacity,
            revision: 0,
        }
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn contains_slot(&self, slot: u64) -> bool {
        self.records.keys().any(|key| key.2 == slot)
    }
    pub fn get(
        &self,
        world: crate::protocol::WorldId,
        issuer: crate::realm_completion::PeerGeneration,
        slot: u64,
    ) -> Option<PinnedEnvironment> {
        self.records
            .range((world, issuer, slot, 0)..=(world, issuer, slot, u64::MAX))
            .next_back()
            .map(|(_, record)| PinnedEnvironment {
                snapshot: record.snapshot.clone(),
                _pin: record.pin.clone(),
            })
    }
    pub fn records(
        &self,
    ) -> impl Iterator<
        Item = (
            crate::protocol::WorldId,
            &std::sync::Arc<EnvironmentSnapshot>,
        ),
    > {
        self.records
            .iter()
            .map(|((world, _, _, _), value)| (*world, &value.snapshot))
    }
    pub fn retain_required_roots(&self, roots: &mut EnvironmentRoots) {
        let required = self
            .records
            .iter()
            .filter(|(key, record)| {
                roots.source_slots.contains(&key.2) || std::sync::Arc::strong_count(&record.pin) > 1
            })
            .map(|((world, _, _, _), record)| (*world, record.snapshot.clone()))
            .collect::<Vec<_>>();
        for (world, snapshot) in required {
            roots
                .borders
                .insert((world, snapshot.issuer, snapshot.config_revision));
            if let Some(morning) = snapshot.morning {
                roots.pin_morning(world, morning);
            }
        }
    }
    pub fn try_insert(
        &mut self,
        source: crate::realm_completion::PeerGeneration,
        elected: crate::realm_completion::PeerGeneration,
        world: crate::protocol::WorldId,
        snapshot: std::sync::Arc<EnvironmentSnapshot>,
        roots: &EnvironmentRoots,
    ) -> Result<bool, EnvironmentEvidenceError> {
        if source != elected || snapshot.issuer != elected || !valid_environment_snapshot(&snapshot)
        {
            return Err(EnvironmentEvidenceError::Invalid);
        }
        let key = (world, snapshot.issuer, snapshot.slot, snapshot.revision);
        if self
            .records
            .range((world, snapshot.issuer, 0, 0)..=(world, snapshot.issuer, u64::MAX, u64::MAX))
            .any(|(_, record)| {
                let old = &record.snapshot;
                (old.revision == snapshot.revision && **old != *snapshot)
                    || (old.revision < snapshot.revision && old.slot > snapshot.slot)
                    || (old.revision > snapshot.revision && old.slot < snapshot.slot)
            })
        {
            return Err(EnvironmentEvidenceError::Invalid);
        }
        if let Some(old) = self.records.get(&key) {
            if snapshot.revision < old.snapshot.revision {
                return Ok(false);
            }
            if snapshot.revision == old.snapshot.revision {
                return if *old.snapshot == *snapshot {
                    Ok(false)
                } else {
                    Err(EnvironmentEvidenceError::Invalid)
                };
            }
        } else if self.records.len() >= self.capacity {
            let removable =
                self.records
                    .iter()
                    .find(|(key, value)| {
                        !roots.source_slots.contains(&key.2)
                            && roots
                                .published_through_slot
                                .is_some_and(|barrier| key.2 < barrier)
                            && std::sync::Arc::strong_count(&value.pin) == 1
                            && (roots.retirements.covers(
                                EnvironmentDomain::Frames,
                                key.0,
                                key.1,
                                key.2,
                            ) || self.records.keys().any(|other| {
                                other.0 == key.0 && other.1 == key.1 && other.2 > key.2
                            }))
                    })
                    .map(|(key, _)| *key);
            if let Some(key) = removable {
                self.records.remove(&key);
            } else {
                return Err(EnvironmentEvidenceError::Capacity);
            }
        }
        self.records.insert(
            key,
            std::sync::Arc::new(EnvironmentRecord {
                snapshot,
                pin: std::sync::Arc::new(()),
            }),
        );
        self.revision = self.revision.saturating_add(1);
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentSnapshot {
    pub issuer: crate::realm_completion::PeerGeneration,
    pub revision: u64,
    pub slot: u64,
    pub world_age: i64,
    pub config_revision: u64,
    pub config_from_slot: u64,
    pub rain_level: f32,
    pub old_rain_level: f32,
    pub thunder_level: f32,
    pub old_thunder_level: f32,
    pub border: BorderState,
    pub spawn: SpawnState,
    pub time: TimeUpdate,
    pub weather: WeatherUpdate,
    pub morning: Option<MorningTransition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EnvironmentMutation {
    Border(BorderMutation),
    Spawn(SpawnState),
    Set(i64),
    Add(i64),
    Pause(bool),
    Rate(f32),
    Morning {
        slot: u64,
    },
    Weather {
        clear: i32,
        rain: i32,
        raining: bool,
        thundering: bool,
    },
    WeatherFlag {
        rain: Option<bool>,
        thunder: Option<bool>,
    },
    Publish,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EnvironmentControl {
    Frames(Vec<EnvironmentSnapshot>),
    Morning {
        world: crate::protocol::WorldId,
        transition: MorningTransition,
    },
    Config(WorldBorderSnapshot),
    Request {
        dimension: String,
        mutation: EnvironmentMutation,
    },
    Snapshot(EnvironmentSnapshot),
}

pub const ENVIRONMENT_PREFIX: &[u8; 8] = b"EnvSnap1";

pub fn encode_environment(control: &EnvironmentControl) -> Result<Vec<u8>, WorldTimeError> {
    let mut bytes = ENVIRONMENT_PREFIX.to_vec();
    bytes.extend(
        postcard::to_allocvec(control).map_err(|error| WorldTimeError {
            message: error.to_string(),
        })?,
    );
    Ok(bytes)
}

pub fn valid_environment_mutation(mutation: &EnvironmentMutation) -> bool {
    match mutation {
        EnvironmentMutation::Border(value) => valid_border_mutation(value),
        EnvironmentMutation::Spawn(value) => value.yaw.is_finite() && value.pitch.is_finite(),
        EnvironmentMutation::Rate(rate) => is_valid_rate(*rate),
        EnvironmentMutation::Weather { clear, rain, .. } => *clear >= 0 && *rain >= 0,
        _ => true,
    }
}

pub fn valid_environment_snapshot(value: &EnvironmentSnapshot) -> bool {
    value.revision > 0
        && value.config_revision > 0
        && value.config_revision <= value.revision
        && value.config_from_slot <= value.slot
        && value.issuer.generation > 0
        && valid_border(&value.border)
        && value.spawn.yaw.is_finite()
        && value.spawn.pitch.is_finite()
        && is_valid_time_update(&value.time)
        && is_valid_weather_update(&value.weather)
        && value.time.dimension == value.weather.dimension
        && value.morning.is_none_or(|morning| {
            morning.issuer == value.issuer
                && morning.revision > 0
                && morning.revision <= value.revision
                && morning.slot <= value.slot
        })
        && [
            value.rain_level,
            value.old_rain_level,
            value.thunder_level,
            value.old_thunder_level,
        ]
        .iter()
        .all(|level| level.is_finite() && (0.0..=1.0).contains(level))
}

pub fn decode_environment(bytes: &[u8]) -> Result<EnvironmentControl, WorldTimeError> {
    let payload = bytes
        .strip_prefix(ENVIRONMENT_PREFIX)
        .filter(|bytes| bytes.len() <= 4096)
        .ok_or_else(|| WorldTimeError {
            message: String::from("invalid environment frame"),
        })?;
    let control: EnvironmentControl =
        postcard::from_bytes(payload).map_err(|error| WorldTimeError {
            message: error.to_string(),
        })?;
    let valid = match &control {
        EnvironmentControl::Request {
            dimension,
            mutation,
        } => is_valid_dimension(dimension) && valid_environment_mutation(mutation),
        EnvironmentControl::Config(value) => {
            value.revision > 0
                && value.issuer.generation > 0
                && valid_border(&value.border)
                && value.spawn.yaw.is_finite()
                && value.spawn.pitch.is_finite()
        }
        EnvironmentControl::Snapshot(value) => valid_environment_snapshot(value),
        EnvironmentControl::Frames(values) => {
            !values.is_empty() && values.len() <= 8 && values.iter().all(valid_environment_snapshot)
        }
        EnvironmentControl::Morning {
            world: _,
            transition,
        } => transition.issuer.generation > 0 && transition.revision > 0,
    };
    if valid {
        Ok(control)
    } else {
        Err(WorldTimeError {
            message: String::from("invalid environment state"),
        })
    }
}

pub fn accept_environment_snapshot(
    source: crate::realm_completion::PeerGeneration,
    elected: crate::realm_completion::PeerGeneration,
    previous: Option<&EnvironmentSnapshot>,
    incoming: &EnvironmentSnapshot,
) -> bool {
    source == elected
        && incoming.issuer == elected
        && incoming.revision > 0
        && previous.is_none_or(|old| {
            old.issuer != elected || (incoming.revision > old.revision && incoming.slot >= old.slot)
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldTimeParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

impl WorldTimeParcel {
    #[must_use]
    pub fn new(peer: u16, kind: StreamKind, bytes: Vec<u8>) -> Self {
        Self { peer, kind, bytes }
    }
}

#[must_use]
pub const fn world_time_control_kind() -> StreamKind {
    StreamKind::Control
}

#[must_use]
pub fn is_valid_dimension(dimension: &str) -> bool {
    !dimension.is_empty()
        && dimension.len() <= WORLD_TIME_MAX_DIMENSION_LEN
        && dimension.contains(':')
}

#[must_use]
pub fn is_valid_rate(rate: f32) -> bool {
    rate.is_finite() && rate >= WORLD_TIME_MIN_RATE && rate <= WORLD_TIME_MAX_RATE
}

#[must_use]
pub fn is_valid_partial_tick(partial_tick: f32) -> bool {
    partial_tick.is_finite() && (0.0..1.0).contains(&partial_tick)
}

#[must_use]
pub fn is_valid_time_update(update: &TimeUpdate) -> bool {
    is_valid_dimension(&update.dimension)
        && is_valid_rate(update.rate)
        && is_valid_partial_tick(update.partial_tick)
}

#[must_use]
pub fn is_valid_weather_update(update: &WeatherUpdate) -> bool {
    is_valid_dimension(&update.dimension)
        && update.clear_weather_time >= 0
        && update.rain_time >= 0
        && update.thunder_time >= 0
}

#[must_use]
pub fn is_valid_control(control: &WorldTimeControl) -> bool {
    match control {
        WorldTimeControl::Time(update) => is_valid_time_update(update),
        WorldTimeControl::Weather(update) => is_valid_weather_update(update),
    }
}

pub fn encode_control(control: &WorldTimeControl) -> Result<Vec<u8>, WorldTimeError> {
    postcard::to_allocvec(control).map_err(|error| WorldTimeError {
        message: format!("encode world time control: {error}"),
    })
}

pub fn decode_control(bytes: &[u8]) -> Result<WorldTimeControl, WorldTimeError> {
    let control: WorldTimeControl =
        postcard::from_bytes(bytes).map_err(|error| WorldTimeError {
            message: format!("decode world time control: {error}"),
        })?;
    if is_valid_control(&control) {
        Ok(control)
    } else {
        Err(WorldTimeError {
            message: String::from("world time control failed validation"),
        })
    }
}

pub fn control_parcels_for_peers(
    control: &WorldTimeControl,
    peers: &[u16],
) -> Result<Vec<WorldTimeParcel>, WorldTimeError> {
    let bytes = encode_control(control)?;
    Ok(peers
        .iter()
        .map(|peer| WorldTimeParcel::new(*peer, world_time_control_kind(), bytes.clone()))
        .collect())
}

pub fn time_parcels_for_peers(
    update: &TimeUpdate,
    peers: &[u16],
) -> Result<Vec<WorldTimeParcel>, WorldTimeError> {
    control_parcels_for_peers(&WorldTimeControl::Time(update.clone()), peers)
}

pub fn weather_parcels_for_peers(
    update: &WeatherUpdate,
    peers: &[u16],
) -> Result<Vec<WorldTimeParcel>, WorldTimeError> {
    control_parcels_for_peers(&WorldTimeControl::Weather(update.clone()), peers)
}

pub fn submit_world_time_control(
    handle: &PrimarySaveHandle,
    tick: TickStamp,
    control: &WorldTimeControl,
) -> Result<(), WorldTimeError> {
    let payload = encode_control(control)?;
    handle
        .try_submit(tick, payload)
        .map_err(|_| WorldTimeError {
            message: String::from("primary world time queue full"),
        })
}

pub fn try_submit_world_time_control(
    tx: &mpsc::Sender<AcceptedTick>,
    tick: TickStamp,
    control: &WorldTimeControl,
) -> Result<(), WorldTimeError> {
    let payload = encode_control(control)?;
    crate::primary::try_submit(tx, tick, payload).map_err(|_| WorldTimeError {
        message: String::from("primary world time queue full"),
    })
}

pub fn decode_primary_tick(tick: &AcceptedTick) -> Result<WorldTimeControl, WorldTimeError> {
    decode_control(&tick.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time_update() -> TimeUpdate {
        TimeUpdate::new(String::from("minecraft:overworld"), 1000, 0.0, 1.0, false)
    }

    fn weather_update() -> WeatherUpdate {
        WeatherUpdate::new(
            String::from("minecraft:overworld"),
            12_000,
            0,
            0,
            false,
            false,
        )
    }

    #[test]
    fn roundtrips() {
        let control = WorldTimeControl::Time(time_update());
        let bytes = encode_control(&control).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), control);
        let control = WorldTimeControl::Weather(weather_update());
        let bytes = encode_control(&control).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), control);
        assert_eq!(world_time_control_kind(), StreamKind::Control);
    }

    #[test]
    fn rejects_invalid_dimensions() {
        assert!(is_valid_dimension("minecraft:overworld"));
        assert!(!is_valid_dimension(""));
        assert!(!is_valid_dimension("overworld"));
        let mut bad = time_update();
        bad.dimension = String::from("nocolon");
        assert!(!is_valid_time_update(&bad));
        assert!(decode_control(&encode_control(&WorldTimeControl::Time(bad)).unwrap()).is_err());
    }

    #[test]
    fn rejects_invalid_rates_and_partials() {
        let mut bad = time_update();
        bad.rate = f32::NAN;
        assert!(!is_valid_time_update(&bad));
        bad.rate = 0.0;
        assert!(!is_valid_time_update(&bad));
        bad.rate = 1.0;
        bad.partial_tick = 1.0;
        assert!(!is_valid_time_update(&bad));
        bad.partial_tick = -0.5;
        assert!(!is_valid_time_update(&bad));
        let mut bad_weather = weather_update();
        bad_weather.clear_weather_time = -5;
        assert!(!is_valid_weather_update(&bad_weather));
    }

    #[test]
    fn rejects_garbage_bytes() {
        assert!(decode_control(&[]).is_err());
        assert!(decode_control(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn parcels_target_every_peer() {
        let parcels = time_parcels_for_peers(&time_update(), &[2, 3]).unwrap();
        assert_eq!(parcels.len(), 2);
        assert_eq!(parcels[0].peer, 2);
        assert_eq!(parcels[1].peer, 3);
        assert!(
            parcels
                .iter()
                .all(|parcel| parcel.kind == StreamKind::Control)
        );
        for parcel in &parcels {
            assert_eq!(
                decode_control(&parcel.bytes).unwrap(),
                WorldTimeControl::Time(time_update())
            );
        }
        let parcels = weather_parcels_for_peers(&weather_update(), &[5]).unwrap();
        assert_eq!(parcels.len(), 1);
        assert_eq!(
            decode_control(&parcels[0].bytes).unwrap(),
            WorldTimeControl::Weather(weather_update())
        );
    }

    #[test]
    fn primary_tick_roundtrips() {
        let (handle, _inbox) = crate::primary::primary_save_channel(8);
        let control = WorldTimeControl::Time(time_update());
        submit_world_time_control(&handle, TickStamp(9), &control).unwrap();
        let tick = AcceptedTick {
            tick: TickStamp(9),
            payload: encode_control(&control).unwrap(),
        };
        assert_eq!(decode_primary_tick(&tick).unwrap(), control);
    }

    #[tokio::test]
    async fn try_submit_roundtrips_without_locks() {
        let (tx, mut rx) = mpsc::channel(4);
        let control = WorldTimeControl::Weather(weather_update());
        try_submit_world_time_control(&tx, TickStamp(4), &control).unwrap();
        let tick = rx.recv().await.unwrap();
        assert_eq!(decode_primary_tick(&tick).unwrap(), control);
    }
}
