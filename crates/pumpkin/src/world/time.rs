use pumpkin_cluster::time::{TICKS_PER_WRAP, TickStamp};
use pumpkin_protocol::{bedrock::client::set_time::CSetTime, java::client::play::CUpdateTime};

use super::World;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClusterTimeModel {
    pub double_day_counter: u16,
    pub sync_time_offset: i16,
}

impl ClusterTimeModel {
    #[must_use]
    pub const fn new(double_day_counter: u16, sync_time_offset: i16) -> Self {
        Self {
            double_day_counter,
            sync_time_offset,
        }
    }

    #[must_use]
    pub const fn time_at(self, sync_tick: TickStamp) -> i64 {
        self.double_day_counter as i64 * TICKS_PER_WRAP
            + sync_tick.0 as i64
            + self.sync_time_offset as i64
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClockInstance {
    pub total_ticks: i64,
    pub partial_tick: f32,
    pub rate: f32,
    pub paused: bool,
}

impl Default for ClockInstance {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockInstance {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            total_ticks: 0,
            partial_tick: 0.0,
            rate: 1.0,
            paused: false,
        }
    }

    pub const fn load_from(
        &mut self,
        total_ticks: i64,
        partial_tick: f32,
        rate: f32,
        paused: bool,
    ) {
        self.total_ticks = total_ticks;
        self.partial_tick = partial_tick;
        self.rate = rate;
        self.paused = paused;
    }

    pub fn tick(&mut self) {
        if !self.paused {
            self.partial_tick += self.rate;
            let full_ticks = self.partial_tick.floor() as i32;
            self.partial_tick -= full_ticks as f32;
            self.total_ticks += full_ticks as i64;
        }
    }

    pub const fn set_total_ticks(&mut self, total_ticks: i64) {
        self.total_ticks = total_ticks;
        self.partial_tick = 0.0;
    }

    pub fn add_ticks(&mut self, ticks: i64) {
        self.total_ticks = (self.total_ticks + ticks).max(0);
    }

    pub const fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub const fn set_rate(&mut self, rate: f32) {
        self.rate = rate;
    }

    #[must_use]
    pub const fn pack_network_state(&self, advance_time: bool) -> (i64, f32, f32) {
        let paused = self.paused || !advance_time;
        let rate = if paused { 0.0 } else { self.rate };
        (self.total_ticks, self.partial_tick, rate)
    }
}

#[derive(Clone, Debug)]
pub struct LevelTime {
    pub time_of_day: i64,
    pub world_age: i64,
    pub partial_tick: f32,
    pub rate: f32,
    pub paused: bool,
    pub double_day_counter: u16,
    pub sync_time_offset: i16,
    legacy_time_of_day: Option<i64>,
    last_sync_tick: Option<TickStamp>,
}

impl Default for LevelTime {
    fn default() -> Self {
        Self::new()
    }
}

impl LevelTime {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            time_of_day: 0,
            world_age: 0,
            partial_tick: 0.0,
            rate: 1.0,
            paused: false,
            double_day_counter: 0,
            sync_time_offset: 0,
            legacy_time_of_day: None,
            last_sync_tick: None,
        }
    }

    pub const fn load_from(&mut self, time_of_day: i64, world_age: i64) {
        self.time_of_day = time_of_day;
        self.world_age = world_age;
    }

    #[must_use]
    pub fn from_persisted(
        double_day_counter: u16,
        sync_time_offset: i16,
        legacy_time_of_day: Option<i64>,
    ) -> Self {
        let mut time = Self::new();
        time.double_day_counter = double_day_counter;
        time.sync_time_offset = sync_time_offset;
        time.legacy_time_of_day = legacy_time_of_day;
        time.time_of_day = time.time_at(TickStamp(0));
        time
    }

    pub fn tick(
        &mut self,
        advance_time: bool,
        sync_tick: Option<TickStamp>,
        sync_driven: bool,
    ) -> bool {
        self.world_age += 1;
        if sync_driven {
            let Some(sync_tick) = sync_tick else {
                return false;
            };
            let migrated = self.legacy_time_of_day.take().is_some_and(|time_of_day| {
                self.set_time_from_sync(time_of_day, sync_tick);
                true
            });
            let advanced = self.last_sync_tick.is_some_and(|previous| {
                sync_tick.0 < previous.0
                    && sync_tick.distance_since(previous) < (TICKS_PER_WRAP as u16 / 2)
            });
            if advanced {
                self.double_day_counter = self.double_day_counter.wrapping_add(1);
            }
            self.last_sync_tick = Some(sync_tick);
            self.time_of_day = self.time_at(sync_tick);
            self.partial_tick = 0.0;
            self.rate = 1.0;
            self.paused = false;
            return migrated || advanced;
        }
        if advance_time && !self.paused {
            self.partial_tick += self.rate;
            let full_ticks = self.partial_tick.floor() as i32;
            self.partial_tick -= full_ticks as f32;
            self.time_of_day += full_ticks as i64;
        }
        false
    }

    #[must_use]
    pub const fn time_at(&self, sync_tick: TickStamp) -> i64 {
        ClusterTimeModel::new(self.double_day_counter, self.sync_time_offset).time_at(sync_tick)
    }

    pub fn set_time_from_sync(&mut self, time_of_day: i64, sync_tick: TickStamp) {
        let relative = time_of_day.saturating_sub(sync_tick.0 as i64);
        let double_days = relative
            .saturating_add(TICKS_PER_WRAP / 2)
            .div_euclid(TICKS_PER_WRAP);
        let offset = relative.saturating_sub(double_days.saturating_mul(TICKS_PER_WRAP));
        self.double_day_counter = double_days.rem_euclid(u16::MAX as i64 + 1) as u16;
        self.sync_time_offset = offset as i16;
        self.legacy_time_of_day = None;
        self.last_sync_tick = Some(sync_tick);
        self.time_of_day = self.time_at(sync_tick);
        self.partial_tick = 0.0;
    }

    pub fn send_time(&self, world: &World) {
        let advance_time = {
            let lock = world.level_info.load();
            lock.game_rules.advance_time
        };

        let synced_time = world
            .server
            .upgrade()
            .filter(|server| server.advanced_config.cluster.enabled)
            .and_then(|_| crate::server::cluster::disciplined_tick_now())
            .map(|sync_tick| self.time_at(sync_tick));
        let (total_ticks, partial_tick, rate) = synced_time.map_or_else(
            || self.pack_network_state(advance_time),
            |time_of_day| (time_of_day, 0.0, 1.0),
        );

        world.broadcast_packet_except_editioned(
            &[],
            &CUpdateTime::new_clock(self.world_age, 0, total_ticks, partial_tick, rate),
            &CSetTime::new(total_ticks as _), // TODO do we need to tell bedrock that time is frozen?
        );
    }

    pub fn add_time(&mut self, time: i64) {
        self.time_of_day = (self.time_of_day + time).max(0);
    }

    pub const fn set_time(&mut self, time: i64) {
        self.time_of_day = time;
        self.partial_tick = 0.0;
    }

    pub const fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub const fn set_rate(&mut self, rate: f32) {
        self.rate = rate;
    }

    #[must_use]
    pub const fn pack_network_state(&self, advance_time: bool) -> (i64, f32, f32) {
        let paused = self.paused || !advance_time;
        let rate = if paused { 0.0 } else { self.rate };
        (self.time_of_day, self.partial_tick, rate)
    }

    #[must_use]
    pub const fn query_daytime(&self) -> i64 {
        self.time_of_day % 24000
    }

    #[must_use]
    pub const fn query_gametime(&self) -> i64 {
        self.world_age
    }

    #[must_use]
    pub const fn query_day(&self) -> i64 {
        self.time_of_day / 24000
    }

    #[must_use]
    pub const fn is_night(&self) -> bool {
        (self.time_of_day % 24000) >= 12000 && (self.time_of_day % 24000) <= 23999
    }
}

impl World {
    pub fn store_cluster_time_model(&self, double_day_counter: u16, sync_time_offset: i16) {
        self.cluster_time_model.store(std::sync::Arc::new(ClusterTimeModel::new(
            double_day_counter,
            sync_time_offset,
        )));
    }

    #[must_use]
    pub fn cluster_time_model(&self) -> ClusterTimeModel {
        **self.cluster_time_model.load()
    }

    pub fn send_cluster_time(&self) {
        let Some(sync_tick) = self
            .server
            .upgrade()
            .filter(|server| server.advanced_config.cluster.enabled)
            .and_then(|_| crate::server::cluster::disciplined_tick_now())
        else {
            return;
        };
        let time_of_day = self.cluster_time_model().time_at(sync_tick);
        self.broadcast_packet_except_editioned(
            &[],
            &CUpdateTime::new_clock(
                self.cluster_world_age.load(std::sync::atomic::Ordering::Acquire),
                0,
                time_of_day,
                0.0,
                1.0,
            ),
            &CSetTime::new(time_of_day as _),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_instance_ticking() {
        let mut clock = ClockInstance::new();
        assert_eq!(clock.total_ticks, 0);
        assert_eq!(clock.partial_tick, 0.0);
        assert_eq!(clock.rate, 1.0);
        assert!(!clock.paused);

        // Standard tick at rate 1.0
        clock.tick();
        assert_eq!(clock.total_ticks, 1);
        assert_eq!(clock.partial_tick, 0.0);

        // Half rate tick
        clock.set_rate(0.5);
        clock.tick();
        assert_eq!(clock.total_ticks, 1);
        assert_eq!(clock.partial_tick, 0.5);
        clock.tick();
        assert_eq!(clock.total_ticks, 2);
        assert_eq!(clock.partial_tick, 0.0);

        // Double rate tick
        clock.set_rate(2.0);
        clock.tick();
        assert_eq!(clock.total_ticks, 4);
        assert_eq!(clock.partial_tick, 0.0);

        // Paused clock
        clock.set_paused(true);
        clock.tick();
        assert_eq!(clock.total_ticks, 4);
    }

    #[test]
    fn level_time_set_and_add() {
        let mut time = LevelTime::new();
        time.set_time(1000);
        assert_eq!(time.time_of_day, 1000);
        assert_eq!(time.partial_tick, 0.0);

        time.add_time(500);
        assert_eq!(time.time_of_day, 1500);

        time.add_time(-2000);
        assert_eq!(time.time_of_day, 0);
    }

    #[test]
    fn sync_time_uses_double_day_counter_and_offset() {
        let time = LevelTime::from_persisted(3, -1200, None);
        assert_eq!(time.time_at(TickStamp(2400)), 145_200);
    }

    #[test]
    fn setting_sync_time_keeps_the_requested_time() {
        let mut time = LevelTime::new();
        let tick = TickStamp(47_500);
        time.set_time_from_sync(123_456, tick);
        assert_eq!(time.time_at(tick), 123_456);
        assert!((-24_000..24_000).contains(&i32::from(time.sync_time_offset)));
    }

    #[test]
    fn legacy_time_is_migrated_on_first_sync_tick() {
        let mut time = LevelTime::from_persisted(0, 0, Some(96_123));
        assert!(time.tick(true, Some(TickStamp(123)), true));
        assert_eq!(time.time_at(TickStamp(123)), 96_123);
    }

    #[test]
    fn sync_wrap_advances_the_persisted_double_day() {
        let mut time = LevelTime::from_persisted(4, 0, None);
        assert!(!time.tick(true, Some(TickStamp(47_999)), true));
        assert!(time.tick(true, Some(TickStamp(0)), true));
        assert_eq!(time.double_day_counter, 5);
        assert_eq!(time.time_of_day, 240_000);
    }
}
