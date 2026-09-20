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
    handle.try_submit(tick, payload).map_err(|_| WorldTimeError {
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
        WeatherUpdate::new(String::from("minecraft:overworld"), 12_000, 0, 0, false, false)
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
        assert!(parcels.iter().all(|parcel| parcel.kind == StreamKind::Control));
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
