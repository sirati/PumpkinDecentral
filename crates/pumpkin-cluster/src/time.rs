//! Shared tick clock: NTP-disciplined wall time quantized to 1/20 s stamps.
//!
//! Every peer derives the same [`TickStamp`] from the same moment in time by
//! computing `floor((unix_millis + ntp_offset) / 50)` and reducing it into a
//! wrapping [`u16`]. [`NtpDiscipline`] yields `None` until it has an NTP
//! offset, then applies that offset to every stamp.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::Error as _};

/// Milliseconds per tick: 50 ms, i.e. 20 ticks per second.
pub const MILLIS_PER_TICK: i64 = 50;
/// Tick stamps wrap after the full [`u16`] range.
pub const TICKS_PER_WRAP: i64 = 48_000;

/// A tick instant quantized to 1/20 s, reduced into a wrapping [`u16`].
///
/// Two peers that agree on disciplined wall-clock millis (see
/// [`NtpDiscipline`]) agree on the stamp for the same instant. The counter
/// wraps every `65_536 * 50 ms` (~54.6 minutes); use [`TickStamp::distance_since`]
/// for wrap-aware comparison instead of raw ordering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TickStamp(pub u16);

impl Serialize for TickStamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if i64::from(self.0) >= TICKS_PER_WRAP {
            return Err(S::Error::custom("sync tick exceeds two-day wrap"));
        }
        serializer.serialize_u16(self.0)
    }
}

impl<'de> Deserialize<'de> for TickStamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        if i64::from(value) >= TICKS_PER_WRAP {
            return Err(D::Error::custom("sync tick exceeds two-day wrap"));
        }
        Ok(Self(value))
    }
}

impl TickStamp {
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self((value as i64 % TICKS_PER_WRAP) as u16)
    }

    #[must_use]
    pub const fn from_counter(counter: i64) -> Self {
        let reduced = counter.rem_euclid(TICKS_PER_WRAP);
        Self(reduced as u16)
    }

    /// Quantizes disciplined millis down to a 50 ms tick, wrapping into [`u16`].
    ///
    /// Steps, in order: `disciplined = max(millis + offset, 0)` with
    /// saturation, then `floor(disciplined / 50)`, then `mod 65_536`.
    /// Rounding is always down so a stamp never names a tick in the future;
    /// nearby wall times on either side of a boundary therefore settle on the
    /// same stamp once peers share the same disciplined millis.
    #[must_use]
    pub fn from_disciplined_millis(millis: i64, offset_millis: i64) -> Self {
        let disciplined = millis.saturating_add(offset_millis).max(0);
        let ticks = disciplined / MILLIS_PER_TICK;
        Self::from_counter(ticks)
    }

    /// Wrap-aware forward distance from `older` to `self`.
    ///
    /// `TickStamp(1).distance_since(TickStamp(0xFFFF)) == 2`, so accept
    /// windows and dedup maps keep working across the wrap boundary.
    #[must_use]
    pub const fn distance_since(self, older: Self) -> u16 {
        let current = self.0 as i64 % TICKS_PER_WRAP;
        let previous = older.0 as i64 % TICKS_PER_WRAP;
        (current - previous).rem_euclid(TICKS_PER_WRAP) as u16
    }

    #[must_use]
    pub const fn advance(self, ticks: u16) -> Self {
        Self::from_counter(self.0 as i64 + ticks as i64)
    }

    #[must_use]
    pub const fn is_newer_than(self, older: Self) -> bool {
        let distance = self.distance_since(older);
        distance != 0 && distance < (TICKS_PER_WRAP as u16 / 2)
    }

    #[must_use]
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|age| age.as_millis() as i64)
            .unwrap_or(0);
        Self::from_disciplined_millis(millis, crate::ntp::shared_offset_millis().unwrap_or(0))
    }
}

/// NTP clock discipline: the shared-across-peers gate for [`TickStamp`].
///
/// Holds the latest filtered offset (`server - local`, in millis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NtpDiscipline {
    /// Latest filtered NTP offset in millis (`server - local`), if any.
    pub offset_millis: Option<i64>,
}

impl NtpDiscipline {
    /// Starts undisciplined: [`NtpDiscipline::tick_at`] yields `None` until [`NtpDiscipline::observe`].
    #[must_use]
    pub const fn unconfigured() -> Self {
        Self {
            offset_millis: None,
        }
    }

    /// Records a fresh filtered offset sample from the NTP pipeline.
    pub const fn observe(&mut self, offset_millis: i64) {
        self.offset_millis = Some(offset_millis);
    }

    /// Stamps `unix_millis` if a filtered NTP offset exists.
    #[must_use]
    pub fn tick_at(&self, unix_millis: i64) -> Option<TickStamp> {
        match self.offset_millis {
            None => None,
            Some(offset) => Some(TickStamp::from_disciplined_millis(unix_millis, offset)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_millis_to_ticks() {
        assert_eq!(
            TickStamp::from_disciplined_millis(50, 0),
            TickStamp(1)
        );
        assert_eq!(
            TickStamp::from_disciplined_millis(0, 50),
            TickStamp(1)
        );
    }

    #[test]
    fn wraps_around() {
        let full = TICKS_PER_WRAP * MILLIS_PER_TICK;
        assert_eq!(
            TickStamp::from_disciplined_millis(full, 0),
            TickStamp(0)
        );
    }

    #[test]
    fn distance_and_advance_use_double_day_wrap() {
        assert_eq!(TickStamp(1).distance_since(TickStamp(47_999)), 2);
        assert_eq!(TickStamp(47_999).advance(1), TickStamp(0));
        assert!(TickStamp(0).is_newer_than(TickStamp(47_999)));
        assert_eq!(TickStamp::from_counter(-1), TickStamp(47_999));
    }

    #[test]
    fn rejects_out_of_range_wire_ticks() {
        let encoded = postcard::to_allocvec(&u16::MAX).unwrap();
        assert!(postcard::from_bytes::<TickStamp>(&encoded).is_err());
        assert!(postcard::to_allocvec(&TickStamp(u16::MAX)).is_err());
    }

    #[test]
    fn now_advances_with_wall_time() {
        let _serial = crate::ntp::SHARED_OFFSET_SERIAL.lock().unwrap();
        let first = TickStamp::now();
        let second = TickStamp::now();
        assert!(second.distance_since(first) <= 1);
    }

    #[test]
    fn undisciplined_clock_yields_nothing() {
        let clock = NtpDiscipline::unconfigured();
        assert_eq!(clock.tick_at(1_000), None);
    }

    #[test]
    fn every_observed_offset_mints_a_corrected_stamp() {
        let mut clock = NtpDiscipline::unconfigured();
        clock.observe(10_000);
        assert_eq!(
            clock.tick_at(1_000),
            Some(TickStamp::from_disciplined_millis(1_000, 10_000))
        );
        clock.observe(-10_000);
        assert_eq!(
            clock.tick_at(20_000),
            Some(TickStamp::from_disciplined_millis(20_000, -10_000))
        );
    }
}
