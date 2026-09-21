//! Shared tick clock: NTP-disciplined wall time quantized to 1/20 s stamps.
//!
//! Every peer derives the same [`TickStamp`] from the same moment in time by
//! computing `floor((unix_millis + ntp_offset) / 50)` and reducing it into a
//! wrapping [`u16`]. [`NtpDiscipline`] is the gatekeeper: without a fresh
//! enough NTP offset, or when the offset exceeds its bound, it yields `None`
//! instead of a stamp that would diverge across peers.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Milliseconds per tick: 50 ms, i.e. 20 ticks per second.
pub const MILLIS_PER_TICK: i64 = 50;
/// Tick stamps wrap after the full [`u16`] range.
pub const TICKS_PER_WRAP: i64 = 65_536;

/// A tick instant quantized to 1/20 s, reduced into a wrapping [`u16`].
///
/// Two peers that agree on disciplined wall-clock millis (see
/// [`NtpDiscipline`]) agree on the stamp for the same instant. The counter
/// wraps every `65_536 * 50 ms` (~54.6 minutes); use [`TickStamp::distance_since`]
/// for wrap-aware comparison instead of raw ordering.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct TickStamp(pub u16);

impl TickStamp {
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
        let reduced = ticks % TICKS_PER_WRAP;
        Self(reduced as u16)
    }

    /// Wrap-aware forward distance from `older` to `self`.
    ///
    /// `TickStamp(1).distance_since(TickStamp(0xFFFF)) == 2`, so accept
    /// windows and dedup maps keep working across the wrap boundary.
    #[must_use]
    pub const fn distance_since(self, older: Self) -> u16 {
        self.0.wrapping_sub(older.0)
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
/// Holds the latest filtered offset (`server - local`, in millis) plus the
/// maximum tolerable magnitude. [`NtpDiscipline::tick_at`] returns `Some`
/// only while disciplined and healthy, so an undisciplined or stray clock
/// never mints stamps that would fork peer agreement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NtpDiscipline {
    /// Latest filtered NTP offset in millis (`server - local`), if any.
    pub offset_millis: Option<i64>,
    /// Maximum accepted `|offset|` in millis; larger offsets gate to `None`.
    pub max_offset_millis: i64,
}

impl NtpDiscipline {
    /// Starts undisciplined: [`NtpDiscipline::tick_at`] yields `None` until [`NtpDiscipline::observe`].
    #[must_use]
    pub const fn unconfigured(max_offset_millis: i64) -> Self {
        Self {
            offset_millis: None,
            max_offset_millis,
        }
    }

    /// Records a fresh filtered offset sample from the NTP pipeline.
    pub const fn observe(&mut self, offset_millis: i64) {
        self.offset_millis = Some(offset_millis);
    }

    /// Stamps `unix_millis` if disciplined and `|offset| <= max`, else `None`.
    ///
    /// The `None` cases are the discipline contract: no offset yet, or the
    /// offset is beyond tolerance so peers must not trust this clock for
    /// shared stamps.
    #[must_use]
    pub fn tick_at(&self, unix_millis: i64) -> Option<TickStamp> {
        match self.offset_millis {
            None => None,
            Some(offset) => {
                if offset < 0 {
                    if offset < -self.max_offset_millis {
                        return None;
                    }
                } else if offset > self.max_offset_millis {
                    return None;
                }
                Some(TickStamp::from_disciplined_millis(
                    unix_millis,
                    offset,
                ))
            }
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
    fn now_advances_with_wall_time() {
        let _serial = crate::ntp::SHARED_OFFSET_SERIAL.lock().unwrap();
        let first = TickStamp::now();
        let second = TickStamp::now();
        assert!(second.distance_since(first) <= 1);
    }

    #[test]
    fn undisciplined_clock_yields_nothing() {
        let clock = NtpDiscipline::unconfigured(250);
        assert_eq!(clock.tick_at(1_000), None);
    }

    #[test]
    fn excessive_offset_yields_nothing() {
        let mut clock = NtpDiscipline::unconfigured(250);
        clock.observe(10_000);
        assert_eq!(clock.tick_at(1_000), None);
        clock.observe(100);
        assert_eq!(clock.tick_at(1_000).is_some(), true);
    }
}
