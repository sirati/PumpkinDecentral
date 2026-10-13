use std::collections::HashMap;

use crate::actor::ActorId;
use crate::canonical_action::ActionId;
use crate::time::TickStamp;

pub const MAX_ACTIONS_PER_ACTOR_TICK: u32 = 4096;
pub const MAX_OPEN_ACTOR_ACTIONS: u32 = 32767;
pub const MAX_OPEN_ACTOR_TICKS: usize =
    (MAX_OPEN_ACTOR_ACTIONS / MAX_ACTIONS_PER_ACTOR_TICK) as usize;

struct IssueState {
    next: u16,
    ticks: HashMap<TickStamp, u32>,
    outstanding: u32,
    tail_reservation: Option<(TickStamp, u64)>,
}

#[derive(Default)]
pub struct ActionIssueClock {
    actors: HashMap<(ActorId, u64), IssueState>,
    next_reservation: u64,
    closed_through: Option<TickStamp>,
}

pub struct ActionIssueReservation {
    actor: ActorId,
    incarnation: u64,
    tick: TickStamp,
    first: u16,
    count: u32,
    token: u64,
}

impl ActionIssueReservation {
    pub fn id(&self, index: u32, family: u8) -> Option<ActionId> {
        (index < self.count).then_some(ActionId {
            actor: self.actor,
            incarnation: self.incarnation,
            sequence: self.first.wrapping_add(index as u16),
            family,
        })
    }
}

impl ActionIssueClock {
    pub fn available(&self, actor: ActorId, incarnation: u64, tick: TickStamp) -> u32 {
        if incarnation == 0
            || self
                .closed_through
                .is_some_and(|closed| !tick.is_after(closed))
        {
            return 0;
        }
        match self.actors.get(&(actor, incarnation)) {
            Some(state) => {
                if !state.ticks.contains_key(&tick) && state.ticks.len() >= MAX_OPEN_ACTOR_TICKS {
                    return 0;
                }
                (MAX_ACTIONS_PER_ACTOR_TICK - state.ticks.get(&tick).copied().unwrap_or(0))
                    .min(MAX_OPEN_ACTOR_ACTIONS - state.outstanding)
            }
            _ => MAX_ACTIONS_PER_ACTOR_TICK,
        }
    }

    pub fn reserve(
        &mut self,
        actor: ActorId,
        incarnation: u64,
        tick: TickStamp,
        count: u32,
    ) -> Option<ActionIssueReservation> {
        if count == 0 || count > self.available(actor, incarnation, tick) {
            return None;
        }
        let token = self.next_reservation.checked_add(1)?;
        self.next_reservation = token;
        let state = self
            .actors
            .entry((actor, incarnation))
            .or_insert(IssueState {
                next: 0,
                ticks: HashMap::new(),
                outstanding: 0,
                tail_reservation: None,
            });
        let reservation = ActionIssueReservation {
            actor,
            incarnation,
            tick,
            first: state.next,
            count,
            token,
        };
        state.next = state.next.wrapping_add(count as u16);
        *state.ticks.entry(tick).or_default() += count;
        state.outstanding += count;
        state.tail_reservation = Some((tick, token));
        Some(reservation)
    }

    pub fn open_tick(&mut self, actor: ActorId, incarnation: u64, tick: TickStamp) -> bool {
        if self
            .actors
            .get(&(actor, incarnation))
            .is_some_and(|state| state.ticks.contains_key(&tick))
        {
            return true;
        }
        if self.available(actor, incarnation, tick) == 0 {
            return false;
        }
        self.actors
            .entry((actor, incarnation))
            .or_insert(IssueState {
                next: 0,
                ticks: HashMap::new(),
                outstanding: 0,
                tail_reservation: None,
            })
            .ticks
            .entry(tick)
            .or_default();
        true
    }

    pub fn release_unused(&mut self, reservation: ActionIssueReservation) -> bool {
        let Some(state) = self
            .actors
            .get_mut(&(reservation.actor, reservation.incarnation))
        else {
            return false;
        };
        if state.tail_reservation != Some((reservation.tick, reservation.token))
            || state.next != reservation.first.wrapping_add(reservation.count as u16)
            || state.ticks.get(&reservation.tick).copied().unwrap_or(0) < reservation.count
        {
            return false;
        }
        state.next = reservation.first;
        let issued = state.ticks.get_mut(&reservation.tick).unwrap();
        *issued -= reservation.count;
        if *issued == 0 {
            state.ticks.remove(&reservation.tick);
        }
        state.outstanding -= reservation.count;
        state.tail_reservation = None;
        true
    }

    pub fn retire_settled(&mut self, actor: ActorId, incarnation: u64) {
        self.actors.remove(&(actor, incarnation));
    }

    pub fn can_close_tick(&self, tick: TickStamp) -> bool {
        if self
            .closed_through
            .is_some_and(|closed| !tick.is_after(closed))
        {
            return self.closed_through == Some(tick);
        }
        !self
            .actors
            .values()
            .any(|state| state.ticks.keys().any(|open| tick.is_after(*open)))
    }

    pub fn close_tick(&mut self, tick: TickStamp) -> bool {
        if !self.can_close_tick(tick) {
            return false;
        }
        for state in self.actors.values_mut() {
            if let Some(issued) = state.ticks.remove(&tick) {
                state.outstanding -= issued;
                if state
                    .tail_reservation
                    .is_some_and(|(reserved_tick, _)| reserved_tick == tick)
                {
                    state.tail_reservation = None;
                }
            }
        }
        self.closed_through = Some(tick);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_original_tick_reserves_after_newer_tick_without_reset() {
        let actor = ActorId::Peer(crate::identity::ServerId(0));
        let mut clock = ActionIssueClock::default();
        let old = TickStamp(u16::MAX);
        let newer = TickStamp(0);
        let first = clock.reserve(actor, 1, old, 2).unwrap();
        let next = clock.reserve(actor, 1, newer, 1).unwrap();
        let resumed = clock.reserve(actor, 1, old, 1).unwrap();
        assert_eq!(first.id(0, 1).unwrap().sequence, 0);
        assert_eq!(next.id(0, 1).unwrap().sequence, 2);
        assert_eq!(resumed.id(0, 1).unwrap().sequence, 3);
        assert_eq!(
            clock.available(actor, 1, old),
            MAX_ACTIONS_PER_ACTOR_TICK - 3
        );
        assert!(!clock.close_tick(newer));
        assert!(clock.close_tick(old));
        assert_eq!(clock.available(actor, 1, old), 0);
        assert!(!clock.release_unused(resumed));
        assert!(clock.close_tick(newer));
        assert_eq!(
            clock
                .reserve(actor, 1, TickStamp(1), 1)
                .unwrap()
                .id(0, 1)
                .unwrap()
                .sequence,
            4
        );
    }

    #[test]
    fn closing_older_tick_keeps_newer_unused_tail_releasable() {
        let actor = ActorId::Peer(crate::identity::ServerId(0));
        let mut clock = ActionIssueClock::default();
        clock.reserve(actor, 1, TickStamp(9), 1).unwrap();
        let newer = clock.reserve(actor, 1, TickStamp(10), 2).unwrap();
        assert!(clock.close_tick(TickStamp(9)));
        assert!(clock.release_unused(newer));
        assert_eq!(
            clock.available(actor, 1, TickStamp(10)),
            MAX_ACTIONS_PER_ACTOR_TICK
        );
        assert_eq!(
            clock
                .reserve(actor, 1, TickStamp(10), 1)
                .unwrap()
                .id(0, 1)
                .unwrap()
                .sequence,
            1
        );
    }

    #[test]
    fn open_actor_reservations_never_span_half_sequence_range() {
        let actor = ActorId::Peer(crate::identity::ServerId(0));
        let mut clock = ActionIssueClock::default();
        for tick in 0..7 {
            clock.reserve(actor, 1, TickStamp(tick), 4096).unwrap();
        }
        assert_eq!(clock.available(actor, 1, TickStamp(7)), 0);
        assert!(clock.reserve(actor, 1, TickStamp(8), 1).is_none());
        assert!(clock.close_tick(TickStamp(0)));
        assert_eq!(clock.available(actor, 1, TickStamp(8)), 4096);
        assert!(clock.open_tick(actor, 1, TickStamp(8)));
        assert_eq!(clock.available(actor, 1, TickStamp(9)), 0);
    }

    #[test]
    fn unused_tail_release_preserves_prior_and_newer_reservations() {
        let mut clock = ActionIssueClock::default();
        let actor = ActorId::Peer(crate::identity::ServerId(0));
        let tick = TickStamp(9);
        let prior = clock.reserve(actor, 1, tick, 2).unwrap();
        let unused = clock.reserve(actor, 1, tick, 3).unwrap();
        assert!(!clock.release_unused(prior));
        assert!(clock.release_unused(unused));
        assert_eq!(
            clock.available(actor, 1, tick),
            MAX_ACTIONS_PER_ACTOR_TICK - 2
        );
        let replacement = clock.reserve(actor, 1, tick, 1).unwrap();
        assert_eq!(replacement.id(0, 4).unwrap().sequence, 2);
        clock.reserve(actor, 1, TickStamp(10), 1).unwrap();
        assert!(!clock.release_unused(replacement));
    }

    #[test]
    fn retired_actor_reservation_cannot_release_reused_ordinal() {
        let mut clock = ActionIssueClock::default();
        let actor = ActorId::Peer(crate::identity::ServerId(0));
        let tick = TickStamp(9);
        let stale = clock.reserve(actor, 1, tick, 1).unwrap();
        clock.retire_settled(actor, 1);
        let current = clock.reserve(actor, 1, tick, 1).unwrap();
        assert_eq!(stale.id(0, 4), current.id(0, 4));
        assert!(!clock.release_unused(stale));
        assert!(clock.release_unused(current));
    }
}
