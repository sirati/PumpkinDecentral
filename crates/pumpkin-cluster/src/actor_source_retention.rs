use std::collections::{BTreeMap, BTreeSet};

use crate::actor::ActorId;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActorSourceFamily {
    Living,
    Mob,
    Object,
    Vehicle,
    Environment,
    Item,
    ItemPhysics,
    ItemLifetime,
    Projectile,
    Controller,
}

#[derive(Default)]
pub struct ActorSourceRetention {
    sources: BTreeMap<(ActorId, u64), BTreeSet<(TickStamp, ActorSourceFamily)>>,
}

impl ActorSourceRetention {
    pub fn contains(&self, actor: &(ActorId, u64)) -> bool {
        self.sources.contains_key(actor)
    }

    pub fn hold(&mut self, actor: (ActorId, u64), tick: TickStamp, family: ActorSourceFamily) {
        self.sources
            .entry(actor)
            .or_default()
            .insert((tick, family));
    }

    pub fn release(&mut self, actor: (ActorId, u64), tick: TickStamp, family: ActorSourceFamily) {
        if let Some(sources) = self.sources.get_mut(&actor) {
            sources.remove(&(tick, family));
            if sources.is_empty() {
                self.sources.remove(&actor);
            }
        }
    }

    pub fn release_tick(&mut self, tick: TickStamp, family: ActorSourceFamily) {
        self.sources.retain(|_, sources| {
            sources.remove(&(tick, family));
            !sources.is_empty()
        });
    }
}
