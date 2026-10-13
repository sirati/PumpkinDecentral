use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::player_admin::EffectState;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveEffectSet {
    pub visible: Vec<EffectState>,
    pub hidden: BTreeMap<u16, Vec<EffectState>>,
}

fn remaining(duration: i32) -> i64 {
    if duration == -1 {
        i64::MAX
    } else {
        i64::from(duration)
    }
}

impl ActiveEffectSet {
    pub fn valid(&self) -> bool {
        self.visible.len() <= 256
            && self.hidden.values().map(Vec::len).sum::<usize>() <= 256
            && self.visible.iter().all(|effect| effect.duration >= -1)
            && self
                .visible
                .iter()
                .map(|effect| effect.kind)
                .collect::<BTreeSet<_>>()
                .len()
                == self.visible.len()
            && self.hidden.iter().all(|(kind, chain)| {
                let Some(visible) = self.visible.iter().find(|effect| effect.kind == *kind) else {
                    return false;
                };
                let mut previous = visible;
                chain.iter().all(|effect| {
                    let valid = effect.kind == *kind
                        && effect.duration >= -1
                        && effect.amplifier < previous.amplifier
                        && remaining(effect.duration) > remaining(previous.duration);
                    previous = effect;
                    valid
                })
            })
    }

    pub fn insert(&mut self, effect: EffectState) -> bool {
        if effect.duration < -1 {
            return false;
        }
        let mut next = self.clone();
        let mut chain = next.hidden.remove(&effect.kind).unwrap_or_default();
        if let Some(index) = next
            .visible
            .iter()
            .position(|existing| existing.kind == effect.kind)
        {
            chain.push(next.visible.remove(index));
        }
        chain.push(effect.clone());
        chain.sort_by_key(|effect| {
            (
                std::cmp::Reverse(effect.amplifier),
                std::cmp::Reverse(remaining(effect.duration)),
            )
        });
        let mut retained = Vec::<EffectState>::new();
        for candidate in chain {
            if retained.last().is_none_or(|previous| {
                candidate.amplifier < previous.amplifier
                    && remaining(candidate.duration) > remaining(previous.duration)
            }) {
                retained.push(candidate);
            }
        }
        if retained.is_empty() {
            return false;
        }
        let mut visible = retained.remove(0);
        if visible.amplifier == effect.amplifier {
            visible.ambient &= effect.ambient;
            visible.show_particles = effect.show_particles;
            visible.show_icon = effect.show_icon;
        }
        next.visible.push(visible);
        next.visible.sort_by_key(|effect| effect.kind);
        if !retained.is_empty() {
            next.hidden.insert(effect.kind, retained);
        }
        if !next.valid() {
            return false;
        }
        *self = next;
        true
    }

    pub fn remove(&mut self, kind: u16) {
        self.visible.retain(|effect| effect.kind != kind);
        self.hidden.remove(&kind);
    }

    pub fn tick(&mut self) {
        self.advance(1);
    }

    pub fn advance(&mut self, elapsed: u16) {
        if elapsed == 0 {
            return;
        }
        for effect in self
            .visible
            .iter_mut()
            .chain(self.hidden.values_mut().flatten())
        {
            if effect.duration > 0 {
                effect.duration = effect.duration.saturating_sub(i32::from(elapsed)).max(0);
            }
        }
        for chain in self.hidden.values_mut() {
            chain.retain(|effect| effect.duration != 0);
        }
        for visible in &mut self.visible {
            if visible.duration == 0
                && let Some(chain) = self.hidden.get_mut(&visible.kind)
                && !chain.is_empty()
            {
                *visible = chain.remove(0);
            }
        }
        self.visible.retain(|effect| effect.duration != 0);
        self.hidden.retain(|_, chain| !chain.is_empty());
    }
}
