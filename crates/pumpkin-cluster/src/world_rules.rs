use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canonical_action::ApplyFailure;
use crate::protocol::WorldId;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorldRulesId {
    pub world: WorldId,
    pub issuer: PeerGeneration,
    pub epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldRulesWitness {
    pub rules: WorldRulesId,
    pub slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorldConfigurationVersion {
    pub issuer: PeerGeneration,
    pub revision: u64,
    pub from_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldRulesSnapshot {
    pub rules: WorldRulesId,
    pub configuration: WorldConfigurationVersion,
    pub from_slot: u64,
    pub keep_inventory: bool,
    pub do_entity_drops: bool,
    pub do_mob_loot: bool,
    pub mob_griefing: bool,
    pub tnt_explodes: bool,
    pub random_tick_speed: i64,
    pub natural_regeneration: bool,
    pub advance_time: bool,
    pub advance_weather: bool,
    pub players_sleeping_percentage: i64,
    pub do_mob_spawning: bool,
    pub water_source_conversion: bool,
    pub lava_source_conversion: bool,
    pub fire_spread_radius_around_player: i64,
    pub min_y: i32,
    pub height: u32,
    pub fire_damage: bool,
    pub drowning_damage: bool,
    pub freeze_damage: bool,
    pub fall_damage: bool,
    pub difficulty: u8,
}

impl WorldRulesSnapshot {
    pub fn valid(&self) -> bool {
        self.rules.issuer.generation != 0
            && self.rules.epoch != 0
            && self.configuration.issuer == self.rules.issuer
            && self.configuration.revision != 0
            && self.configuration.from_slot <= self.from_slot
            && self.height != 0
            && self.height <= 4096
            && self.height % 16 == 0
            && self.min_y % 16 == 0
            && self.min_y.checked_add(self.height as i32).is_some()
            && self.difficulty <= 3
            && self.random_tick_speed >= 0
    }
}

#[derive(Clone, Default)]
pub struct WorldRulesHistory {
    snapshots: BTreeMap<WorldRulesId, WorldRulesSnapshot>,
}

impl WorldRulesHistory {
    pub fn insert(
        &mut self,
        authenticated: PeerGeneration,
        elected_primary: PeerGeneration,
        snapshot: WorldRulesSnapshot,
    ) -> Result<(), ApplyFailure> {
        if !snapshot.valid()
            || authenticated != elected_primary
            || snapshot.rules.issuer != authenticated
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(previous) = self.snapshots.get(&snapshot.rules) {
            return if *previous == snapshot {
                Ok(())
            } else {
                Err(ApplyFailure::InvalidAction)
            };
        }
        if self.snapshots.len() >= 8192 {
            return Err(ApplyFailure::Capacity);
        }
        let first = WorldRulesId { epoch: 0, ..snapshot.rules };
        let last = WorldRulesId { epoch: u64::MAX, ..snapshot.rules };
        if self.snapshots.range(first..snapshot.rules).next_back()
            .is_some_and(|(_, previous)| previous.from_slot >= snapshot.from_slot)
            || self.snapshots.range((std::ops::Bound::Excluded(snapshot.rules), std::ops::Bound::Included(last))).next()
                .is_some_and(|(_, later)| later.from_slot <= snapshot.from_slot) {
            return Err(ApplyFailure::InvalidAction);
        }
        self.snapshots.insert(snapshot.rules, snapshot);
        Ok(())
    }

    pub fn resolve(
        &self,
        witness: WorldRulesWitness,
        tick: TickStamp,
    ) -> Result<&WorldRulesSnapshot, ApplyFailure> {
        let snapshot = self
            .snapshots
            .get(&witness.rules)
            .ok_or(ApplyFailure::InvalidAction)?;
        if witness.slot as u16 != tick.0 || witness.slot < snapshot.from_slot {
            return Err(ApplyFailure::InvalidAction);
        }
        let last = WorldRulesId { epoch: u64::MAX, ..witness.rules };
        if self.snapshots.range((std::ops::Bound::Excluded(witness.rules), std::ops::Bound::Included(last))).next()
            .is_some_and(|(_, later)| later.from_slot <= witness.slot) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(snapshot)
    }

    pub fn contains(&self, rules: WorldRulesId) -> bool {
        self.snapshots.contains_key(&rules)
    }

    pub fn is_extension_of(&self, older: &Self) -> bool {
        older
            .snapshots
            .iter()
            .all(|(key, value)| self.snapshots.get(key) == Some(value))
    }

    pub fn revision(&self) -> usize {
        self.snapshots.len()
    }

    pub fn snapshots(&self) -> impl Iterator<Item = &WorldRulesSnapshot> {
        self.snapshots.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ServerId;

    fn snapshot(epoch: u64, from_slot: u64) -> WorldRulesSnapshot {
        WorldRulesSnapshot {
            rules: WorldRulesId {
                world: WorldId {
                    dimension: 0,
                    key: 1,
                },
                issuer: PeerGeneration {
                    server: ServerId(1),
                    generation: 9,
                },
                epoch,
            },
            configuration: WorldConfigurationVersion { issuer: PeerGeneration { server: ServerId(1), generation: 9 }, revision: epoch, from_slot },
            from_slot,
            keep_inventory: false,
            do_entity_drops: true,
            do_mob_loot: true,
            mob_griefing: true,
            tnt_explodes: true,
            random_tick_speed: 3,
            natural_regeneration: true,
            advance_time: true,
            advance_weather: true,
            players_sleeping_percentage: 100,
            do_mob_spawning: true,
            water_source_conversion: true,
            lava_source_conversion: false,
            fire_spread_radius_around_player: 128,
            min_y: -64,
            height: 384,
            fire_damage: true,
            drowning_damage: true,
            freeze_damage: true,
            fall_damage: true,
            difficulty: 2,
        }
    }

    #[test]
    fn chronology_checks_nearest_versions_with_foreign_worlds_and_out_of_order_delivery() {
        let mut history = WorldRulesHistory::default();
        for (epoch, slot) in [(4, 40), (1, 10), (3, 30), (2, 20)] {
            let value = snapshot(epoch, slot);
            history.insert(value.rules.issuer, value.rules.issuer, value).unwrap();
        }
        let mut foreign = snapshot(2, 1);
        foreign.rules.world.key = 2;
        history.insert(foreign.rules.issuer, foreign.rules.issuer, foreign).unwrap();
        let invalid = snapshot(5, 39);
        assert_eq!(history.insert(invalid.rules.issuer, invalid.rules.issuer, invalid), Err(ApplyFailure::InvalidAction));
        let mut negative = snapshot(5, 50);
        negative.random_tick_speed = -1;
        assert!(!negative.valid());
        let first = snapshot(1, 10);
        assert!(history.resolve(WorldRulesWitness { rules: first.rules, slot: 19 }, TickStamp(19)).is_ok());
        assert_eq!(history.resolve(WorldRulesWitness { rules: first.rules, slot: 20 }, TickStamp(20)), Err(ApplyFailure::InvalidAction));
        let mut high = snapshot(u64::MAX, 50);
        high.rules.world.key = 3;
        history.insert(high.rules.issuer, high.rules.issuer, high.clone()).unwrap();
        assert_eq!(history.resolve(WorldRulesWitness { rules: high.rules, slot: 51 }, TickStamp(51)), Ok(&high));
    }

    #[test]
    fn archive_extension_preserves_every_original_version() {
        let first = snapshot(1, 10);
        let second = snapshot(2, 20);
        let mut original = WorldRulesHistory::default();
        original
            .insert(first.rules.issuer, first.rules.issuer, first.clone())
            .unwrap();
        let mut extended = original.clone();
        extended
            .insert(second.rules.issuer, second.rules.issuer, second.clone())
            .unwrap();
        assert!(extended.is_extension_of(&original));
        assert!(!original.is_extension_of(&extended));
        let mut replaced = extended.clone();
        replaced
            .snapshots
            .get_mut(&first.rules)
            .unwrap()
            .keep_inventory = true;
        assert!(!replaced.is_extension_of(&original));
        assert_eq!(
            extended.resolve(
                WorldRulesWitness {
                    rules: first.rules,
                    slot: 19
                },
                TickStamp(19)
            ),
            Ok(&first)
        );
    }

    #[test]
    fn original_epoch_resolves_across_wrapping_tick_boundary() {
        let first = snapshot(1, 65_530);
        let second = snapshot(2, 65_540);
        let mut history = WorldRulesHistory::default();
        history
            .insert(first.rules.issuer, first.rules.issuer, first.clone())
            .unwrap();
        history
            .insert(second.rules.issuer, second.rules.issuer, second.clone())
            .unwrap();
        assert_eq!(
            history.resolve(
                WorldRulesWitness {
                    rules: first.rules,
                    slot: 65_536
                },
                TickStamp(0)
            ),
            Ok(&first)
        );
        assert!(
            history
                .resolve(
                    WorldRulesWitness {
                        rules: first.rules,
                        slot: 65_541
                    },
                    TickStamp(5)
                )
                .is_err()
        );
        assert_eq!(
            history.resolve(
                WorldRulesWitness {
                    rules: second.rules,
                    slot: 65_541
                },
                TickStamp(5)
            ),
            Ok(&second)
        );
    }

    #[test]
    fn foreign_issuer_and_conflicting_epoch_are_rejected() {
        let mut history = WorldRulesHistory::default();
        let original = snapshot(1, 10);
        let foreign = PeerGeneration {
            server: ServerId(2),
            generation: 3,
        };
        assert!(
            history
                .insert(foreign, original.rules.issuer, original.clone())
                .is_err()
        );
        history
            .insert(
                original.rules.issuer,
                original.rules.issuer,
                original.clone(),
            )
            .unwrap();
        let mut altered = original.clone();
        altered.keep_inventory = true;
        assert!(
            history
                .insert(original.rules.issuer, original.rules.issuer, altered)
                .is_err()
        );
    }
}
