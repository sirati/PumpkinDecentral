use std::collections::VecDeque;

use pumpkin_config::lighting::LightingEngineConfig;
use pumpkin_data::BlockStateId;

use crate::chunk::palette::BlockPalette;

/// Derived lighting for the center of one immutable, full-height 3×3 palette cut.
/// The halo is mandatory: missing neighbors cannot be interpreted as darkness.
#[derive(Clone)]
pub struct OwnedChunkLighting {
    pub block: Vec<Box<[u8; 2048]>>,
    pub sky: Vec<Box<[u8; 2048]>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedLightError {
    IncompleteHalo,
    Height,
    Budget,
}

impl OwnedChunkLighting {
    pub fn resolve(
        halo: [&[BlockPalette]; 9],
        config: LightingEngineConfig,
        has_skylight: bool,
        budget: usize,
    ) -> Result<Self, OwnedLightError> {
        let cells = halo[4]
            .len()
            .checked_mul(16 * 48 * 48)
            .ok_or(OwnedLightError::Height)?;
        if cells > budget / 2 {
            return Err(OwnedLightError::Budget);
        }
        let mut work = OwnedLightingWork::start(halo, config, has_skylight)?;
        if !work.advance(budget - cells * 2) {
            return Err(OwnedLightError::Budget);
        }
        work.finish().ok_or(OwnedLightError::Budget)
    }
}

/// A private monotone computation which can yield between owner turns. No
/// partial light arrays may be published before both propagation layers drain.
pub struct OwnedLightingWork {
    sections: usize,
    height: usize,
    opacity: Vec<u8>,
    block: Vec<u8>,
    sky: Vec<u8>,
    queue: VecDeque<usize>,
    layer: u8,
    seed_cursor: usize,
}

impl OwnedLightingWork {
    pub fn start(
        halo: [&[BlockPalette]; 9],
        config: LightingEngineConfig,
        has_skylight: bool,
    ) -> Result<Self, OwnedLightError> {
        let sections = halo[4].len();
        if sections == 0 || sections > 32 {
            return Err(OwnedLightError::Height);
        }
        if halo.iter().any(|chunk| chunk.len() != sections) {
            return Err(OwnedLightError::IncompleteHalo);
        }
        let height = sections * 16;
        let cells = height * 48 * 48;
        let index = |x: usize, y: usize, z: usize| (y * 48 + z) * 48 + x;
        let mut opacity = vec![0_u8; cells];
        let mut block = vec![0_u8; cells];
        let mut sky = vec![0_u8; cells];
        for x in 0..48 {
            for z in 0..48 {
                let palettes = halo[(x / 16) * 3 + z / 16];
                let mut column = if has_skylight { 15_u8 } else { 0 };
                for y in (0..height).rev() {
                    let id = palettes[y / 16].get(x % 16, y % 16, z % 16);
                    let state = id.to_state();
                    let at = index(x, y, z);
                    opacity[at] = if id == BlockStateId::AIR {
                        0
                    } else if state.can_occlude() {
                        state.opacity.max(1)
                    } else {
                        state.opacity
                    };
                    match config {
                        LightingEngineConfig::Dark => {}
                        LightingEngineConfig::Full => {
                            block[at] = 15;
                            sky[at] = 15;
                        }
                        LightingEngineConfig::Default => {
                            block[at] = state.luminance;
                            column = column.saturating_sub(opacity[at]);
                            sky[at] = column;
                        }
                    }
                }
            }
        }
        Ok(Self {
            sections,
            height,
            opacity,
            block,
            sky,
            queue: VecDeque::new(),
            seed_cursor: 0,
            layer: if config == LightingEngineConfig::Default {
                0
            } else {
                2
            },
        })
    }

    /// Performs at most `budget` cell/edge visits. A false return preserves the
    /// exact work frontier for the next owner turn.
    pub fn advance(&mut self, mut budget: usize) -> bool {
        while self.layer < 2 {
            let skylight = self.layer == 1;
            let light = if skylight {
                &mut self.sky
            } else {
                &mut self.block
            };
            while self.seed_cursor < light.len() && budget > 0 {
                let at = self.seed_cursor;
                self.seed_cursor += 1;
                budget -= 1;
                if light[at] > 1 {
                    self.queue.push_back(at);
                }
            }
            if self.seed_cursor != light.len() {
                return false;
            }
            while !self.queue.is_empty() {
                if budget < 6 {
                    return false;
                }
                budget -= 6;
                let at = self.queue.pop_front().expect("nonempty light queue");
                let x = at % 48;
                let z = (at / 48) % 48;
                let y = at / (48 * 48);
                let source = light[at];
                let neighbors = [
                    (x > 0).then(|| (at - 1, false)),
                    (x < 47).then(|| (at + 1, false)),
                    (z > 0).then(|| (at - 48, false)),
                    (z < 47).then(|| (at + 48, false)),
                    (y > 0).then(|| (at - 48 * 48, true)),
                    (y + 1 < self.height).then(|| (at + 48 * 48, false)),
                ];
                for (next, down) in neighbors.into_iter().flatten() {
                    let value = if skylight && down && source == 15 && self.opacity[next] == 0 {
                        15
                    } else {
                        source.saturating_sub(self.opacity[next].max(1))
                    };
                    if value > light[next] {
                        light[next] = value;
                        if value > 1 {
                            self.queue.push_back(next);
                        }
                    }
                }
            }
            self.layer += 1;
            self.seed_cursor = 0;
        }
        true
    }

    pub fn finish(self) -> Option<OwnedChunkLighting> {
        if self.layer != 2 {
            return None;
        }
        let index = |x: usize, y: usize, z: usize| (y * 48 + z) * 48 + x;
        let pack = |light: &[u8]| {
            (0..self.sections)
                .map(|section| {
                    let mut data = Box::new([0_u8; 2048]);
                    for y in 0..16 {
                        for z in 0..16 {
                            for x in 0..16 {
                                let local = (y * 16 + z) * 16 + x;
                                let value = light[index(x + 16, section * 16 + y, z + 16)];
                                data[local / 2] |= value << ((local % 2) * 4);
                            }
                        }
                    }
                    data
                })
                .collect()
        };
        Some(OwnedChunkLighting {
            block: pack(&self.block),
            sky: pack(&self.sky),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_data::Block;

    fn at(data: &[Box<[u8; 2048]>], x: usize, y: usize, z: usize) -> u8 {
        let index = ((y % 16) * 16 + z) * 16 + x;
        (data[y / 16][index / 2] >> ((index % 2) * 4)) & 15
    }

    #[test]
    fn yielded_work_matches_one_pass_and_honors_full_dark_modes() {
        let mut chunks = (0..9)
            .map(|_| vec![BlockPalette::default()])
            .collect::<Vec<_>>();
        chunks[4][0].set(4, 8, 4, Block::GLOWSTONE.default_state.id);
        let halo = std::array::from_fn(|index| chunks[index].as_slice());
        let expected =
            OwnedChunkLighting::resolve(halo, LightingEngineConfig::Default, true, 10_000_000)
                .unwrap();
        let mut work = OwnedLightingWork::start(halo, LightingEngineConfig::Default, true).unwrap();
        assert!(!work.advance(0));
        let mut turns = 0;
        while !work.advance(113) {
            turns += 1;
            assert!(turns < 100_000, "bounded work must make progress");
        }
        assert!(turns > 1);
        let actual = work.finish().unwrap();
        assert_eq!(actual.block, expected.block);
        assert_eq!(actual.sky, expected.sky);
        let no_sky =
            OwnedChunkLighting::resolve(halo, LightingEngineConfig::Default, false, 10_000_000)
                .unwrap();
        assert_eq!(at(&no_sky.sky, 8, 0, 8), 0);
        assert_eq!(at(&no_sky.block, 4, 8, 4), 15);
        for (config, value) in [
            (LightingEngineConfig::Full, 15),
            (LightingEngineConfig::Dark, 0),
        ] {
            let mut work = OwnedLightingWork::start(halo, config, true).unwrap();
            assert!(work.advance(0));
            let result = work.finish().unwrap();
            assert_eq!(at(&result.block, 0, 0, 0), value);
            assert_eq!(at(&result.sky, 0, 0, 0), value);
        }
    }

    #[test]
    fn halo_source_removal_and_occlusion_are_recomputed_from_one_cut() {
        let mut chunks = (0..9)
            .map(|_| vec![BlockPalette::default()])
            .collect::<Vec<_>>();
        chunks[1][0].set(15, 8, 8, Block::GLOWSTONE.default_state.id);
        let solve = |chunks: &[Vec<BlockPalette>]| {
            OwnedChunkLighting::resolve(
                std::array::from_fn(|index| chunks[index].as_slice()),
                LightingEngineConfig::Default,
                true,
                10_000_000,
            )
            .unwrap()
        };
        let lit = solve(&chunks);
        assert_eq!(at(&lit.block, 0, 8, 8), 14);
        assert_eq!(at(&lit.sky, 8, 0, 8), 15);
        chunks[4][0].set(0, 8, 8, Block::STONE.default_state.id);
        let blocked = solve(&chunks);
        assert_eq!(at(&blocked.block, 0, 8, 8), 0);
        assert!(at(&blocked.block, 1, 8, 8) < at(&lit.block, 1, 8, 8));
        chunks[1][0].set(15, 8, 8, BlockStateId::AIR);
        assert_eq!(at(&solve(&chunks).block, 1, 8, 8), 0);
        assert!(matches!(
            OwnedChunkLighting::resolve(
                std::array::from_fn(|index| chunks[index].as_slice()),
                LightingEngineConfig::Default,
                true,
                1,
            ),
            Err(OwnedLightError::Budget)
        ));
        chunks[0].clear();
        assert!(matches!(
            OwnedChunkLighting::resolve(
                std::array::from_fn(|index| chunks[index].as_slice()),
                LightingEngineConfig::Default,
                true,
                10_000_000,
            ),
            Err(OwnedLightError::IncompleteHalo)
        ));
    }
}
