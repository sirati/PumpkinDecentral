use serde::{Deserialize, Serialize};

use crate::protocol::{BlockPos, ChunkKey};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorldPresentationIntent {
    Explosion {
        chunk: ChunkKey,
        center: [u64; 3],
        power: u32,
        block_count: u32,
    },
    CustomSound {
        chunk: ChunkKey,
        position: BlockPos,
        name: String,
        range_milli: Option<u32>,
        volume_milli: u16,
        pitch_milli: u16,
        category: u8,
    },
    Sound {
        chunk: ChunkKey,
        position: BlockPos,
        sound: u16,
        volume_milli: u16,
        pitch_milli: u16,
        category: u8,
    },
    GameEvent {
        chunk: ChunkKey,
        position: BlockPos,
        event: u16,
    },
    BreakParticles {
        chunk: ChunkKey,
        position: BlockPos,
        state: u16,
    },
    WorldEvent {
        chunk: ChunkKey,
        position: BlockPos,
        event: u16,
        data: i32,
    },
}

impl WorldPresentationIntent {
    pub fn chunk(&self) -> ChunkKey {
        match self {
            Self::WorldEvent { chunk, .. }
            | Self::Explosion { chunk, .. }
            | Self::Sound { chunk, .. }
            | Self::CustomSound { chunk, .. }
            | Self::GameEvent { chunk, .. }
            | Self::BreakParticles { chunk, .. } => *chunk,
        }
    }

    pub fn valid(&self) -> bool {
        if let Self::WorldEvent { event, data, .. } = self {
            if !matches!(*event, 3004 | 3005) || *data != 0 {
                return false;
            }
        }
        if let Self::Explosion {
            chunk,
            center,
            power,
            block_count,
        } = self
        {
            let center = center.map(f64::from_bits);
            let power = f32::from_bits(*power);
            return center
                .iter()
                .all(|value| value.is_finite() && value.abs() < 30_000_000.0)
                && power.is_finite()
                && (0.0..=128.0).contains(&power)
                && *block_count <= 4096
                && (center[0].floor() as i32).div_euclid(16) == chunk.x()
                && (center[2].floor() as i32).div_euclid(16) == chunk.z();
        }
        if matches!(self, Self::Sound { category, .. } | Self::CustomSound { category, .. } if *category > 10)
        {
            return false;
        }
        if let Self::CustomSound {
            name, range_milli, ..
        } = self
        {
            if name.is_empty()
                || name.len() > 256
                || !name.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b':' | b'/' | b'_' | b'-' | b'.')
                })
                || range_milli.is_some_and(|range| range == 0 || range > 128_000)
            {
                return false;
            }
        }
        let position = match self {
            Self::Explosion { .. } => return false,
            Self::WorldEvent { position, .. }
            | Self::Sound { position, .. }
            | Self::CustomSound { position, .. }
            | Self::GameEvent { position, .. }
            | Self::BreakParticles { position, .. } => *position,
        };
        position.x.div_euclid(16) == self.chunk().x()
            && position.z.div_euclid(16) == self.chunk().z()
    }
}
