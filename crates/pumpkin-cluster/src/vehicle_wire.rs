use serde::{Deserialize, Serialize};

use crate::actor_relation::VehicleControlInput;
use crate::realm_completion::PeerGeneration;

const MAGIC: [u8; 4] = *b"VHCL";
const MAX_FRAME: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleControlFrame {
    pub source: PeerGeneration,
    pub recipient: PeerGeneration,
    pub input: VehicleControlInput,
}

impl VehicleControlFrame {
    pub fn valid(&self) -> bool {
        self.source.generation != 0
            && self.recipient.generation != 0
            && self.input.driver.gid.server == self.source.server
            && self.input.is_valid()
    }
}

pub fn recognizes(bytes: &[u8]) -> bool {
    bytes.starts_with(&MAGIC)
}

pub fn encode(frame: &VehicleControlFrame) -> Option<Vec<u8>> {
    if !frame.valid() {
        return None;
    }
    let mut bytes = MAGIC.to_vec();
    bytes.extend(postcard::to_allocvec(frame).ok()?);
    (bytes.len() <= MAX_FRAME).then_some(bytes)
}

pub fn decode(bytes: &[u8]) -> Option<VehicleControlFrame> {
    if !recognizes(bytes) || bytes.len() > MAX_FRAME {
        return None;
    }
    let frame: VehicleControlFrame = postcard::from_bytes(&bytes[MAGIC.len()..]).ok()?;
    frame.valid().then_some(frame)
}
