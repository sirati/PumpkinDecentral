use pumpkin_data::packet::serverbound::play::SET_CREATIVE_MODE_SLOT;
use pumpkin_macros::java_packet;
use pumpkin_util::version::JavaMinecraftVersion;

use crate::{
    ServerPacket,
    codec::item_stack_seralizer::ItemStackSerializer,
    ser::{NetworkReadExt, ReadingError},
};

#[java_packet(SET_CREATIVE_MODE_SLOT)]
pub struct SSetCreativeSlot {
    pub slot: i16,
    pub clicked_item: ItemStackSerializer<'static>,
}

impl SSetCreativeSlot {
    #[must_use]
    pub const fn new(slot: i16, clicked_item: ItemStackSerializer<'static>) -> Self {
        Self { slot, clicked_item }
    }
}

impl<'a> ServerPacket<'a> for SSetCreativeSlot {
    fn read(mut read: &mut &'a [u8], version: &JavaMinecraftVersion) -> Result<Self, ReadingError> {
        let slot = if *version >= JavaMinecraftVersion::V_1_20_5 {
            read.get_u16_be()? as i16
        } else {
            read.get_i16_be()?
        };
        let clicked_item = ItemStackSerializer::read_untrusted_with_version(&mut read, version)?;
        Ok(Self { slot, clicked_item })
    }
}

impl crate::ClientPacket for SSetCreativeSlot {
    fn write_packet_data(
        &self,
        mut write: impl std::io::Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), crate::ser::WritingError> {
        use crate::ser::NetworkWriteExt;
        write.write_i16_be(self.slot)?;
        if *version >= JavaMinecraftVersion::V_1_21_5 {
            self.clicked_item
                .write_length_prefixed_with_version(&mut write, version)?;
        } else {
            self.clicked_item.write_with_version(&mut write, version)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientPacket;
    use pumpkin_data::{item::Item, item_stack::ItemStack};

    #[test]
    fn creative_packet_decodes_versioned_components_once() {
        for version in [
            JavaMinecraftVersion::V_1_20_5,
            JavaMinecraftVersion::V_1_21_5,
            JavaMinecraftVersion::V_26_1,
            JavaMinecraftVersion::V_26_2,
        ] {
            let mut stack = ItemStack::new(2, &Item::STONE);
            stack.set_custom_name("alpha".to_owned());
            let packet = SSetCreativeSlot::new(36, ItemStackSerializer::from(stack));
            let mut bytes = Vec::new();
            packet.write_packet_data(&mut bytes, &version).unwrap();
            bytes.push(127);
            let mut read = bytes.as_slice();
            let decoded = SSetCreativeSlot::read(&mut read, &version).unwrap();
            assert_eq!(decoded.slot, 36);
            let stack = decoded.clicked_item.to_stack();
            assert_eq!(stack.item.id, Item::STONE.id);
            assert_eq!(stack.item_count, 2);
            assert_eq!(stack.get_custom_name().unwrap().clone().get_text(), "alpha");
            assert_eq!(read, &[127]);
        }
    }
}
