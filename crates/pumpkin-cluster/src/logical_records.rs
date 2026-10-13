use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant};

use crate::canonical_action::{ApplyFailure, MAX_LOGICAL_RECORDS};

#[derive(Clone, Copy)]
struct Count { records: usize, byte: bool }

#[derive(Debug)]
struct Limit;

impl std::fmt::Display for Limit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("logical record limit exceeded") }
}
impl std::error::Error for Limit {}
impl serde::ser::Error for Limit {
    fn custom<T: std::fmt::Display>(_: T) -> Self { Self }
}

struct Counter;
struct Compound { records: usize, entries: bool }

impl Compound {
    fn add<T: Serialize + ?Sized>(&mut self, value: &T, entry: bool) -> Result<(), Limit> {
        let count = value.serialize(Counter)?;
        self.records = self.records.checked_add(count.records).and_then(|records|
            records.checked_add(usize::from(entry && !count.byte))).ok_or(Limit)?;
        if self.records > MAX_LOGICAL_RECORDS { return Err(Limit); }
        Ok(())
    }
    fn finish(self) -> Result<Count, Limit> { Ok(Count { records: self.records, byte: false }) }
}

macro_rules! scalar {
    ($($name:ident($kind:ty)),* $(,)?) => {$ (
        fn $name(self, _: $kind) -> Result<Count, Limit> { Ok(Count { records: 0, byte: false }) }
    )*};
}

impl serde::Serializer for Counter {
    type Ok = Count;
    type Error = Limit;
    type SerializeSeq = Compound;
    type SerializeTuple = Compound;
    type SerializeTupleStruct = Compound;
    type SerializeTupleVariant = Compound;
    type SerializeMap = Compound;
    type SerializeStruct = Compound;
    type SerializeStructVariant = Compound;
    scalar!(serialize_bool(bool), serialize_i8(i8), serialize_i16(i16), serialize_i32(i32), serialize_i64(i64), serialize_i128(i128),
        serialize_u16(u16), serialize_u32(u32), serialize_u64(u64), serialize_u128(u128), serialize_f32(f32), serialize_f64(f64), serialize_char(char));
    fn serialize_u8(self, _: u8) -> Result<Count, Limit> { Ok(Count { records: 0, byte: true }) }
    fn serialize_str(self, _: &str) -> Result<Count, Limit> { Ok(Count { records: 0, byte: false }) }
    fn serialize_bytes(self, _: &[u8]) -> Result<Count, Limit> { Ok(Count { records: 0, byte: false }) }
    fn serialize_none(self) -> Result<Count, Limit> { self.serialize_unit() }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Count, Limit> { value.serialize(self).map(|count| Count { byte: false, ..count }) }
    fn serialize_unit(self) -> Result<Count, Limit> { Ok(Count { records: 0, byte: false }) }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Count, Limit> { self.serialize_unit() }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<Count, Limit> { self.serialize_unit() }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, value: &T) -> Result<Count, Limit> { value.serialize(self).map(|count| Count { byte: false, ..count }) }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, value: &T) -> Result<Count, Limit> { value.serialize(self).map(|count| Count { byte: false, ..count }) }
    fn serialize_seq(self, _: Option<usize>) -> Result<Compound, Limit> { Ok(Compound { records: 0, entries: true }) }
    fn serialize_tuple(self, _: usize) -> Result<Compound, Limit> { self.serialize_seq(None) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Compound, Limit> { self.serialize_seq(None) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Compound, Limit> { self.serialize_seq(None) }
    fn serialize_map(self, _: Option<usize>) -> Result<Compound, Limit> { self.serialize_seq(None) }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Compound, Limit> { Ok(Compound { records: 0, entries: false }) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Compound, Limit> { self.serialize_struct("", 0) }
    fn is_human_readable(&self) -> bool { false }
}
impl SerializeSeq for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Limit> { self.add(value, true) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}
impl SerializeTuple for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Limit> { self.add(value, true) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}
impl SerializeTupleStruct for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Limit> { self.add(value, true) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}
impl SerializeTupleVariant for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Limit> { self.add(value, true) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}
impl SerializeMap for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Limit> {
        self.records = self.records.checked_add(1).ok_or(Limit)?;
        self.add(value, false)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Limit> { self.add(value, false) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}
impl SerializeStruct for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> Result<(), Limit> { self.add(value, self.entries) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}
impl SerializeStructVariant for Compound {
    type Ok = Count; type Error = Limit;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> Result<(), Limit> { self.add(value, self.entries) }
    fn end(self) -> Result<Count, Limit> { self.finish() }
}

pub fn count<T: Serialize + ?Sized>(value: &T) -> Result<usize, ApplyFailure> {
    let count = value.serialize(Counter).map_err(|_| ApplyFailure::ResourceLimit)?.records;
    count.checked_add(1).filter(|count| *count <= MAX_LOGICAL_RECORDS).ok_or(ApplyFailure::ResourceLimit)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_value_layout_remains_bounded() {
        let sizes = [std::mem::size_of::<crate::canonical_action::Action>(),
            std::mem::size_of::<crate::canonical_action::Effect>(),
            std::mem::size_of::<crate::canonical_action::Expected>(),
            std::mem::size_of::<crate::canonical_action::Value>()];
        eprintln!("canonical Action/Effect/Expected/Value layout bytes: {sizes:?}");
        let cell = std::mem::size_of::<crate::canonical_action::Cell>();
        let entry = std::mem::size_of::<(crate::canonical_action::Target, Option<crate::canonical_action::Cell>)>();
        eprintln!("canonical Cell/map entry/43000 slot inline bytes: {cell} {entry} {}", entry * 43000);
        assert!(sizes[0] <= 4 * 1024);
        assert!(sizes[1..].iter().all(|size| *size <= 4 * 1024));
    }

    #[test]
    fn nested_records_are_bounded_without_treating_item_bytes_as_records() {
        let bytes = vec![0u8; MAX_LOGICAL_RECORDS + 1];
        assert_eq!(count(&bytes), Ok(1));
        let nested = vec![vec![false; 1024]; 1024];
        assert_eq!(count(&nested), Err(ApplyFailure::ResourceLimit));
        assert_eq!(count(&vec![vec![0u8; 100]; 3]), Ok(4));
    }
}
