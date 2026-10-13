use std::marker::PhantomData;

use serde::Deserialize;
use serde::de::{Deserializer, Error, IgnoredAny, SeqAccess, Visitor};

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T>(PhantomData<T>);

    impl<'de, T: Deserialize<'de>> Visitor<'de> for Bounded<T> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bounded logical records")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
            let maximum = crate::canonical_action::MAX_LOGICAL_RECORDS;
            let declared = sequence.size_hint();
            if declared.is_some_and(|count| count > maximum) {
                return Err(A::Error::custom("logical record count exceeds bound"));
            }
            let element_bytes = std::mem::size_of::<T>();
            let initial_capacity = if element_bytes == 0 {
                0
            } else {
                declared
                    .unwrap_or(0)
                    .min(4096)
                    .min((1024 * 1024) / element_bytes)
            };
            let mut entries = Vec::with_capacity(initial_capacity);
            loop {
                if entries.len() == maximum {
                    if sequence.next_element::<IgnoredAny>()?.is_some() {
                        return Err(A::Error::custom("logical record count exceeds bound"));
                    }
                    break;
                }
                match sequence.next_element()? {
                    Some(entry) => entries.push(entry),
                    None => break,
                }
            }
            Ok(entries)
        }
    }

    deserializer.deserialize_seq(Bounded(PhantomData))
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Bounded {
        #[serde(deserialize_with = "super::deserialize")]
        values: Vec<u32>,
    }

    #[derive(Serialize)]
    struct Original {
        values: Vec<u32>,
    }

    #[test]
    fn postcard_preserves_existing_vector_wire_and_values() {
        let values = vec![0, 127, 128, u32::MAX];
        let original = postcard::to_allocvec(&Original {
            values: values.clone(),
        })
        .unwrap();
        let bounded = Bounded { values };
        assert_eq!(postcard::to_allocvec(&bounded).unwrap(), original);
        assert_eq!(postcard::from_bytes::<Bounded>(&original).unwrap(), bounded);
    }

    #[test]
    fn oversized_declared_length_rejects_before_element_deserialization() {
        struct UnreachableElement;
        impl<'de> Deserialize<'de> for UnreachableElement {
            fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
                panic!("oversized vector reached element deserialization")
            }
        }
        #[derive(Deserialize)]
        struct Malformed {
            #[serde(deserialize_with = "super::deserialize")]
            _values: Vec<UnreachableElement>,
        }
        let count = crate::canonical_action::MAX_LOGICAL_RECORDS + 1;
        let mut bytes = postcard::to_allocvec(&count).unwrap();
        bytes.resize(bytes.len() + count, 0);
        assert!(postcard::from_bytes::<Malformed>(&bytes).is_err());
    }
}
