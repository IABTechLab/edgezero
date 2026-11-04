//! Non-materializing traversal through the reader's bounded normal decoder.
//! Unlike `IgnoredAny`, this follows every pointer. The identifier entry point
//! rejects pointer chains before dispatching non-string values to `deserialize_any`.
//! Calling `deserialize_any` directly would retain the upstream pointer-chain gap.

#![expect(
    clippy::missing_trait_methods,
    reason = "Serde defaults reject types that are not emitted by the pinned MMDB decoder"
)]

use std::fmt;

use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

pub(super) struct StructuralValue;

impl<'de> Deserialize<'de> for StructuralValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Pinned reader's try_read_identifier_bytes checks pointer targets even
        // for non-strings, then rewinds and uses normal any decoding. Strings
        // arrive as borrowed bytes; verify() has already checked their UTF-8.
        deserializer.deserialize_identifier(StructuralVisitor)
    }
}

struct StructuralVisitor;

impl<'de> Visitor<'de> for StructuralVisitor {
    type Value = StructuralValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an MMDB value with string map keys")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // &str uses strict string decoding, not deserialize_identifier, which
        // may accept numeric struct field identifiers.
        while map.next_key::<&'de str>()?.is_some() {
            map.next_value::<StructuralValue>()?;
        }
        Ok(StructuralValue)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<StructuralValue>()?.is_some() {}
        Ok(StructuralValue)
    }

    fn visit_borrowed_str<E>(self, _: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_borrowed_bytes<E>(self, _: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_i32<E>(self, _: i32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_u16<E>(self, _: u16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_u32<E>(self, _: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_u128<E>(self, _: u128) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_f32<E>(self, _: f32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(StructuralValue)
    }
}
