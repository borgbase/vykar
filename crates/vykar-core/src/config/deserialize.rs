use std::collections::HashMap;
use std::fmt;

use serde::Deserialize;

pub(super) const STRICT_STRING_ERROR: &str = "string value must be quoted";
pub(super) const NULL_VALUE_ERROR: &str =
    "value cannot be null or empty; provide a value or omit the field";

/// Generates `visit_*` methods that reject non-string YAML scalars (bool, int, float, null)
/// with [`STRICT_STRING_ERROR`]. Use inside a `serde::de::Visitor` impl block.
macro_rules! reject_non_string_visits {
    ($out:ty) => {
        fn visit_bool<E: de::Error>(self, _v: bool) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
        fn visit_i64<E: de::Error>(self, _v: i64) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
        fn visit_u64<E: de::Error>(self, _v: u64) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
        fn visit_i128<E: de::Error>(self, _v: i128) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
        fn visit_u128<E: de::Error>(self, _v: u128) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
        fn visit_f64<E: de::Error>(self, _v: f64) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
        fn visit_unit<E: de::Error>(self) -> Result<$out, E> {
            Err(E::custom(STRICT_STRING_ERROR))
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct StrictString(String);

impl StrictString {
    pub(super) fn into_inner(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for StrictString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct StrictStringVisitor;

        impl<'de> de::Visitor<'de> for StrictStringVisitor {
            type Value = StrictString;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<StrictString, E> {
                Ok(StrictString(v.to_string()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<StrictString, E> {
                Ok(StrictString(v))
            }

            reject_non_string_visits!(StrictString);
        }

        deserializer.deserialize_any(StrictStringVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DurationString(String);

impl DurationString {
    pub(super) fn into_inner(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for DurationString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct DurationStringVisitor;

        impl<'de> de::Visitor<'de> for DurationStringVisitor {
            type Value = DurationString;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a duration string or integer")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<DurationString, E> {
                Ok(DurationString(v.to_string()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<DurationString, E> {
                Ok(DurationString(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<DurationString, E> {
                Ok(DurationString(v.to_string()))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<DurationString, E> {
                Ok(DurationString(v.to_string()))
            }

            fn visit_i128<E: de::Error>(self, v: i128) -> Result<DurationString, E> {
                Ok(DurationString(v.to_string()))
            }

            fn visit_u128<E: de::Error>(self, v: u128) -> Result<DurationString, E> {
                Ok(DurationString(v.to_string()))
            }

            fn visit_bool<E: de::Error>(self, _v: bool) -> Result<DurationString, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }

            fn visit_f64<E: de::Error>(self, _v: f64) -> Result<DurationString, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }

            fn visit_unit<E: de::Error>(self) -> Result<DurationString, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }
        }

        deserializer.deserialize_any(DurationStringVisitor)
    }
}

pub(super) fn deserialize_strict_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    StrictString::deserialize(deserializer).map(StrictString::into_inner)
}

pub(super) fn deserialize_optional_strict_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    match Option::<StrictString>::deserialize(deserializer)? {
        Some(v) => Ok(Some(v.into_inner())),
        None => Err(D::Error::custom(NULL_VALUE_ERROR)),
    }
}

pub(super) fn deserialize_vec_strict_string<'de, D>(
    deserializer: D,
) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = Vec::<StrictString>::deserialize(deserializer)?;
    Ok(values.into_iter().map(StrictString::into_inner).collect())
}

pub(super) fn deserialize_optional_vec_strict_string<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    match Option::<Vec<StrictString>>::deserialize(deserializer)? {
        Some(values) => Ok(Some(
            values
                .into_iter()
                .map(StrictString::into_inner)
                .collect::<Vec<_>>(),
        )),
        None => Err(D::Error::custom(NULL_VALUE_ERROR)),
    }
}

pub(super) fn deserialize_duration_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    DurationString::deserialize(deserializer).map(DurationString::into_inner)
}

pub(super) fn deserialize_optional_duration_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    match Option::<DurationString>::deserialize(deserializer)? {
        Some(v) => Ok(Some(v.into_inner())),
        None => Err(D::Error::custom(NULL_VALUE_ERROR)),
    }
}

pub(super) fn deserialize_strict_hooks_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = HashMap::<StrictString, Vec<StrictString>>::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .map(|(k, v)| {
            (
                k.into_inner(),
                v.into_iter().map(StrictString::into_inner).collect(),
            )
        })
        .collect())
}

/// Deserialize a YAML field that can be either a single string or a list of strings.
pub(super) fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a string or a list of strings")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Vec<String>, E> {
            Ok(vec![v.to_string()])
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Vec<String>, E> {
            Ok(vec![v])
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<String>, A::Error> {
            let mut v = Vec::new();
            while let Some(s) = seq.next_element::<StrictString>()? {
                v.push(s.into_inner());
            }
            Ok(v)
        }

        reject_non_string_visits!(Vec<String>);
    }

    deserializer.deserialize_any(StringOrVec)
}
