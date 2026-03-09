use std::collections::HashMap;
use std::fmt;

use serde::de;
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

/// Visitor that accepts either a single string or a list of strings,
/// rejecting non-string scalars (bool, int, float, null).
struct StringOrVecVisitor;

impl<'de> de::Visitor<'de> for StringOrVecVisitor {
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

/// `DeserializeSeed` adapter so `StringOrVecVisitor` can be used for map values.
struct StringOrVecSeed;

impl<'de> de::DeserializeSeed<'de> for StringOrVecSeed {
    type Value = Vec<String>;

    fn deserialize<D>(self, deserializer: D) -> Result<Vec<String>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StringOrVecVisitor)
    }
}

pub(super) fn deserialize_strict_hooks_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StrictHooksMapVisitor;

    impl<'de> de::Visitor<'de> for StrictHooksMapVisitor {
        type Value = HashMap<String, Vec<String>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a map of hook keys to strings or lists of strings")
        }

        fn visit_map<A: de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<HashMap<String, Vec<String>>, A::Error> {
            let mut result = HashMap::new();
            while let Some(key) = map.next_key::<StrictString>()? {
                let value = map.next_value_seed(StringOrVecSeed)?;
                result.insert(key.into_inner(), value);
            }
            Ok(result)
        }
    }

    deserializer.deserialize_map(StrictHooksMapVisitor)
}

/// Deserialize a YAML field that can be either a single string or a list of strings.
pub(super) fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_any(StringOrVecVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct OptionalStrictStringTest {
        #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
        value: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct OptionalDurationStringTest {
        #[serde(default, deserialize_with = "deserialize_optional_duration_string")]
        value: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct OptionalVecStrictStringTest {
        #[serde(
            default,
            deserialize_with = "deserialize_optional_vec_strict_string"
        )]
        value: Option<Vec<String>>,
    }

    #[test]
    fn optional_strict_string_rejects_null() {
        let err = serde_yaml::from_str::<OptionalStrictStringTest>("value: ~")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(NULL_VALUE_ERROR),
            "expected NULL_VALUE_ERROR, got: {err}"
        );
    }

    #[test]
    fn optional_strict_string_accepts_string() {
        let result: OptionalStrictStringTest =
            serde_yaml::from_str("value: \"hello\"").unwrap();
        assert_eq!(result.value, Some("hello".to_string()));
    }

    #[test]
    fn optional_duration_string_rejects_null() {
        let err = serde_yaml::from_str::<OptionalDurationStringTest>("value: null")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(NULL_VALUE_ERROR),
            "expected NULL_VALUE_ERROR, got: {err}"
        );
    }

    #[test]
    fn optional_duration_string_rejects_bool() {
        let err = serde_yaml::from_str::<OptionalDurationStringTest>("value: true")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(STRICT_STRING_ERROR),
            "expected STRICT_STRING_ERROR, got: {err}"
        );
    }

    #[test]
    fn optional_duration_string_rejects_float() {
        let err = serde_yaml::from_str::<OptionalDurationStringTest>("value: 3.14")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(STRICT_STRING_ERROR),
            "expected STRICT_STRING_ERROR, got: {err}"
        );
    }

    #[test]
    fn optional_duration_string_accepts_int() {
        let result: OptionalDurationStringTest =
            serde_yaml::from_str("value: 42").unwrap();
        assert_eq!(result.value, Some("42".to_string()));
    }

    #[test]
    fn optional_vec_strict_string_rejects_null() {
        let err = serde_yaml::from_str::<OptionalVecStrictStringTest>("value: null")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(NULL_VALUE_ERROR),
            "expected NULL_VALUE_ERROR, got: {err}"
        );
    }
}
