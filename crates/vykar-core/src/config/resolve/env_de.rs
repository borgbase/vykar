//! Environment placeholder expansion during deserialization.
//!
//! `from_str_expanding` parses the config twice. The first parse expands
//! nothing: it only records the key paths of the string values that contain
//! a placeholder. The second parse wraps the YAML deserializer in
//! `ExpandingDeserializer`, which replaces the placeholders of every string
//! scalar delivered as a value. Where a number or boolean setting sits at a
//! recorded path, it reads the scalar as text, expands it and parses the
//! result as that type. Mapping keys, comments and ignored values are never
//! expanded, and an expanded value is never parsed as YAML again.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::Deserialize;
use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, IgnoredAny, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};

use super::env::expand_env_placeholders;

/// Parse input as YAML into T, expanding environment placeholders in
/// values from overlay, then the process environment.
pub(super) fn from_str_expanding<T: serde::de::DeserializeOwned>(
    input: &str,
    overlay: &HashMap<String, String>,
) -> Result<T, serde_saphyr::Error> {
    let ctx = EnvExpansion::new(overlay, placeholder_paths(input));
    crate::config::yaml::from_str_with(input, |de| {
        T::deserialize(ExpandingDeserializer::new(de, &ctx))
    })
}

/// Joins key path segments: names with dots, [index] segments attached.
fn join_path(segments: &[String]) -> String {
    let mut out = String::new();
    for segment in segments {
        if !segment.starts_with('[') && !out.is_empty() {
            out.push('.');
        }
        out.push_str(segment);
    }
    out
}

/// The segment recorded for a mapping key that is not a string.
const NON_STRING_KEY: &str = "?";

// ---------------------------------------------------------------------------
// First parse: where are the placeholders?
// ---------------------------------------------------------------------------

/// Key paths of the string values that contain a placeholder. A document the
/// scan cannot read gives an empty set; the expanding parse then reports
/// the problem.
///
/// The empty set is only safe because of the caller. Called on its own, a
/// document whose scan fails (a duplicate key later in the file, say) would
/// make a valid `sftp_timeout: ${N}` fail with saphyr's `invalid u64`, since
/// typed requests no longer find their path in the set. `load_and_resolve`
/// never gets there: its `EnvFilePre` pre-parse buffers the whole document
/// through `deserialize_any` first and rejects every input that breaks the
/// scan.
fn placeholder_paths(input: &str) -> HashSet<String> {
    let mut paths = HashSet::new();
    if let Ok(node) = crate::config::yaml::from_str::<ScanNode>(input) {
        node.collect(&mut Vec::new(), &mut paths);
    }
    paths
}

/// What the scan keeps of a node: placeholder strings and the structure that leads to them.
enum ScanNode {
    Placeholder,
    Seq(Vec<ScanNode>),
    Map(Vec<(Option<String>, ScanNode)>),
    Other,
}

impl ScanNode {
    fn collect(&self, path: &mut Vec<String>, out: &mut HashSet<String>) {
        match self {
            Self::Placeholder => {
                out.insert(join_path(path));
            }
            Self::Seq(items) => {
                for (i, item) in items.iter().enumerate() {
                    path.push(format!("[{i}]"));
                    item.collect(path, out);
                    path.pop();
                }
            }
            Self::Map(entries) => {
                for (key, value) in entries {
                    path.push(key.clone().unwrap_or_else(|| NON_STRING_KEY.to_string()));
                    value.collect(path, out);
                    path.pop();
                }
            }
            Self::Other => {}
        }
    }
}

impl<'de> Deserialize<'de> for ScanNode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ScanVisitor;

        impl<'de> Visitor<'de> for ScanVisitor {
            type Value = ScanNode;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any YAML node")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<ScanNode, E> {
                Ok(if v.contains("${") {
                    ScanNode::Placeholder
                } else {
                    ScanNode::Other
                })
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_i128<E: de::Error>(self, _: i128) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_u128<E: de::Error>(self, _: u128) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_unit<E: de::Error>(self) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_none<E: de::Error>(self) -> Result<ScanNode, E> {
                Ok(ScanNode::Other)
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<ScanNode, D::Error> {
                ScanNode::deserialize(d)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ScanNode, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<ScanNode>()? {
                    items.push(item);
                }
                Ok(ScanNode::Seq(items))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ScanNode, A::Error> {
                let mut entries = Vec::new();
                while let Some(ScanKey(key)) = map.next_key::<ScanKey>()? {
                    entries.push((key, map.next_value::<ScanNode>()?));
                }
                Ok(ScanNode::Map(entries))
            }
        }

        deserializer.deserialize_any(ScanVisitor)
    }
}

/// A mapping key as the expanding parse records it: its text if it is a string.
struct ScanKey(Option<String>);

impl<'de> Deserialize<'de> for ScanKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = ScanKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a mapping key")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<ScanKey, E> {
                Ok(ScanKey(Some(v.to_owned())))
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_i128<E: de::Error>(self, _: i128) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_u128<E: de::Error>(self, _: u128) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_unit<E: de::Error>(self) -> Result<ScanKey, E> {
                Ok(ScanKey(None))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ScanKey, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(ScanKey(None))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ScanKey, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(ScanKey(None))
            }
        }

        deserializer.deserialize_any(KeyVisitor)
    }
}

// ---------------------------------------------------------------------------
// Second parse: the expanding deserializer
// ---------------------------------------------------------------------------

/// State shared by every layer of one expanding parse.
struct EnvExpansion<'o> {
    overlay: &'o HashMap<String, String>,
    /// Key paths of the values that contain a placeholder (from the scan).
    placeholders: HashSet<String>,
    /// Key path of the value being deserialized: names and [index] segments.
    path: RefCell<Vec<String>>,
    /// The last mapping key seen, consumed by the next value.
    pending_key: RefCell<Option<String>>,
}

impl<'o> EnvExpansion<'o> {
    fn new(overlay: &'o HashMap<String, String>, placeholders: HashSet<String>) -> Self {
        Self {
            overlay,
            placeholders,
            path: RefCell::new(Vec::new()),
            pending_key: RefCell::new(None),
        }
    }

    /// The current key path, e.g. repositories[0].url.
    fn path_string(&self) -> String {
        join_path(&self.path.borrow())
    }

    /// Whether the value being deserialized is a string with a placeholder.
    fn at_placeholder(&self) -> bool {
        !self.placeholders.is_empty() && self.placeholders.contains(&self.path_string())
    }

    fn with_segment<T>(&self, segment: String, f: impl FnOnce() -> T) -> T {
        self.path.borrow_mut().push(segment);
        let result = f();
        self.path.borrow_mut().pop();
        result
    }

    /// Expand one string value; an error names the key path.
    fn expand<E: de::Error>(&self, value: &str) -> Result<(String, Vec<String>), E> {
        match expand_env_placeholders(value, self.overlay) {
            Ok(expanded) => Ok((expanded.text, expanded.vars)),
            Err(message) => {
                let path = self.path_string();
                if path.is_empty() {
                    Err(E::custom(message))
                } else {
                    Err(E::custom(format!("{message} in '{path}'")))
                }
            }
        }
    }
}

/// The error for an expanded value its setting cannot take. It names the key
/// path and the variables, never the value, which can be a secret.
fn rejected<E: de::Error>(path: &str, vars: &[String], expected: &dyn fmt::Display) -> E {
    let vars = vars
        .iter()
        .map(|v| format!("${{{v}}}"))
        .collect::<Vec<_>>()
        .join(", ");
    E::custom(format!(
        "the value of '{path}' set from {vars} is invalid: expected {expected}"
    ))
}

/// Whether the wrapped node is a mapping key (recorded, never expanded) or a value.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Value,
    Key,
}

/// A Deserializer that expands environment placeholders in string values.
struct ExpandingDeserializer<'x, 'o, D> {
    inner: D,
    ctx: &'x EnvExpansion<'o>,
    role: Role,
}

impl<'x, 'o, D> ExpandingDeserializer<'x, 'o, D> {
    fn new(inner: D, ctx: &'x EnvExpansion<'o>) -> Self {
        Self {
            inner,
            ctx,
            role: Role::Value,
        }
    }

    fn wrap<V>(&self, inner: V) -> Wrapped<'x, 'o, V> {
        Wrapped {
            inner,
            ctx: self.ctx,
            role: self.role,
        }
    }

    /// Whether a number or boolean request must read the scalar as text.
    fn reads_text(&self) -> bool {
        self.role == Role::Value && self.ctx.at_placeholder()
    }
}

/// A visitor, seed or access wrapped so that what it reaches is expanded too.
struct Wrapped<'x, 'o, T> {
    inner: T,
    ctx: &'x EnvExpansion<'o>,
    role: Role,
}

impl<'x, 'o, T> Wrapped<'x, 'o, T> {
    fn wrap<U>(&self, inner: U) -> Wrapped<'x, 'o, U> {
        Wrapped {
            inner,
            ctx: self.ctx,
            role: self.role,
        }
    }

    fn deserializer<D>(&self, inner: D) -> ExpandingDeserializer<'x, 'o, D> {
        ExpandingDeserializer {
            inner,
            ctx: self.ctx,
            role: self.role,
        }
    }
}

macro_rules! forward_wrapped {
    ($($method:ident),* $(,)?) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            let visitor = self.wrap(visitor);
            self.inner.$method(visitor)
        }
    )*};
}

/// Number and boolean requests: at a placeholder path the scalar is taken as
/// text (deserialize_any) and parsed after expansion; elsewhere unchanged.
macro_rules! forward_scalar {
    ($($method:ident => $kind:ident),* $(,)?) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            if self.reads_text() {
                return self.inner.deserialize_any(FromText {
                    inner: visitor,
                    ctx: self.ctx,
                    kind: Kind::$kind,
                });
            }
            let visitor = self.wrap(visitor);
            self.inner.$method(visitor)
        }
    )*};
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for ExpandingDeserializer<'_, '_, D> {
    type Error = D::Error;

    forward_wrapped!(
        deserialize_any,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_seq,
        deserialize_map,
        deserialize_identifier,
    );

    forward_scalar!(
        deserialize_bool => Bool,
        deserialize_i8 => I8,
        deserialize_i16 => I16,
        deserialize_i32 => I32,
        deserialize_i64 => I64,
        deserialize_i128 => I128,
        deserialize_u8 => U8,
        deserialize_u16 => U16,
        deserialize_u32 => U32,
        deserialize_u64 => U64,
        deserialize_u128 => U128,
        deserialize_f32 => F32,
        deserialize_f64 => F64,
    );

    /// Ignored values are skipped unexpanded: an unused placeholder cannot fail a load.
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_ignored_any(visitor)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let visitor = self.wrap(visitor);
        self.inner.deserialize_unit_struct(name, visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let visitor = self.wrap(visitor);
        self.inner.deserialize_newtype_struct(name, visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let visitor = self.wrap(visitor);
        self.inner.deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let visitor = self.wrap(visitor);
        self.inner.deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let visitor = self.wrap(visitor);
        self.inner.deserialize_struct(name, fields, visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let visitor = self.wrap(visitor);
        self.inner.deserialize_enum(name, variants, visitor)
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

/// A number or boolean type a setting asked for.
#[derive(Clone, Copy)]
enum Kind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
}

impl Kind {
    /// The error for literal text that is not this type, worded as the YAML
    /// deserializer words its own.
    fn invalid_literal(self) -> String {
        let ty = match self {
            Self::Bool => return "invalid boolean (strict mode expects true/false)".to_string(),
            Self::I8 => "i8",
            Self::I16 => "i16",
            Self::I32 => "i32",
            Self::I64 => "i64",
            Self::I128 => "i128",
            Self::U8 => "u8",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::U128 => "u128",
            Self::F32 | Self::F64 => "floating point",
        };
        format!("invalid {ty}")
    }

    /// What an expanded value must be, for the error when it is not.
    fn expected(self) -> &'static str {
        match self {
            Self::Bool => "true or false",
            Self::I8 | Self::I16 | Self::I32 | Self::I64 | Self::I128 => "a whole number",
            Self::U8 | Self::U16 | Self::U32 | Self::U64 | Self::U128 => {
                "a non-negative whole number"
            }
            Self::F32 | Self::F64 => "a number",
        }
    }
}

/// Parse text as kind and hand it to visitor. Gives the visitor back when the
/// text is not a value of that type (or out of its range).
fn visit_parsed<'de, V: Visitor<'de>, E: de::Error>(
    kind: Kind,
    text: &str,
    visitor: V,
) -> Result<Result<V::Value, E>, V> {
    let t = text.trim();
    macro_rules! int {
        ($ty:ty, $visit:ident) => {
            match t.parse::<$ty>() {
                Ok(v) => Ok(visitor.$visit(v)),
                Err(_) => Err(visitor),
            }
        };
    }
    macro_rules! float {
        ($ty:ty, $visit:ident) => {
            match t.parse::<$ty>() {
                Ok(v) if v.is_finite() => Ok(visitor.$visit(v)),
                _ => Err(visitor),
            }
        };
    }
    match kind {
        Kind::Bool if t.eq_ignore_ascii_case("true") => Ok(visitor.visit_bool(true)),
        Kind::Bool if t.eq_ignore_ascii_case("false") => Ok(visitor.visit_bool(false)),
        Kind::Bool => Err(visitor),
        Kind::I8 => int!(i8, visit_i8),
        Kind::I16 => int!(i16, visit_i16),
        Kind::I32 => int!(i32, visit_i32),
        Kind::I64 => int!(i64, visit_i64),
        Kind::I128 => int!(i128, visit_i128),
        Kind::U8 => int!(u8, visit_u8),
        Kind::U16 => int!(u16, visit_u16),
        Kind::U32 => int!(u32, visit_u32),
        Kind::U64 => int!(u64, visit_u64),
        Kind::U128 => int!(u128, visit_u128),
        Kind::F32 => float!(f32, visit_f32),
        Kind::F64 => float!(f64, visit_f64),
    }
}

/// The visitor for a number or boolean setting at a placeholder path. It
/// receives the scalar as text, expands it and parses the result as kind.
struct FromText<'x, 'o, V> {
    inner: V,
    ctx: &'x EnvExpansion<'o>,
    kind: Kind,
}

impl<'de, V: Visitor<'de>> FromText<'_, '_, V> {
    fn read<E: de::Error>(self, raw: &str) -> Result<V::Value, E> {
        let (text, vars) = if raw.contains("${") {
            let (text, vars) = self.ctx.expand::<E>(raw)?;
            (Cow::Owned(text), vars)
        } else {
            (Cow::Borrowed(raw), Vec::new())
        };
        match visit_parsed::<V, E>(self.kind, &text, self.inner) {
            Ok(result) => result,
            Err(_) if vars.is_empty() => Err(E::custom(self.kind.invalid_literal())),
            Err(_) => Err(rejected(
                &self.ctx.path_string(),
                &vars,
                &self.kind.expected(),
            )),
        }
    }
}

impl<'de, V: Visitor<'de>> Visitor<'de> for FromText<'_, '_, V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(f)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<V::Value, E> {
        self.read(v)
    }
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<V::Value, E> {
        self.read(v)
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<V::Value, E> {
        self.read(&v)
    }

    // A scalar the YAML deserializer already typed (the path matched, the
    // node is not a placeholder string) goes through as it is.
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<V::Value, E> {
        self.inner.visit_bool(v)
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<V::Value, E> {
        self.inner.visit_i64(v)
    }
    fn visit_i128<E: de::Error>(self, v: i128) -> Result<V::Value, E> {
        self.inner.visit_i128(v)
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<V::Value, E> {
        self.inner.visit_u64(v)
    }
    fn visit_u128<E: de::Error>(self, v: u128) -> Result<V::Value, E> {
        self.inner.visit_u128(v)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<V::Value, E> {
        self.inner.visit_f64(v)
    }
    fn visit_unit<E: de::Error>(self) -> Result<V::Value, E> {
        self.inner.visit_unit()
    }
}

/// What a string scalar turned into.
enum Scalar {
    /// Deliver the original text.
    Unchanged,
    /// Deliver this expanded text; on rejection report the path and variables.
    Expanded { text: String, vars: Vec<String> },
}

impl<V> Wrapped<'_, '_, V> {
    fn classify<E: de::Error>(&self, value: &str) -> Result<Scalar, E> {
        match self.role {
            Role::Key => {
                *self.ctx.pending_key.borrow_mut() = Some(value.to_owned());
                Ok(Scalar::Unchanged)
            }
            Role::Value if !value.contains("${") => Ok(Scalar::Unchanged),
            Role::Value => {
                let (text, vars) = self.ctx.expand::<E>(value)?;
                Ok(Scalar::Expanded { text, vars })
            }
        }
    }
}

/// Formats a visitor's expecting text.
struct Expecting<'v, V>(&'v V);

impl<'de, V: Visitor<'de>> fmt::Display for Expecting<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }
}

/// Hand an expanded value to the target as a string. If the target rejects
/// it, the error names the key path and variables in place of the target's
/// own message, which may quote the value.
fn deliver_expanded<'de, V: Visitor<'de>, E: de::Error>(
    visitor: V,
    text: String,
    vars: &[String],
    ctx: &EnvExpansion<'_>,
) -> Result<V::Value, E> {
    let expecting = Expecting(&visitor).to_string();
    visitor
        .visit_string(text)
        .map_err(|_: E| rejected(&ctx.path_string(), vars, &expecting))
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Wrapped<'_, '_, V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(f)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<V::Value, E> {
        self.inner.visit_bool(v)
    }
    fn visit_i8<E: de::Error>(self, v: i8) -> Result<V::Value, E> {
        self.inner.visit_i8(v)
    }
    fn visit_i16<E: de::Error>(self, v: i16) -> Result<V::Value, E> {
        self.inner.visit_i16(v)
    }
    fn visit_i32<E: de::Error>(self, v: i32) -> Result<V::Value, E> {
        self.inner.visit_i32(v)
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<V::Value, E> {
        self.inner.visit_i64(v)
    }
    fn visit_i128<E: de::Error>(self, v: i128) -> Result<V::Value, E> {
        self.inner.visit_i128(v)
    }
    fn visit_u8<E: de::Error>(self, v: u8) -> Result<V::Value, E> {
        self.inner.visit_u8(v)
    }
    fn visit_u16<E: de::Error>(self, v: u16) -> Result<V::Value, E> {
        self.inner.visit_u16(v)
    }
    fn visit_u32<E: de::Error>(self, v: u32) -> Result<V::Value, E> {
        self.inner.visit_u32(v)
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<V::Value, E> {
        self.inner.visit_u64(v)
    }
    fn visit_u128<E: de::Error>(self, v: u128) -> Result<V::Value, E> {
        self.inner.visit_u128(v)
    }
    fn visit_f32<E: de::Error>(self, v: f32) -> Result<V::Value, E> {
        self.inner.visit_f32(v)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<V::Value, E> {
        self.inner.visit_f64(v)
    }
    fn visit_char<E: de::Error>(self, v: char) -> Result<V::Value, E> {
        self.inner.visit_char(v)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<V::Value, E> {
        match self.classify::<E>(v)? {
            Scalar::Unchanged => self.inner.visit_str(v),
            Scalar::Expanded { text, vars } => deliver_expanded(self.inner, text, &vars, self.ctx),
        }
    }
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<V::Value, E> {
        match self.classify::<E>(v)? {
            Scalar::Unchanged => self.inner.visit_borrowed_str(v),
            Scalar::Expanded { text, vars } => deliver_expanded(self.inner, text, &vars, self.ctx),
        }
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<V::Value, E> {
        match self.classify::<E>(&v)? {
            Scalar::Unchanged => self.inner.visit_string(v),
            Scalar::Expanded { text, vars } => deliver_expanded(self.inner, text, &vars, self.ctx),
        }
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<V::Value, E> {
        self.inner.visit_bytes(v)
    }
    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<V::Value, E> {
        self.inner.visit_borrowed_bytes(v)
    }
    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<V::Value, E> {
        self.inner.visit_byte_buf(v)
    }
    fn visit_none<E: de::Error>(self) -> Result<V::Value, E> {
        self.inner.visit_none()
    }
    fn visit_unit<E: de::Error>(self) -> Result<V::Value, E> {
        self.inner.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<V::Value, D::Error> {
        let d = self.deserializer(d);
        self.inner.visit_some(d)
    }
    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<V::Value, D::Error> {
        let d = self.deserializer(d);
        self.inner.visit_newtype_struct(d)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        let seq = SeqWrapped {
            inner: self.wrap(seq),
            index: 0,
        };
        self.inner.visit_seq(seq)
    }
    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        let map = self.wrap(map);
        self.inner.visit_map(map)
    }
    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        let data = self.wrap(data);
        self.inner.visit_enum(data)
    }
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Wrapped<'_, '_, S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<S::Value, D::Error> {
        let d = self.deserializer(d);
        self.inner.deserialize(d)
    }
}

struct SeqWrapped<'x, 'o, A> {
    inner: Wrapped<'x, 'o, A>,
    index: usize,
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for SeqWrapped<'_, '_, A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        let segment = format!("[{}]", self.index);
        self.index += 1;
        let seed = self.inner.wrap(seed);
        let ctx = self.inner.ctx;
        ctx.with_segment(segment, || self.inner.inner.next_element_seed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.inner.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for Wrapped<'_, '_, A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        *self.ctx.pending_key.borrow_mut() = None;
        let seed = Wrapped {
            inner: seed,
            ctx: self.ctx,
            role: Role::Key,
        };
        self.inner.next_key_seed(seed)
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        let key = self
            .ctx
            .pending_key
            .borrow_mut()
            .take()
            .unwrap_or_else(|| NON_STRING_KEY.to_string());
        let seed = Wrapped {
            inner: seed,
            ctx: self.ctx,
            role: Role::Value,
        };
        let ctx = self.ctx;
        ctx.with_segment(key, || self.inner.next_value_seed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'x, 'o, 'de, A: EnumAccess<'de>> EnumAccess<'de> for Wrapped<'x, 'o, A> {
    type Error = A::Error;
    type Variant = Wrapped<'x, 'o, A::Variant>;

    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Self::Variant), A::Error> {
        let seed = self.wrap(seed);
        let (value, variant) = self.inner.variant_seed(seed)?;
        Ok((
            value,
            Wrapped {
                inner: variant,
                ctx: self.ctx,
                role: self.role,
            },
        ))
    }
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for Wrapped<'_, '_, A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, A::Error> {
        let seed = self.wrap(seed);
        self.inner.newtype_variant_seed(seed)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        let visitor = self.wrap(visitor);
        self.inner.tuple_variant(len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        let visitor = self.wrap(visitor);
        self.inner.struct_variant(fields, visitor)
    }
}
