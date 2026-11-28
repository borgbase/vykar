//! The one place vykar parses YAML.
//!
//! Every config parse must go through [`from_str`] or [`from_str_with`] rather
//! than calling `serde_saphyr` directly, because the default options are wrong
//! for vykar.
//!
//! serde-saphyr resolves the YAML 1.1 boolean literals — `yes`/`no`/`on`/`off`
//! and even bare `y`/`n`, case-insensitively — as booleans by default.
//! serde_yaml 0.9, which vykar shipped for its whole life, treats only
//! `true`/`false` as booleans and passes the rest through as plain strings.
//! Without `strict_booleans` a config reading `label: no` would change meaning
//! on upgrade: `StrictString` fields would start rejecting it, and any field
//! typed as a bool would silently flip. `strict_booleans` restores exactly the
//! serde_yaml 0.9 behaviour; the tests in `resolve::document` are the guard.

use serde::de::DeserializeOwned;

/// The parse options every config parse uses.
fn options() -> serde_saphyr::Options {
    let mut options = serde_saphyr::Options::default();
    // Only `true`/`false` are booleans — see the module comment.
    options.strict_booleans = true;
    // Both parsing passes can contain secrets, including expanded env-file
    // values. Source snippets would print neighboring credentials on errors.
    options.with_snippet = false;
    options
}

/// Parse YAML with vykar's scalar-resolution rules.
pub(crate) fn from_str<T: DeserializeOwned>(input: &str) -> Result<T, serde_saphyr::Error> {
    serde_saphyr::from_str_with_options(input, options())
}

/// Parse YAML with vykar's scalar-resolution rules through a caller-supplied
/// wrapper around the deserializer (environment expansion uses this).
pub(crate) fn from_str_with<'de, T, F>(input: &'de str, f: F) -> Result<T, serde_saphyr::Error>
where
    for<'e> F: FnOnce(serde_saphyr::Deserializer<'de, 'e>) -> Result<T, serde_saphyr::Error>,
{
    serde_saphyr::with_deserializer_from_str_with_options(input, options(), f)
}
