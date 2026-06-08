//! Best-effort GitHub release check run once at startup.
//!
//! Fetches the latest published release tag from the GitHub API, compares it to
//! the compiled-in version, and reports a newer release if one exists. All
//! failures (network, parse, odd tags) resolve to `None` — we never notify on
//! uncertainty.

use std::time::Duration;

const RELEASES_API: &str = "https://api.github.com/repos/borgbase/vykar/releases/latest";
const RELEASES_FALLBACK_URL: &str = "https://github.com/borgbase/vykar/releases/latest";

pub(crate) struct UpdateInfo {
    pub(crate) version: String,
    pub(crate) url: String,
}

/// Return `Some(UpdateInfo)` when the latest GitHub release is strictly newer
/// than `current`. Best-effort: any error returns `None`.
pub(crate) fn check(current: &str) -> Option<UpdateInfo> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(5)))
        .build()
        .into();

    // GitHub rejects requests without a User-Agent.
    let body = agent
        .get(RELEASES_API)
        .header("User-Agent", concat!("vykar/", env!("CARGO_PKG_VERSION")))
        .call()
        .ok()?
        .body_mut()
        .read_to_string()
        .ok()?;

    parse_release(&body, current)
}

/// Map a GitHub `releases/latest` JSON body to an `UpdateInfo` when it names a
/// release strictly newer than `current`. Pure (no I/O) so it can be tested
/// directly. Returns `None` on malformed JSON, a missing/odd `tag_name`, or a
/// release that is not newer.
fn parse_release(body: &str, current: &str) -> Option<UpdateInfo> {
    let json: serde_json::Value = serde_json::from_str(body).ok()?;

    let tag = json.get("tag_name")?.as_str()?;
    let latest = tag.strip_prefix('v').unwrap_or(tag);

    if !is_newer(latest, current) {
        return None;
    }

    let url = json
        .get("html_url")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(RELEASES_FALLBACK_URL)
        .to_string();

    Some(UpdateInfo {
        version: latest.to_string(),
        url,
    })
}

/// Parse `major.minor.patch` into a tuple. Returns `None` on any unexpected
/// shape (extra segments, non-numeric, pre-release suffixes).
fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

/// `true` only when both parse cleanly and `latest` is strictly greater.
fn is_newer(latest: &str, current: &str) -> bool {
    match (parse_version(latest), parse_version(current)) {
        (Some(l), Some(c)) => l > c,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_version_detected() {
        assert!(is_newer("0.16.3", "0.16.2"));
        assert!(is_newer("1.0.0", "0.16.2"));
        assert!(is_newer("0.17.0", "0.16.9"));
    }

    #[test]
    fn older_version_not_detected() {
        assert!(!is_newer("0.16.1", "0.16.2"));
        assert!(!is_newer("0.9.9", "1.0.0"));
    }

    #[test]
    fn equal_version_not_detected() {
        assert!(!is_newer("0.16.2", "0.16.2"));
    }

    #[test]
    fn malformed_version_not_detected() {
        assert!(!is_newer("0.16", "0.16.2"));
        assert!(!is_newer("0.16.2", "0.16"));
        assert!(!is_newer("0.16.2-rc1", "0.16.2"));
        assert!(!is_newer("abc", "0.16.2"));
        assert!(!is_newer("0.16.2.1", "0.16.2"));
        assert!(!is_newer("", "0.16.2"));
    }

    #[test]
    fn parse_release_maps_tag_and_url() {
        let body = r#"{"tag_name": "v0.16.3", "html_url": "https://github.com/borgbase/vykar/releases/tag/v0.16.3"}"#;
        let info = parse_release(body, "0.16.2").expect("newer release");
        assert_eq!(info.version, "0.16.3");
        assert_eq!(
            info.url,
            "https://github.com/borgbase/vykar/releases/tag/v0.16.3"
        );
    }

    #[test]
    fn parse_release_strips_v_prefix_when_absent() {
        // Tag without a leading `v` is accepted verbatim.
        let body = r#"{"tag_name": "0.17.0", "html_url": "https://example.test/r"}"#;
        let info = parse_release(body, "0.16.2").expect("newer release");
        assert_eq!(info.version, "0.17.0");
    }

    #[test]
    fn parse_release_falls_back_when_url_missing_or_empty() {
        let missing = r#"{"tag_name": "v0.16.3"}"#;
        assert_eq!(
            parse_release(missing, "0.16.2").expect("newer").url,
            RELEASES_FALLBACK_URL
        );

        let empty = r#"{"tag_name": "v0.16.3", "html_url": ""}"#;
        assert_eq!(
            parse_release(empty, "0.16.2").expect("newer").url,
            RELEASES_FALLBACK_URL
        );
    }

    #[test]
    fn parse_release_none_when_not_newer() {
        let body = r#"{"tag_name": "v0.16.2", "html_url": "https://example.test/r"}"#;
        assert!(parse_release(body, "0.16.2").is_none());
        assert!(parse_release(body, "0.16.3").is_none());
    }

    #[test]
    fn parse_release_none_on_malformed_or_missing_fields() {
        assert!(parse_release("not json", "0.16.2").is_none());
        assert!(parse_release("{}", "0.16.2").is_none());
        // tag_name present but not a string.
        assert!(parse_release(r#"{"tag_name": 42}"#, "0.16.2").is_none());
        // Odd tag that fails version parsing.
        assert!(parse_release(r#"{"tag_name": "latest"}"#, "0.16.2").is_none());
    }
}
