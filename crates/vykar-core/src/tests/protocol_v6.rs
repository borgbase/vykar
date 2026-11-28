//! Format v6 protocol between clients that no versioned object carries:
//! the session-marker JSON, the key shapes under `sessions/`, and the
//! timing constants of the commit and compact rules. Binaries of different
//! releases share a repository, and an older one keeps applying the values
//! pinned here. Changing any of them is a format change: it needs a new
//! `config` feature name (`repo::KNOWN_CONFIG_FEATURES`), so older clients
//! refuse the repository instead of breaking the safety argument
//! (architecture.md, "Protocol between v6 clients"; AGENTS.md).
//!
//! Do not update an expected value here to make a test pass.

use std::time::Duration;

use chrono::{TimeZone, Utc};

use crate::repo::ages::{
    CLOCK_PROBE_DEBRIS_AGE, CLOCK_PROBE_PREFIX, HEAD_FRESHNESS_LIMIT, HEAD_LANDING_ALLOWANCE,
    INDEX_WRITE_LIMIT, MARGIN, PACK_AGE_LIMIT, PUT_IF_ABSENT_PROBE_PREFIX, RECOVERY_MAX_PACK_AGE,
    T_IDX, T_PACK, is_probe_key,
};
use crate::repo::index_store::{HEADS_KEPT, INDEX_GC_GRACE, INDEX_OBJECT_GRACE};
use crate::repo::sessions::{
    COMMIT_MAX_WAIT, EXCLUSIVE_ID_PREFIX, MARKER_GAP_LIMIT, REFRESH_INTERVAL, SESSION_STALE_SECS,
    SESSIONS_PREFIX, SessionEntry, exclusive_session_id, is_exclusive_session,
    parse_session_marker_key, session_index_key, session_marker_key, session_marker_key_at,
};
use crate::repo::{HostEnv, HostIdentity};

/// The message of every assertion below.
const RULE: &str = "format v6 protocol between clients: changing this is a format \
    change and needs a new config feature name (KNOWN_CONFIG_FEATURES) so older \
    clients refuse; see architecture.md \"Protocol between v6 clients\" and AGENTS.md";

const ID: &str = "0123456789abcdef0123456789abcdef";

fn host(full: bool) -> HostEnv {
    HostEnv::system().with_host(HostIdentity {
        hostname: "host-a".into(),
        pid: 4242,
        boot_id: full.then(|| "boot-1".into()),
        installation_id: full.then(|| "inst-1".into()),
        pid_ns: full.then_some(4_026_531_836),
    })
}

/// The exact marker body this binary writes, for a registration and with
/// both flags: field names, order, `null` for an unknown identity field,
/// flags omitted while false.
#[test]
fn marker_json_is_pinned() {
    let at = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    let plain = SessionEntry::new(&host(true), at, at);
    assert_eq!(
        serde_json::to_string(&plain).unwrap(),
        r#"{"hostname":"host-a","pid":4242,"registered_at":"2026-01-02T03:04:05+00:00","last_refresh":"2026-01-02T03:04:05+00:00","boot_id":"boot-1","installation_id":"inst-1","pid_ns":4026531836,"elapsed_secs":0}"#,
        "{RULE}"
    );
    let bare = SessionEntry {
        elapsed_secs: None,
        ..SessionEntry::new(&host(false), at, at)
    };
    assert_eq!(
        serde_json::to_string(&bare).unwrap(),
        r#"{"hostname":"host-a","pid":4242,"registered_at":"2026-01-02T03:04:05+00:00","last_refresh":"2026-01-02T03:04:05+00:00","boot_id":null,"installation_id":null,"pid_ns":null,"elapsed_secs":null}"#,
        "{RULE}"
    );
    let flagged = SessionEntry {
        elapsed_secs: Some(90),
        committing: true,
        writes_heads: true,
        ..plain.clone()
    };
    assert_eq!(
        serde_json::to_string(&flagged).unwrap(),
        r#"{"hostname":"host-a","pid":4242,"registered_at":"2026-01-02T03:04:05+00:00","last_refresh":"2026-01-02T03:04:05+00:00","boot_id":"boot-1","installation_id":"inst-1","pid_ns":4026531836,"elapsed_secs":90,"committing":true,"writes_heads":true}"#,
        "{RULE}"
    );
}

/// A fixed body reads field by field, and unknown fields (a later
/// writer's additions, of any JSON type, anywhere in the map) are ignored.
#[test]
fn marker_json_reads_by_name_and_tolerates_unknown_fields() {
    let body = r#"{"future_scalar":1,"hostname":"host-a","pid":4242,
        "registered_at":"2026-01-02T03:04:05+00:00","last_refresh":"2026-01-02T03:19:05+00:00",
        "boot_id":"boot-1","installation_id":"inst-1","pid_ns":7,"elapsed_secs":900,
        "committing":true,"writes_heads":false,"future_obj":{"a":[1,null,"x"]}}"#;
    let e: SessionEntry = serde_json::from_str(body).expect(RULE);
    assert_eq!(e.hostname, "host-a", "{RULE}");
    assert_eq!(e.pid, 4242, "{RULE}");
    assert_eq!(e.registered_at, "2026-01-02T03:04:05+00:00", "{RULE}");
    assert_eq!(e.last_refresh, "2026-01-02T03:19:05+00:00", "{RULE}");
    assert_eq!(e.boot_id.as_deref(), Some("boot-1"), "{RULE}");
    assert_eq!(e.installation_id.as_deref(), Some("inst-1"), "{RULE}");
    assert_eq!(e.pid_ns, Some(7), "{RULE}");
    assert_eq!(e.elapsed_secs, Some(900), "{RULE}");
    assert!(e.committing && !e.writes_heads, "{RULE}");
    // The shape of 0.24 and of writers before `elapsed_secs`: only the four
    // required fields; everything else defaults.
    let old = r#"{"hostname":"h","pid":1,"registered_at":"x","last_refresh":"y"}"#;
    let e: SessionEntry = serde_json::from_str(old).expect(RULE);
    assert!(
        e.boot_id.is_none()
            && e.installation_id.is_none()
            && e.pid_ns.is_none()
            && e.elapsed_secs.is_none()
            && !e.committing
            && !e.writes_heads,
        "{RULE}"
    );
}

/// Key shapes under `sessions/` that other clients list, parse and reap.
#[test]
fn session_key_shapes_are_pinned() {
    assert_eq!(SESSIONS_PREFIX, "sessions/", "{RULE}");
    assert_eq!(
        session_marker_key(ID),
        format!("sessions/{ID}.json"),
        "{RULE}"
    );
    assert_eq!(
        session_marker_key_at(ID, 0),
        format!("sessions/{ID}.json"),
        "{RULE}"
    );
    assert_eq!(
        session_marker_key_at(ID, 7),
        format!("sessions/{ID}.7.json"),
        "{RULE}"
    );
    assert_eq!(
        session_index_key(ID),
        format!("sessions/{ID}.index"),
        "{RULE}"
    );
    let key0 = format!("sessions/{ID}.json");
    assert_eq!(parse_session_marker_key(&key0), Some((ID, 0)), "{RULE}");
    let key12 = format!("sessions/{ID}.12.json");
    assert_eq!(parse_session_marker_key(&key12), Some((ID, 12)), "{RULE}");
    // A non-canonical generation is part of the ID (it still counts).
    let stem = format!("{ID}.07");
    let odd = format!("sessions/{stem}.json");
    assert_eq!(
        parse_session_marker_key(&odd),
        Some((stem.as_str(), 0)),
        "{RULE}"
    );

    assert_eq!(EXCLUSIVE_ID_PREFIX, "x-", "{RULE}");
    let x = exclusive_session_id();
    assert!(
        x.len() == 34
            && x.starts_with("x-")
            && x[2..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "{RULE}: {x}"
    );
    assert!(
        is_exclusive_session(&x) && !is_exclusive_session(ID),
        "{RULE}"
    );
    let xkey = session_marker_key_at(&x, 3);
    assert_eq!(xkey, format!("sessions/{x}.3.json"), "{RULE}");
    assert_eq!(
        parse_session_marker_key(&xkey),
        Some((x.as_str(), 3)),
        "{RULE}"
    );

    assert_eq!(CLOCK_PROBE_PREFIX, "sessions/vykar-clock-", "{RULE}");
    assert_eq!(
        PUT_IF_ABSENT_PROBE_PREFIX, "sessions/vykar-probe-",
        "{RULE}"
    );
    assert_eq!(
        vykar_storage::PUT_IF_ABSENT_PROBE_KEY_PREFIX,
        "sessions/vykar-probe-",
        "{RULE}"
    );
    assert!(
        is_probe_key("sessions/vykar-clock-abc") && is_probe_key("sessions/vykar-probe-abc/k"),
        "{RULE}"
    );
    assert!(
        !is_probe_key(&session_marker_key(ID)) && !is_probe_key(&session_index_key(ID)),
        "{RULE}"
    );
}

/// The timing constants other clients' safety arguments rely on, by value.
#[test]
fn protocol_constants_are_pinned() {
    const H: u64 = 3600;
    const MIN: u64 = 60;
    let pins: &[(&str, Duration, u64)] = &[
        ("T_PACK", T_PACK, 12 * H),
        ("T_IDX", T_IDX, H),
        ("MARGIN", MARGIN, 10 * MIN),
        ("HEAD_LANDING_ALLOWANCE", HEAD_LANDING_ALLOWANCE, 30 * MIN),
        ("PACK_AGE_LIMIT", PACK_AGE_LIMIT, 11 * H + 50 * MIN),
        ("INDEX_WRITE_LIMIT", INDEX_WRITE_LIMIT, 11 * H + 50 * MIN),
        ("HEAD_FRESHNESS_LIMIT", HEAD_FRESHNESS_LIMIT, 50 * MIN),
        ("RECOVERY_MAX_PACK_AGE", RECOVERY_MAX_PACK_AGE, 6 * H),
        ("INDEX_OBJECT_GRACE", INDEX_OBJECT_GRACE, 12 * H),
        ("INDEX_GC_GRACE", INDEX_GC_GRACE, H),
        ("CLOCK_PROBE_DEBRIS_AGE", CLOCK_PROBE_DEBRIS_AGE, H),
        ("REFRESH_INTERVAL", REFRESH_INTERVAL, 15 * MIN),
        ("MARKER_GAP_LIMIT", MARKER_GAP_LIMIT, 30 * MIN),
        ("COMMIT_MAX_WAIT", COMMIT_MAX_WAIT, 2 * H),
    ];
    for (name, got, secs) in pins {
        assert_eq!(*got, Duration::from_secs(*secs), "{name}: {RULE}");
    }
    assert_eq!(SESSION_STALE_SECS, 45 * 60, "SESSION_STALE_SECS: {RULE}");
    assert_eq!(HEADS_KEPT, 16, "HEADS_KEPT: {RULE}");
}
