//! Pinned JSON of the types `vykar --json` and the daemon's status.json
//! serialize (schema 1). A change to one of these strings is a schema
//! change: fields may only be added within a schema version.

use chrono::{TimeZone, Utc};

use crate::app::operations::{
    AbortedRun, BackupRunReport, BackupSourceResult, FailedSource, SkippedSource, StepOutcome,
};
use crate::app::views::SnapshotRecord;
use crate::app::{SkipKind, SkipReason, StepError, StepErrorKind};
use crate::commands::check::{CheckError, CheckResult, ItemImpact};
use crate::commands::compact::{CompactStats, SweepSkipped};
use crate::commands::compact_refusal::CompactRefusal;
use crate::commands::info::{IndexLayoutStats, InfoStats};
use crate::commands::prune::{PruneListEntry, PruneStats};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::{EncryptionMode, RepoFormat, WriterMode};
use crate::snapshot::SnapshotStats;
use vykar_types::chunk_id::ChunkId;
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

fn pretty<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap()
}

fn stats() -> SnapshotStats {
    SnapshotStats {
        nfiles: 3,
        original_size: 4096,
        compressed_size: 2048,
        deduplicated_size: 1024,
        errors: 0,
    }
}

#[test]
fn outcomes_serialize_as_pinned_json() {
    let cases = [
        (StepOutcome::Ok, r#"{"status":"ok"}"#),
        (StepOutcome::Partial, r#"{"status":"partial"}"#),
        (
            StepOutcome::Skipped(SkipReason::new(SkipKind::Hook, "hook 'x' requested skip")),
            r#"{"status":"skipped","kind":"hook","message":"hook 'x' requested skip"}"#,
        ),
        (
            StepOutcome::Failed(StepError::new(
                StepErrorKind::Passphrase,
                "decryption failed",
            )),
            r#"{"status":"failed","kind":"passphrase","message":"decryption failed"}"#,
        ),
    ];
    for (outcome, json) in cases {
        assert_eq!(serde_json::to_string(&outcome).unwrap(), json);
    }
}

#[test]
fn backup_run_report_json() {
    let report = BackupRunReport {
        created: vec![BackupSourceResult {
            source_label: "docs".into(),
            snapshot_name: "a1b2c3d4".into(),
            source_paths: vec!["/home/u/docs".into()],
            stats: stats(),
            dataless_files_skipped: 0,
            dataless_dirs_skipped: 0,
        }],
        skipped: vec![SkippedSource {
            label: "mail".into(),
            reason: SkipReason::new(SkipKind::Hook, "hook 'x' requested skip (exit 75)"),
        }],
        failed: vec![FailedSource {
            label: "gone".into(),
            error: StepError::new(StepErrorKind::Other, "no such file"),
        }],
        aborted: Some(AbortedRun {
            label: "photos".into(),
            error: StepError::new(StepErrorKind::Interrupted, "interrupted"),
            interrupted: true,
            not_attempted: vec!["music".into()],
        }),
    };
    assert_eq!(
        pretty(&report),
        r#"{
  "created": [
    {
      "source_label": "docs",
      "snapshot_name": "a1b2c3d4",
      "source_paths": [
        "/home/u/docs"
      ],
      "stats": {
        "nfiles": 3,
        "original_size": 4096,
        "compressed_size": 2048,
        "deduplicated_size": 1024,
        "errors": 0
      },
      "dataless_files_skipped": 0,
      "dataless_dirs_skipped": 0
    }
  ],
  "skipped": [
    {
      "source_label": "mail",
      "reason": {
        "kind": "hook",
        "message": "hook 'x' requested skip (exit 75)"
      }
    }
  ],
  "failed": [
    {
      "source_label": "gone",
      "error": {
        "kind": "other",
        "message": "no such file"
      }
    }
  ],
  "aborted": {
    "source_label": "photos",
    "error": {
      "kind": "interrupted",
      "message": "interrupted"
    },
    "interrupted": true,
    "not_attempted": [
      "music"
    ]
  }
}"#
    );
}

#[test]
fn prune_stats_and_entries_json() {
    let stats = PruneStats {
        kept: 1,
        pruned: 1,
        warnings: vec!["w".into()],
    };
    let entries = vec![
        PruneListEntry {
            action: "keep".into(),
            snapshot_name: "aaaa0001".into(),
            reasons: vec!["last 1".into()],
        },
        PruneListEntry {
            action: "prune".into(),
            snapshot_name: "aaaa0002".into(),
            reasons: vec![],
        },
    ];
    assert_eq!(
        pretty(&stats),
        r#"{
  "kept": 1,
  "pruned": 1,
  "warnings": [
    "w"
  ]
}"#
    );
    assert_eq!(
        serde_json::to_string(&entries).unwrap(),
        r#"[{"action":"keep","snapshot_name":"aaaa0001","reasons":["last 1"]},{"action":"prune","snapshot_name":"aaaa0002","reasons":[]}]"#
    );
}

#[test]
fn compact_stats_json() {
    let stats = CompactStats {
        sweep_skipped: Some(SweepSkipped::Committing),
        ..CompactStats::default()
    };
    assert_eq!(
        pretty(&stats),
        r#"{
  "packs_total": 0,
  "packs_repacked": 0,
  "packs_deleted_empty": 0,
  "blobs_live": 0,
  "space_freed": 0,
  "packs_corrupt": 0,
  "packs_orphan": 0,
  "packs_orphan_young": 0,
  "packs_orphan_unknown_age": 0,
  "packs_orphan_live": 0,
  "temp_files_removed": 0,
  "packs_abandoned": 0,
  "packs_repack_held": 0,
  "snapshots_marked": 0,
  "chunks_unreferenced": 0,
  "bytes_unreferenced": 0,
  "chunks_unreferenced_young": 0,
  "bytes_unreferenced_young": 0,
  "sweep_skipped": "committing"
}"#
    );
    assert_eq!(
        serde_json::to_string(&SweepSkipped::Contention).unwrap(),
        r#""contention""#
    );
}

#[test]
fn check_result_json_leaves_out_item_impacts() {
    let result = CheckResult {
        snapshots_checked: 2,
        key_files_checked: 1,
        items_checked: 10,
        chunks_existence_checked: 20,
        packs_existence_checked: 5,
        chunks_data_verified: 0,
        errors: vec![CheckError {
            context: "snapshot aaaa0001".into(),
            message: "references missing pack".into(),
        }],
        item_impacts: vec![ItemImpact {
            snapshot_id: SnapshotId::from_bytes([1; 32]),
            snapshot_name: "aaaa0001".into(),
            item_index: 0,
            item_path: "f".into(),
            affected_chunks: vec![(ChunkId::from_bytes([2; 32]), PackId::from_bytes([3; 32]))],
        }],
        skipped: false,
    };
    assert_eq!(
        pretty(&result),
        r#"{
  "snapshots_checked": 2,
  "key_files_checked": 1,
  "items_checked": 10,
  "chunks_existence_checked": 20,
  "packs_existence_checked": 5,
  "chunks_data_verified": 0,
  "errors": [
    {
      "context": "snapshot aaaa0001",
      "message": "references missing pack"
    }
  ],
  "skipped": false
}"#
    );
}

#[test]
fn info_stats_json() {
    let t = Utc.with_ymd_and_hms(2026, 11, 27, 10, 0, 0).unwrap();
    let info = InfoStats {
        snapshot_count: 1,
        last_snapshot_time: Some(t),
        original_size: 100,
        compressed_size: 50,
        deduplicated_size: 40,
        unique_stored_size: 45,
        referenced_stored_size: None,
        last_sweep_time: None,
        unique_chunks: 7,
        repo_created: t,
        encryption: EncryptionMode::Aes256Gcm,
        format: RepoFormat::V6(HashAlgorithm::Blake3),
        index: Some(IndexLayoutStats {
            base_bytes: 10,
            segments: 1,
            segment_bytes: 2,
        }),
        compact_refused: Some(CompactRefusal {
            first: t,
            last: t,
            detail: "d".into(),
        }),
        writer_mode: WriterMode::Multi,
        put_if_absent_offered: false,
    };
    assert_eq!(
        pretty(&info),
        r#"{
  "snapshot_count": 1,
  "last_snapshot_time": "2026-11-27T10:00:00Z",
  "original_size": 100,
  "compressed_size": 50,
  "deduplicated_size": 40,
  "unique_stored_size": 45,
  "referenced_stored_size": null,
  "last_sweep_time": null,
  "unique_chunks": 7,
  "repo_created": "2026-11-27T10:00:00Z",
  "encryption": "aes256gcm",
  "format": {
    "version": 6,
    "chunk_hash": "blake3"
  },
  "index": {
    "base_bytes": 10,
    "segments": 1,
    "segment_bytes": 2
  },
  "compact_refused": {
    "first": "2026-11-27T10:00:00Z",
    "last": "2026-11-27T10:00:00Z",
    "detail": "d"
  },
  "writer_mode": "multi",
  "put_if_absent_offered": false
}"#
    );
    let single = InfoStats {
        encryption: EncryptionMode::None,
        format: RepoFormat::V6(HashAlgorithm::Blake2b),
        writer_mode: WriterMode::Single,
        ..info
    };
    let v = serde_json::to_value(&single).unwrap();
    assert_eq!(v["encryption"], "none");
    assert_eq!(v["format"]["chunk_hash"], "blake2b");
    assert_eq!(v["writer_mode"], "single");
}

#[test]
fn snapshot_record_json() {
    let entry = SnapshotEntry {
        name: "a1b2c3d4".into(),
        id: SnapshotId::from_bytes([9; 32]),
        time: Utc.with_ymd_and_hms(2026, 11, 27, 10, 0, 0).unwrap(),
        source_label: String::new(),
        label: "legacy".into(),
        source_paths: vec![],
        hostname: String::new(),
    };
    assert_eq!(
        pretty(&SnapshotRecord::new(&entry, None)),
        r#"{
  "name": "a1b2c3d4",
  "time": "2026-11-27T10:00:00Z",
  "hostname": "",
  "source_label": "legacy",
  "source_paths": [],
  "stats": null
}"#
    );
    let entry = SnapshotEntry {
        source_label: "docs".into(),
        source_paths: vec!["/home/u/docs".into()],
        hostname: "h".into(),
        ..entry
    };
    assert_eq!(
        serde_json::to_string(&SnapshotRecord::new(&entry, Some(&stats()))).unwrap(),
        r#"{"name":"a1b2c3d4","time":"2026-11-27T10:00:00Z","hostname":"h","source_label":"docs","source_paths":["/home/u/docs"],"stats":{"nfiles":3,"original_size":4096,"compressed_size":2048,"deduplicated_size":1024,"errors":0}}"#
    );
}
