//! HTML rendering for the daemon's read-only status page.
//!
//! Pure function over `DaemonStatus` — no I/O, no globals. Embeds a small
//! hand-written stylesheet so the page is one self-contained response.

use std::fmt::Write;
use std::time::Duration;

use chrono::{DateTime, Local, Utc};
use vykar_common::display::format_bytes;
use vykar_core::app::views::{SnapshotRowView, format_last_snapshot};

use super::status::{CycleOutcome, DaemonStatus, RepoState};

const STYLE: &str = r#"
:root {
  color-scheme: light dark;
  --bg: #fafafa;
  --fg: #111;
  --muted: #666;
  --card: #fff;
  --border: #e2e2e2;
  --row-alt: #f5f5f5;
  --accent: #2563eb;
  --ok: #16a34a;
  --warn: #d97706;
  --err: #dc2626;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #161616;
    --fg: #eee;
    --muted: #999;
    --card: #1f1f1f;
    --border: #2c2c2c;
    --row-alt: #1a1a1a;
    --accent: #60a5fa;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; background: var(--bg); color: var(--fg);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 14px; line-height: 1.4; }
.wrap { max-width: 1100px; margin: 0 auto; padding: 1.25rem; }
header { display: flex; justify-content: space-between; align-items: baseline;
  flex-wrap: wrap; gap: 0.5rem; margin-bottom: 1rem; }
header h1 { font-size: 1.25rem; margin: 0; font-weight: 600; }
header .meta { color: var(--muted); font-size: 0.85rem; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 0.75rem; margin-bottom: 1.5rem; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 6px;
  padding: 0.85rem 1rem; }
.card .label { color: var(--muted); font-size: 0.75rem; text-transform: uppercase;
  letter-spacing: 0.05em; }
.card .value { font-size: 1.4rem; font-weight: 600; margin-top: 0.2rem; }
.card .sub { color: var(--muted); font-size: 0.8rem; margin-top: 0.15rem; }
.outcome-ok { color: var(--ok); }
.outcome-partial { color: var(--warn); }
.outcome-errors { color: var(--err); }
.outcome-skipped { color: var(--muted); }
.state-ok { color: var(--ok); }
.state-error { color: var(--err); }
.state-skipped, .state-pending { color: var(--muted); }
.asof { color: var(--muted); font-size: 0.75rem; }
section { margin-bottom: 1.5rem; }
section h2 { font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.05em;
  color: var(--muted); margin: 0 0 0.5rem; font-weight: 600; }
table { width: 100%; border-collapse: collapse; background: var(--card);
  border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
th, td { text-align: left; padding: 0.45rem 0.7rem; font-size: 0.85rem;
  border-bottom: 1px solid var(--border); }
th { color: var(--muted); font-weight: 600; font-size: 0.75rem;
  text-transform: uppercase; letter-spacing: 0.04em; }
tr:nth-child(even) td { background: var(--row-alt); }
tr:last-child td { border-bottom: none; }
.sources-list { background: var(--card); border: 1px solid var(--border);
  border-radius: 6px; overflow: hidden; }
.sources-list details { border-bottom: 1px solid var(--border); }
.sources-list details:last-child { border-bottom: none; }
.sources-list summary { padding: 0.5rem 0.75rem; cursor: pointer;
  list-style: none; display: flex; align-items: baseline; gap: 0.5rem;
  flex-wrap: wrap; }
.sources-list summary::-webkit-details-marker { display: none; }
.sources-list summary::before { content: "▸"; color: var(--muted);
  font-size: 0.7rem; transition: transform 0.15s ease; display: inline-block; }
.sources-list details[open] > summary::before { transform: rotate(90deg); }
.sources-list summary:hover { background: var(--row-alt); }
.sources-list .label { font-weight: 600; }
.sources-list .paths { color: var(--muted); font-size: 0.8rem;
  word-break: break-all; flex: 1 1 auto; }
.sources-list .target { color: var(--accent); font-size: 0.8rem; }
.sources-list .detail { padding: 0.25rem 0.75rem 0.75rem 1.5rem;
  font-size: 0.8rem; }
.sources-list .detail dt { color: var(--muted); font-weight: 600;
  text-transform: uppercase; font-size: 0.7rem; letter-spacing: 0.05em;
  margin-top: 0.6rem; }
.sources-list .detail dd { margin: 0.15rem 0 0; word-break: break-all;
  white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular,
  Menlo, Consolas, monospace; }
footer { margin-top: 2rem; color: var(--muted); font-size: 0.75rem; text-align: center; }
.empty { color: var(--muted); padding: 0.6rem; font-style: italic; }
"#;

fn render_detail_list(out: &mut String, label: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }
    let _ = write!(out, "<dt>{}</dt><dd>", esc(label));
    let mut first = true;
    for item in items {
        if !first {
            out.push('\n');
        }
        first = false;
        out.push_str(&esc(item));
    }
    out.push_str("</dd>");
}

fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m {}s", s / 60, s % 60)
    } else if s < 86_400 {
        format!("{}h {}m", s / 3600, (s % 3600) / 60)
    } else {
        format!("{}d {}h", s / 86_400, (s % 86_400) / 3600)
    }
}

/// Local `%Y-%m-%d %H:%M:%S`, as the page has always shown times.
fn local_time(t: DateTime<Utc>) -> String {
    t.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

pub(crate) fn render_html(status: &DaemonStatus) -> String {
    let mut out = String::with_capacity(8 * 1024);

    let total_snapshots: usize = status
        .repos
        .iter()
        .map(|r| r.snapshot_count.unwrap_or(0))
        .sum();

    let last_outcome = status.last_cycle.state.map_or("n/a", CycleOutcome::as_str);
    let outcome_class = match status.last_cycle.state {
        Some(CycleOutcome::Ok) => "outcome-ok",
        Some(CycleOutcome::Partial) => "outcome-partial",
        Some(CycleOutcome::Errors) => "outcome-errors",
        Some(CycleOutcome::Skipped) => "outcome-skipped",
        Some(CycleOutcome::Running) | None => "",
    };
    let uptime = format_duration(Duration::from_secs(status.process.uptime_seconds));

    let _ = write!(
        out,
        "<!doctype html><html lang=\"en\"><head>\
            <meta charset=\"utf-8\">\
            <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
            <meta http-equiv=\"refresh\" content=\"30\">\
            <title>vykar daemon</title>\
            <style>{style}</style>\
         </head><body><div class=\"wrap\">\
            <header>\
              <h1>vykar daemon</h1>\
              <div class=\"meta\">{host} &middot; pid {pid} &middot; v{version} &middot; up {uptime}</div>\
            </header>",
        style = STYLE,
        host = esc(&status.process.hostname),
        pid = status.process.pid,
        version = esc(&status.process.version),
        uptime = esc(&uptime),
    );

    // Metric cards
    let next_run = status
        .process
        .next_run
        .map_or_else(|| "—".to_string(), local_time);
    let last_started = status
        .last_cycle
        .started_at
        .map_or_else(|| "—".to_string(), local_time);
    let last_duration = status.last_cycle.duration_seconds.map_or_else(
        || "—".to_string(),
        |s| format_duration(Duration::from_secs(s)),
    );

    let _ = write!(
        out,
        "<div class=\"cards\">\
            <div class=\"card\"><div class=\"label\">Repositories</div>\
              <div class=\"value\">{nrepos}</div>\
              <div class=\"sub\">{nsnap} snapshots total</div></div>\
            <div class=\"card\"><div class=\"label\">Schedule</div>\
              <div class=\"value\">{schedule}</div>\
              <div class=\"sub\">next: {next}</div></div>\
            <div class=\"card\"><div class=\"label\">Last cycle</div>\
              <div class=\"value {oc}\">{outcome}</div>\
              <div class=\"sub\">{started} &middot; {dur}</div></div>\
            <div class=\"card\"><div class=\"label\">Sources</div>\
              <div class=\"value\">{nsources}</div>\
              <div class=\"sub\">configured</div></div>\
         </div>",
        nrepos = status.repos.len(),
        nsnap = total_snapshots,
        schedule = esc(&status.schedule_brief),
        next = esc(&next_run),
        oc = outcome_class,
        outcome = esc(last_outcome),
        started = esc(&last_started),
        dur = esc(&last_duration),
        nsources = status.sources.len(),
    );

    // Repositories
    out.push_str("<section><h2>Repositories</h2>");
    if status.repos.is_empty() {
        out.push_str("<div class=\"empty\">No repositories.</div>");
    } else {
        out.push_str(
            "<table><thead><tr>\
                <th>Name</th><th>URL</th><th>Snapshots</th>\
                <th>Last snapshot</th><th>Size</th><th>Next run</th><th>Status</th>\
             </tr></thead><tbody>",
        );
        for r in &status.repos {
            let state_class = match r.state {
                RepoState::Ok => "state-ok",
                RepoState::Error => "state-error",
                RepoState::Skipped => "state-skipped",
                RepoState::Pending => "state-pending",
            };
            let asof = r.refreshed_at.map_or_else(String::new, |t| {
                format!(" <span class=\"asof\">as of {}</span>", esc(&local_time(t)))
            });
            let (snapshots, last_snapshot, size) = match r.snapshot_count {
                None => ("—".to_string(), "—".to_string(), "—".to_string()),
                Some(n) => (
                    n.to_string(),
                    format_last_snapshot(r.last_snapshot_time),
                    r.unique_stored_size
                        .map_or_else(|| "—".to_string(), format_bytes),
                ),
            };
            let next_run = r.next_run.map_or_else(|| "Off".to_string(), local_time);
            let _ = write!(
                out,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
                 <td><span class=\"{}\">{}</span>{}</td></tr>",
                esc(&r.name),
                esc(&r.url),
                esc(&snapshots),
                esc(&last_snapshot),
                esc(&size),
                esc(&next_run),
                state_class,
                esc(r.state.as_str()),
                asof,
            );
        }
        out.push_str("</tbody></table>");
    }
    out.push_str("</section>");

    // Recent snapshots
    out.push_str("<section><h2>Recent snapshots</h2>");
    if status.recent_snapshots.is_empty() {
        out.push_str("<div class=\"empty\">No snapshots yet.</div>");
    } else {
        out.push_str(
            "<table><thead><tr>\
                <th>Time</th><th>Repo</th><th>Snapshot</th>\
                <th>Hostname</th><th>Label</th><th>Files</th><th>Size</th><th>Added</th>\
             </tr></thead><tbody>",
        );
        for s in &status.recent_snapshots {
            let row = SnapshotRowView::from_record(&s.snapshot);
            let _ = write!(
                out,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&row.time),
                esc(&s.repo_name),
                esc(&row.id),
                esc(&row.hostname),
                esc(&row.label),
                esc(&row.files),
                esc(&row.size),
                esc(&row.added),
            );
        }
        out.push_str("</tbody></table>");
    }
    out.push_str("</section>");

    // Sources (expandable via <details>/<summary>; no JS)
    out.push_str("<section><h2>Sources</h2>");
    if status.sources.is_empty() {
        out.push_str("<div class=\"empty\">No sources configured.</div>");
    } else {
        out.push_str("<div class=\"sources-list\">");
        for s in &status.sources {
            let target_repos = if s.target_repos.is_empty() {
                "(all)".to_string()
            } else {
                s.target_repos.join(", ")
            };
            let options = s.options.join(", ");
            let retention = s.retention.join(", ");
            let _ = write!(
                out,
                "<details><summary>\
                    <span class=\"label\">{label}</span>\
                    <span class=\"paths\">{paths}</span>\
                    <span class=\"target\">→ {target}</span>\
                  </summary>\
                  <dl class=\"detail\">",
                label = esc(&s.label),
                paths = esc(&s.paths.join(", ")),
                target = esc(&target_repos),
            );

            render_detail_list(&mut out, "Folders", &s.paths);
            render_detail_list(&mut out, "Exclusions", &s.exclude);
            render_detail_list(&mut out, "Exclude if present", &s.exclude_if_present);
            if !options.is_empty() {
                let _ = write!(out, "<dt>Options</dt><dd>{}</dd>", esc(&options),);
            }
            render_detail_list(&mut out, "Hooks", &s.hooks);
            if !retention.is_empty() {
                let _ = write!(out, "<dt>Retention</dt><dd>{}</dd>", esc(&retention),);
            }
            render_detail_list(&mut out, "Command dumps", &s.command_dumps);
            let _ = write!(
                out,
                "<dt>Target repositories</dt><dd>{}</dd>",
                esc(&target_repos),
            );

            out.push_str("</dl></details>");
        }
        out.push_str("</div>");
    }
    out.push_str("</section>");

    out.push_str(
        "<footer>read-only status page &middot; refreshes every 30s &middot; \
         <a href=\"/api/status.json\">status.json</a></footer>\
         </div></body></html>",
    );

    out
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use vykar_core::app::views::SnapshotRecord;
    use vykar_core::repo::manifest::SnapshotEntry;
    use vykar_core::snapshot::SnapshotStats;
    use vykar_types::snapshot_id::SnapshotId;

    use super::*;
    use crate::cmd::daemon::status::*;

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 27, h, m, s).unwrap()
    }

    fn fixture() -> DaemonStatus {
        let entry = SnapshotEntry {
            name: "snap-xyz".into(),
            id: SnapshotId::from_bytes([0; 32]),
            time: at(10, 0, 0),
            source_label: "home".into(),
            label: String::new(),
            source_paths: vec!["/home/user".into()],
            hostname: "src".into(),
        };
        let stats = SnapshotStats {
            nfiles: 1234,
            original_size: 500_000_000,
            compressed_size: 0,
            deduplicated_size: 20_000_000,
            errors: 0,
        };
        DaemonStatus {
            schema: crate::json::SCHEMA,
            process: ProcessInfo {
                hostname: "host1".into(),
                pid: 1234,
                version: "0.14.0".into(),
                started_at: Some(at(8, 0, 0)),
                uptime_seconds: 3900,
                next_run: Some(at(12, 0, 0)),
            },
            schedule_brief: "0 * * * *".into(),
            repos: vec![RepoInfo {
                name: "repo-a".into(),
                url: "rest:https://example.com/repo".into(),
                snapshot_count: Some(3),
                last_snapshot_time: Some(at(9, 55, 0)),
                unique_stored_size: Some(1_200_000_000),
                next_run: Some(at(12, 0, 0)),
                state: RepoState::Error,
                refreshed_at: Some(at(9, 58, 31)),
            }],
            recent_snapshots: vec![SnapshotRow {
                repo_name: "repo-a".into(),
                snapshot: SnapshotRecord::new(&entry, Some(&stats)),
            }],
            sources: vec![SourceInfo {
                label: "home".into(),
                paths: vec!["/home/user".into()],
                exclude: vec![".cache".into(), "*.tmp".into()],
                exclude_if_present: vec![".nobackup".into()],
                target_repos: vec![],
                options: vec!["git_ignore".into()],
                hooks: vec!["before: echo hi".into()],
                retention: vec!["keep_last: 7".into()],
                command_dumps: vec![],
            }],
            last_cycle: CycleSummary {
                started_at: Some(at(9, 55, 0)),
                finished_at: Some(at(9, 58, 30)),
                state: Some(CycleOutcome::Ok),
                duration_seconds: Some(210),
                had_error: false,
                had_partial: false,
                repos: vec![],
            },
        }
    }

    #[test]
    fn renders_repo_and_snapshot_data() {
        let html = render_html(&fixture());
        assert!(html.contains("repo-a"));
        assert!(html.contains("snap-xyz"));
        assert!(html.contains("home"));
        assert!(html.contains("0 * * * *"));
        assert!(html.contains("host1"));
    }

    #[test]
    fn formats_raw_values_at_render_time() {
        let s = fixture();
        let html = render_html(&s);
        assert!(html.contains("up 1h 5m"), "uptime from uptime_seconds");
        assert!(html.contains(&format_bytes(1_200_000_000)));
        assert!(html.contains("3 snapshots total"));
        assert!(html.contains(&format!("next: {}", local_time(at(12, 0, 0)))));
        assert!(html.contains("3m 30s"));
        assert!(html.contains(&local_time(at(9, 55, 0))));
        assert!(html.contains("(all)"));
        // The recent-snapshot row is the formatted row of its record.
        let row = SnapshotRowView::from_record(&s.recent_snapshots[0].snapshot);
        for cell in [&row.time, &row.files, &row.size, &row.added] {
            assert!(html.contains(&format!("<td>{}</td>", esc(cell))), "{cell}");
        }
    }

    #[test]
    fn placeholders_for_absent_values() {
        let mut s = fixture();
        s.repos[0].snapshot_count = None;
        s.repos[0].unique_stored_size = None;
        s.repos[0].next_run = None;
        s.process.next_run = None;
        s.last_cycle = CycleSummary::default();
        let html = render_html(&s);
        assert!(
            html.contains("<td>—</td><td>—</td><td>—</td><td>Off</td>"),
            "{html}"
        );
        assert!(html.contains("next: —"));
        assert!(html.contains(">n/a<"));
        assert!(html.contains("0 snapshots total"));
    }

    #[test]
    fn repo_row_shows_state_and_freshness() {
        let html = render_html(&fixture());
        assert!(html.contains("<span class=\"state-error\">error</span>"));
        assert!(html.contains(&format!("as of {}", local_time(at(9, 58, 31)))));

        let mut s = fixture();
        s.repos[0].state = RepoState::Pending;
        s.repos[0].refreshed_at = None;
        let html = render_html(&s);
        assert!(html.contains("<span class=\"state-pending\">pending</span>"));
        assert!(!html.contains("as of"));
    }

    #[test]
    fn sources_section_is_expandable() {
        let html = render_html(&fixture());
        assert!(html.contains("<details>"));
        assert!(html.contains("<summary>"));
        // Detail content is present in the markup (collapsed by default
        // visually, but available without JS).
        assert!(html.contains("Exclusions"));
        assert!(html.contains(".nobackup"));
        assert!(html.contains("keep_last: 7"));
    }

    #[test]
    fn read_only_no_action_buttons() {
        let html = render_html(&fixture());
        let lower = html.to_lowercase();
        assert!(!lower.contains("run backup"));
        assert!(!lower.contains("<form"));
        assert!(!lower.contains("<button"));
        assert!(!lower.contains("method=\"post\""));
    }

    #[test]
    fn escapes_html_special_chars() {
        let mut s = fixture();
        s.repos[0].name = "<script>alert(1)</script>".into();
        let html = render_html(&s);
        assert!(!html.contains("<script>alert"));
        assert!(html.contains("&lt;script&gt;"));
    }
}
