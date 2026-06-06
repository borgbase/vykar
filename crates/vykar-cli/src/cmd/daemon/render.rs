//! HTML rendering for the daemon's read-only status page.
//!
//! Pure function over `DaemonStatus` — no I/O, no globals. Embeds a small
//! hand-written stylesheet so the page is one self-contained response.

use std::fmt::Write;

use super::status::DaemonStatus;

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

    let total_snapshots: u64 = status
        .repos
        .iter()
        .map(|r| r.snapshots.parse::<u64>().unwrap_or(0))
        .sum();

    let last_outcome = if status.last_cycle.outcome.is_empty() {
        "n/a"
    } else {
        status.last_cycle.outcome.as_str()
    };
    let outcome_class = match last_outcome {
        "ok" => "outcome-ok",
        "partial" => "outcome-partial",
        "errors" => "outcome-errors",
        _ => "",
    };

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
        uptime = esc(&status.process.uptime),
    );

    // Metric cards
    let next_run = status.process.next_run.as_deref().unwrap_or("—");
    let last_started = status.last_cycle.started_at.as_deref().unwrap_or("—");
    let last_duration = status.last_cycle.duration.as_deref().unwrap_or("—");

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
        next = esc(next_run),
        oc = outcome_class,
        outcome = esc(last_outcome),
        started = esc(last_started),
        dur = esc(last_duration),
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
                <th>Last snapshot</th><th>Size</th>\
             </tr></thead><tbody>",
        );
        for r in &status.repos {
            let _ = write!(
                out,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&r.name),
                esc(&r.url),
                esc(&r.snapshots),
                esc(&r.last_snapshot),
                esc(&r.size),
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
                <th>Hostname</th><th>Label</th><th>Files</th><th>Size</th>\
             </tr></thead><tbody>",
        );
        for s in &status.recent_snapshots {
            let _ = write!(
                out,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&s.time),
                esc(&s.repo_name),
                esc(&s.id),
                esc(&s.hostname),
                esc(&s.label),
                esc(&s.files),
                esc(&s.size),
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
            let _ = write!(
                out,
                "<details><summary>\
                    <span class=\"label\">{label}</span>\
                    <span class=\"paths\">{paths}</span>\
                    <span class=\"target\">→ {target}</span>\
                  </summary>\
                  <dl class=\"detail\">",
                label = esc(&s.label),
                paths = esc(&s.paths_summary),
                target = esc(&s.target_repos),
            );

            render_detail_list(&mut out, "Folders", &s.folders);
            render_detail_list(&mut out, "Exclusions", &s.exclusions);
            render_detail_list(&mut out, "Exclude if present", &s.exclude_if_present);
            if !s.options.is_empty() {
                let _ = write!(out, "<dt>Options</dt><dd>{}</dd>", esc(&s.options),);
            }
            render_detail_list(&mut out, "Hooks", &s.hooks);
            if !s.retention.is_empty() {
                let _ = write!(out, "<dt>Retention</dt><dd>{}</dd>", esc(&s.retention),);
            }
            render_detail_list(&mut out, "Command dumps", &s.command_dumps);
            let _ = write!(
                out,
                "<dt>Target repositories</dt><dd>{}</dd>",
                esc(&s.target_repos),
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
    use super::*;
    use crate::cmd::daemon::status::*;

    fn fixture() -> DaemonStatus {
        DaemonStatus {
            process: ProcessInfo {
                hostname: "host1".into(),
                pid: 1234,
                version: "0.14.0".into(),
                uptime: "1h 5m".into(),
                next_run: Some("2026-04-27 12:00:00".into()),
            },
            schedule_brief: "0 * * * *".into(),
            repos: vec![RepoInfo {
                name: "repo-a".into(),
                url: "rest:https://example.com/repo".into(),
                snapshots: "3".into(),
                last_snapshot: "5m ago".into(),
                size: "1.2 GB".into(),
            }],
            recent_snapshots: vec![SnapshotRow {
                id: "snap-xyz".into(),
                time: "2026-04-27 10:00".into(),
                hostname: "src".into(),
                label: "home".into(),
                files: "1234".into(),
                size: "500 MB".into(),
                repo_name: "repo-a".into(),
            }],
            sources: vec![SourceInfo {
                label: "home".into(),
                paths_summary: "/home/user".into(),
                target_repos: "(all)".into(),
                folders: vec!["/home/user".into()],
                exclusions: vec![".cache".into(), "*.tmp".into()],
                exclude_if_present: vec![".nobackup".into()],
                options: "git_ignore".into(),
                hooks: vec!["before: echo hi".into()],
                retention: "keep_last: 7".into(),
                command_dumps: vec![],
            }],
            last_cycle: CycleSummary {
                started_at: Some("2026-04-27 09:55:00".into()),
                finished_at: Some("2026-04-27 09:58:30".into()),
                outcome: "ok".into(),
                duration: Some("3m 30s".into()),
                had_error: false,
                had_partial: false,
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
