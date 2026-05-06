//! Tiny blocking HTTP server for the daemon's read-only status page.
//!
//! One thread, blocking `recv_timeout` so the loop can periodically check
//! the SHUTDOWN atomic and exit cleanly when the daemon stops.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tiny_http::{Header, Method, Response, Server};

use super::render::render_html;
use super::status::SharedStatus;
use crate::error::{CliError, CliResult};

const POLL_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) fn spawn(
    addr: SocketAddr,
    status: SharedStatus,
    shutdown: &'static AtomicBool,
) -> CliResult<std::thread::JoinHandle<()>> {
    let server =
        Server::http(addr).map_err(|e| CliError::from(format!("HTTP bind {addr} failed: {e}")))?;
    tracing::info!(%addr, "read-only status page listening");

    let handle = std::thread::Builder::new()
        .name("vykar-http".to_string())
        .spawn(move || run_loop(server, status, shutdown))?;
    Ok(handle)
}

fn run_loop(server: Server, status: SharedStatus, shutdown: &'static AtomicBool) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        match server.recv_timeout(POLL_INTERVAL) {
            Ok(Some(req)) => {
                if let Err(e) = handle_request(req, &status) {
                    tracing::debug!(error = %e, "http request handler error");
                }
            }
            Ok(None) => {} // timeout — re-check shutdown
            Err(e) => {
                tracing::warn!(error = %e, "http server recv error; exiting");
                break;
            }
        }
    }
    tracing::debug!("http thread exiting");
}

fn handle_request(req: tiny_http::Request, status: &SharedStatus) -> CliResult<()> {
    if !matches!(req.method(), Method::Get | Method::Head) {
        let resp = Response::from_string("method not allowed").with_status_code(405);
        return Ok(req.respond(resp)?);
    }

    let path = req.url().split('?').next().unwrap_or("/");
    match path {
        "/" | "/index.html" => respond_html(req, status),
        "/healthz" => respond_text(req, 200, "ok\n"),
        "/api/status.json" => respond_json(req, status),
        _ => respond_text(req, 404, "not found\n"),
    }
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes())
        .expect("static header literals are always valid")
}

fn respond_html(req: tiny_http::Request, status: &SharedStatus) -> CliResult<()> {
    let snapshot = status.read().expect("status lock poisoned").clone();
    let body = render_html(&snapshot);
    let resp = Response::from_string(body)
        .with_header(header("Content-Type", "text/html; charset=utf-8"))
        .with_header(header("Cache-Control", "no-store"));
    Ok(req.respond(resp)?)
}

fn respond_json(req: tiny_http::Request, status: &SharedStatus) -> CliResult<()> {
    let snapshot = status.read().expect("status lock poisoned").clone();
    let body = serde_json::to_string(&snapshot)?;
    let resp = Response::from_string(body)
        .with_header(header("Content-Type", "application/json"))
        .with_header(header("Cache-Control", "no-store"));
    Ok(req.respond(resp)?)
}

fn respond_text(req: tiny_http::Request, code: u16, body: &str) -> CliResult<()> {
    let resp = Response::from_string(body)
        .with_status_code(code)
        .with_header(header("Content-Type", "text/plain; charset=utf-8"));
    Ok(req.respond(resp)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::daemon::status::{new_shared, DaemonStatus, ProcessInfo, RepoInfo};
    use std::net::TcpListener;
    use std::sync::OnceLock;

    static TEST_SHUTDOWN: OnceLock<AtomicBool> = OnceLock::new();
    fn test_shutdown() -> &'static AtomicBool {
        TEST_SHUTDOWN.get_or_init(|| AtomicBool::new(false))
    }

    fn pick_free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    }

    fn populate(status: &SharedStatus) {
        let mut s = status.write().unwrap();
        *s = DaemonStatus {
            process: ProcessInfo {
                hostname: "h".into(),
                pid: 1,
                version: "0.0.0".into(),
                uptime: "0s".into(),
                next_run: None,
            },
            schedule_brief: "1h".into(),
            repos: vec![RepoInfo {
                name: "rA".into(),
                url: "u".into(),
                snapshots: "0".into(),
                last_snapshot: "N/A".into(),
                size: "0 B".into(),
            }],
            recent_snapshots: vec![],
            sources: vec![],
            last_cycle: Default::default(),
        };
    }

    #[test]
    fn routes_serve_expected_responses() {
        let port = pick_free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let status = new_shared();
        populate(&status);
        let shutdown = test_shutdown();
        shutdown.store(false, Ordering::SeqCst);
        let handle = spawn(addr, status, shutdown).unwrap();

        // Wait briefly for server to be ready by retrying the first request.
        let base = format!("http://{addr}");
        let mut last_err = None;
        let mut html = None;
        for _ in 0..20 {
            match ureq::get(&format!("{base}/")).call() {
                Ok(r) => {
                    html = Some(r);
                    break;
                }
                Err(e) => last_err = Some(e),
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let html = html.unwrap_or_else(|| panic!("no /: {last_err:?}"));
        assert_eq!(html.status(), 200);
        let ct = html.header("content-type").unwrap_or_default().to_string();
        assert!(ct.starts_with("text/html"), "ct = {ct}");
        let body = html.into_string().unwrap();
        assert!(body.contains("rA"));

        let healthz = ureq::get(&format!("{base}/healthz")).call().unwrap();
        assert_eq!(healthz.status(), 200);
        assert_eq!(
            healthz.header("content-type").unwrap_or_default(),
            "text/plain; charset=utf-8"
        );

        let json = ureq::get(&format!("{base}/api/status.json"))
            .call()
            .unwrap();
        assert_eq!(json.status(), 200);
        assert_eq!(
            json.header("content-type").unwrap_or_default(),
            "application/json"
        );
        let body = json.into_string().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["repos"][0]["name"], "rA");

        let nope = ureq::get(&format!("{base}/nope")).call();
        match nope {
            Err(ureq::Error::Status(404, _)) => {}
            other => panic!("expected 404, got {other:?}"),
        }

        shutdown.store(true, Ordering::SeqCst);
        let _ = handle.join();
    }
}
