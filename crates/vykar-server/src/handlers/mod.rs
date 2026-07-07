pub mod admin;
pub mod objects;

#[cfg(test)]
pub(crate) mod test_helpers;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::Router;
use subtle::ConstantTimeEq;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

/// Body limit for pack uploads (`PUT /{*path}`).
const MAX_OBJECT_BODY_BYTES: usize = 512 * 1024 * 1024; // 512 MiB
/// Body limit for admin JSON requests (POST /?repack, verify-packs, etc.).
/// Sized so the verify-packs byte-volume cap (`MAX_VERIFY_BYTES`) is always the
/// binding constraint, even with very small chunk sizes (~4 KiB → ~24 MiB JSON).
const MAX_ADMIN_BODY_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

pub fn router(state: AppState) -> Router {
    // Admin routes — small body limit for JSON payloads, plus a concurrency
    // cap. Admin operations (repack, verify-packs) are CPU/IO-heavy; bounding
    // them protects the server from a pathological plan monopolizing
    // resources. The per-action timeout lives in admin::dispatch (not a route
    // layer) so repack — whose interrupted response would strand client index
    // updates — can be exempted. Object routes are left untouched: 512 MiB
    // uploads over slow links are legitimate, and slowloris is a reverse-proxy
    // concern.
    let admin_routes = Router::new()
        .route(
            "/",
            axum::routing::get(admin::repo_dispatch).post(admin::repo_action_dispatch),
        )
        .layer(DefaultBodyLimit::max(MAX_ADMIN_BODY_BYTES))
        .layer(tower::limit::GlobalConcurrencyLimitLayer::new(2));

    // Storage object routes — large body limit for pack uploads.
    let object_routes = Router::new()
        .route(
            "/{*path}",
            axum::routing::get(objects::get_or_list)
                .head(objects::head_object)
                .put(objects::put_object)
                .delete(objects::delete_object)
                .post(objects::post_object),
        )
        .layer(DefaultBodyLimit::max(MAX_OBJECT_BODY_BYTES));

    let authed = admin_routes
        .merge(object_routes)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // Health endpoint is unauthenticated
    let public = Router::new().route("/health", axum::routing::get(admin::health));

    public
        .merge(authed)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let expected = state.inner.config.token.as_bytes();

    let provided = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    if provided.as_bytes().ct_eq(expected).into() {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [("Connection", "close"), ("WWW-Authenticate", "Bearer")],
            "invalid or missing token",
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::test_helpers::{assert_status, authed_get, setup_app};

    #[tokio::test]
    async fn admin_route_works_through_hardening_layers() {
        // Confirm the global-concurrency-limit layer on the admin router (and
        // the per-action timeout in admin::dispatch) don't break a normal
        // admin request.
        let (router, _state, _tmp) = setup_app(0);
        let resp = authed_get(router, "/?stats").await;
        assert_status(&resp, StatusCode::OK);
    }
}
