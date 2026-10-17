use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::header::{self, HeaderValue};
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use tracing::{error, info};

use crate::sync::{HttpResponse, TursoSyncServer};

pub struct AppState {
    pub server: TursoSyncServer,
    pub auth_token: Option<String>,
    pub request_timeout: Duration,
}

#[derive(Clone, Copy)]
enum Endpoint {
    Pipeline,
    PullUpdates,
    Checkpoint,
}

pub fn router(state: Arc<AppState>, max_request_bytes: usize) -> Router {
    let api = Router::new()
        .route("/v2/pipeline", post(pipeline))
        .route("/pull-updates", post(pull_updates))
        .route("/db/{name}/v2/pipeline", post(named_pipeline))
        .route("/db/{name}/pull-updates", post(named_pull_updates))
        .route("/admin/checkpoint", post(checkpoint))
        .route("/db/{name}/admin/checkpoint", post(named_checkpoint))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .merge(api)
        .route("/health", get(|| async { "ok" }))
        .fallback(|| async { (StatusCode::NOT_FOUND, "Not Found") })
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(middleware::from_fn_with_state(state.clone(), cors_and_access_log))
        .with_state(state)
}

async fn pipeline(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    dispatch(state, None, Endpoint::Pipeline, body).await
}

async fn pull_updates(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    dispatch(state, None, Endpoint::PullUpdates, body).await
}

async fn named_pipeline(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    dispatch(state, Some(name), Endpoint::Pipeline, body).await
}

async fn named_pull_updates(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    dispatch(state, Some(name), Endpoint::PullUpdates, body).await
}

async fn checkpoint(State(state): State<Arc<AppState>>) -> Response {
    dispatch(state, None, Endpoint::Checkpoint, Bytes::new()).await
}

async fn named_checkpoint(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
    dispatch(state, Some(name), Endpoint::Checkpoint, Bytes::new()).await
}

async fn dispatch(
    state: Arc<AppState>,
    db: Option<String>,
    endpoint: Endpoint,
    body: Bytes,
) -> Response {
    let result = tokio::task::spawn_blocking(move || {
        let handle = match state.server.resolve_db(db.as_deref()) {
            Ok(handle) => handle,
            Err(resp) => return Ok(resp),
        };
        match endpoint {
            Endpoint::Pipeline => state.server.handle_pipeline(&handle, &body),
            Endpoint::PullUpdates => state.server.handle_pull_updates(&handle, &body),
            Endpoint::Checkpoint => state.server.handle_checkpoint(&handle),
        }
    })
    .await;

    let resp = match result {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => {
            error!("request error: {err}");
            internal_error(format!("Internal Server Error: {err}"))
        }
        Err(err) => {
            error!("request handler failed: {err}");
            internal_error("Internal Server Error".to_string())
        }
    };

    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, [(header::CONTENT_TYPE, resp.content_type)], resp.body).into_response()
}

fn internal_error(message: String) -> HttpResponse {
    HttpResponse {
        status: 500,
        content_type: "text/plain".to_string(),
        body: message.into_bytes(),
    }
}

async fn require_auth(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    if let Some(expected) = &state.auth_token {
        let presented = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !presented.is_some_and(|token| constant_time_eq(token.as_bytes(), expected.as_bytes())) {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }
    next.run(req).await
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn cors_and_access_log(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let mut resp = if method == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else {
        // Covers reading the request body, so a client that stalls mid-upload
        // cannot hold a connection forever. Database work already started on a
        // blocking thread runs to completion regardless.
        match tokio::time::timeout(state.request_timeout, next.run(req)).await {
            Ok(resp) => resp,
            Err(_) => (StatusCode::REQUEST_TIMEOUT, "Request Timeout").into_response(),
        }
    };

    let headers = resp.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    // The `*` wildcard never covers Authorization, so it has to be listed explicitly.
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, *"),
    );
    headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("*"));

    if path != "/health" {
        info!(
            "{method} {path} -> {} in {}ms",
            resp.status().as_u16(),
            started.elapsed().as_millis()
        );
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn compares_tokens() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret2"));
        assert!(!constant_time_eq(b"", b"secret"));
    }
}
