use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use archive_core::{
    chunk_rollout_lines, parse_thread_name_update, AgentBatch, Chunk, FileCursor, FileKind,
    IngestResponse, MachineSyncStatus, RolloutLine, StatusCount, SyncContentCounts, SyncFileCounts,
};
use axum::extract::{DefaultBodyLimit, Form, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use reqwest::multipart::{Form as MultipartForm, Part};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

const READ_COOKIE_NAME: &str = "archive_read_token";

#[derive(Clone)]
struct AppState {
    db: PgPool,
    ingest_token: String,
    read_token: String,
    openai_api_key: String,
    embedding_backend: EmbeddingBackend,
    embedding_model: String,
    embedding_dimensions: i32,
    embedding_batch_max_requests: i64,
    embedding_batch_poll_seconds: u64,
    http: Client,
}

#[derive(Debug, Clone)]
struct Config {
    database_url: String,
    ingest_token: String,
    read_token: String,
    openai_api_key: String,
    embedding_backend: EmbeddingBackend,
    embedding_model: String,
    embedding_dimensions: i32,
    embedding_batch_max_requests: i64,
    embedding_batch_poll_seconds: u64,
    bind_addr: SocketAddr,
    max_ingest_body_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbeddingBackend {
    Sync,
    Batch,
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error("missing or invalid bearer token")]
    Unauthorized,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found")]
    NotFound,
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("upstream error: {0}")]
    Upstream(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::Sqlx(_) | ApiError::Upstream(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ThreadSummary {
    thread_id: String,
    name: Option<String>,
    preview: Option<String>,
    cwd: Option<String>,
    source: Option<String>,
    model_provider: Option<String>,
    model: Option<String>,
    git_branch: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct ThreadListParams {
    q: Option<String>,
    cwd: Option<String>,
    source: Option<String>,
    machine: Option<String>,
    model: Option<String>,
    git_branch: Option<String>,
    archived: Option<bool>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct UiThreadListParams {
    q: Option<String>,
    archived: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: String,
    mode: Option<SearchMode>,
    scope: Option<SearchScope>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct UiSearchParams {
    q: Option<String>,
    mode: Option<SearchMode>,
    scope: Option<SearchScope>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct UiLoginQuery {
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UiLoginForm {
    token: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SearchMode {
    Keyword,
    Semantic,
    Hybrid,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SearchScope {
    All,
    Decisions,
    Problems,
    Commands,
    Today,
    Recent,
}

#[derive(Debug, Clone, Serialize)]
struct SearchResult {
    chunk_id: i64,
    thread_id: String,
    turn_id: Option<String>,
    chunk_kind: String,
    role: Option<String>,
    text: String,
    score: f64,
    citation: Citation,
}

#[derive(Debug, Clone, Serialize)]
struct Citation {
    thread_id: String,
    start_line: i64,
    end_line: i64,
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    query: String,
    mode: Option<SearchMode>,
    scope: Option<SearchScope>,
    limit: Option<i64>,
    include_raw: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct CursorParams {
    machine_id: String,
}

#[derive(Debug, Deserialize)]
struct SyncStatusParams {
    machine_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PrioritizeRequest {
    thread_id: Option<String>,
    query: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SearchGroup {
    thread_id: String,
    best_score: f64,
    results: Vec<SearchResult>,
}

#[derive(Debug, Deserialize)]
struct RawParams {
    format: Option<RawFormat>,
    include_private_model_traces: Option<bool>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RawFormat {
    Json,
    Jsonl,
}

#[derive(Debug)]
struct BatchEmbeddingJob {
    chunk_id: i64,
    text: String,
    attempts: i32,
}

#[derive(Debug)]
struct EmbeddingBatchRow {
    id: i64,
    openai_batch_id: String,
}

#[derive(Debug, Serialize)]
struct OpenAiBatchRequestLine<'a> {
    custom_id: String,
    method: &'static str,
    url: &'static str,
    body: OpenAiEmbeddingBody<'a>,
}

#[derive(Debug, Serialize)]
struct OpenAiEmbeddingBody<'a> {
    model: &'a str,
    input: &'a str,
    dimensions: i32,
}

#[derive(Debug, Deserialize)]
struct OpenAiFileObject {
    id: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchObject {
    id: String,
    status: String,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchResultLine {
    custom_id: String,
    response: Option<OpenAiBatchResultResponse>,
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchResultResponse {
    status_code: u16,
    body: Value,
}

enum BatchEmbeddingOutcome {
    Success(Vec<f32>),
    Failure(String),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .context("connect to Postgres")?;
    sqlx::migrate!("./migrations")
        .run(&db)
        .await
        .context("run migrations")?;

    let state = AppState {
        db,
        ingest_token: config.ingest_token,
        read_token: config.read_token,
        openai_api_key: config.openai_api_key,
        embedding_backend: config.embedding_backend,
        embedding_model: config.embedding_model,
        embedding_dimensions: config.embedding_dimensions,
        embedding_batch_max_requests: config.embedding_batch_max_requests,
        embedding_batch_poll_seconds: config.embedding_batch_poll_seconds,
        http: Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("build HTTP client")?,
    };

    spawn_embedding_worker(state.clone());

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/ui", get(ui_threads))
        .route("/ui/login", get(ui_login).post(ui_login_submit))
        .route("/ui/logout", post(ui_logout))
        .route("/ui/search", get(ui_search))
        .route("/ui/threads/:thread_id", get(ui_thread))
        .route("/v1/ingest/batch", post(ingest_batch))
        .route("/v1/ingest/cursors", get(ingest_cursors))
        .route("/v1/sync/status", get(sync_status))
        .route("/v1/embeddings/prioritize", post(prioritize_embeddings))
        .route("/v1/threads", get(list_threads))
        .route("/v1/threads/:thread_id", get(read_thread))
        .route("/v1/threads/:thread_id/raw", get(read_thread_raw))
        .route("/v1/search", get(search))
        .route("/v1/query", post(query))
        .route("/v1/export", get(export_jsonl))
        .layer(DefaultBodyLimit::max(config.max_ingest_body_bytes))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!("archive-server listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("DATABASE_URL is required")?,
            ingest_token: std::env::var("ARCHIVE_INGEST_TOKEN")
                .context("ARCHIVE_INGEST_TOKEN is required")?,
            read_token: std::env::var("ARCHIVE_READ_TOKEN")
                .context("ARCHIVE_READ_TOKEN is required")?,
            openai_api_key: std::env::var("OPENAI_API_KEY").unwrap_or_default(),
            embedding_backend: EmbeddingBackend::from_env()?,
            embedding_model: std::env::var("OPENAI_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".to_string()),
            embedding_dimensions: std::env::var("EMBEDDING_DIMENSIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1536),
            embedding_batch_max_requests: std::env::var("OPENAI_EMBEDDING_BATCH_MAX_REQUESTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(512)
                .max(1),
            embedding_batch_poll_seconds: std::env::var("OPENAI_EMBEDDING_BATCH_POLL_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(30)
                .max(1),
            bind_addr: std::env::var("BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8787".to_string())
                .parse()
                .context("parse BIND_ADDR")?,
            max_ingest_body_bytes: std::env::var("ARCHIVE_MAX_INGEST_BODY_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(64 * 1024 * 1024),
        })
    }
}

impl EmbeddingBackend {
    fn from_env() -> anyhow::Result<Self> {
        match std::env::var("OPENAI_EMBEDDING_BACKEND")
            .unwrap_or_else(|_| "batch".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "sync" => Ok(Self::Sync),
            "batch" => Ok(Self::Batch),
            other => {
                anyhow::bail!("OPENAI_EMBEDDING_BACKEND must be 'sync' or 'batch', got '{other}'")
            }
        }
    }
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> Result<&'static str, ApiError> {
    sqlx::query("SELECT 1").execute(&state.db).await?;
    Ok("ok")
}

async fn ui_login(Query(params): Query<UiLoginQuery>) -> Html<String> {
    render_login_page(params.error.as_deref())
}

async fn ui_login_submit(State(state): State<AppState>, Form(form): Form<UiLoginForm>) -> Response {
    if form.token.trim() != state.read_token {
        return (
            StatusCode::UNAUTHORIZED,
            render_login_page(Some("That token did not match the archive read token.")),
        )
            .into_response();
    }
    let mut response = Redirect::to("/ui").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&auth_cookie(&state.read_token)).unwrap(),
    );
    response
}

async fn ui_logout() -> Response {
    let mut response = Redirect::to("/ui/login").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static("archive_read_token=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),
    );
    response
}

async fn ui_threads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<UiThreadListParams>,
) -> Result<Response, ApiError> {
    if !has_read_access(&headers, &state.read_token) {
        return Ok(Redirect::to("/ui/login").into_response());
    }
    let api_params = ThreadListParams {
        q: params.q.clone(),
        cwd: None,
        source: None,
        machine: None,
        model: None,
        git_branch: None,
        archived: match params.archived.as_deref() {
            Some("active") => Some(false),
            Some("only") => Some(true),
            _ => None,
        },
        limit: params.limit,
        offset: None,
    };
    let threads = fetch_threads(&state.db, &api_params).await?;
    Ok(render_threads_page(&params, &threads).into_response())
}

async fn ui_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<UiSearchParams>,
) -> Result<Response, ApiError> {
    if !has_read_access(&headers, &state.read_token) {
        return Ok(Redirect::to("/ui/login").into_response());
    }
    let mut results = Vec::new();
    let query = params.q.as_deref().unwrap_or("").trim().to_string();
    if !query.is_empty() {
        let mode = params.mode.unwrap_or(SearchMode::Hybrid);
        let scope = params.scope.unwrap_or(SearchScope::All);
        let limit = params.limit.unwrap_or(20);
        let search_results = run_search(&state, &query, mode, scope, limit * 3).await?;
        results = collapse_results(search_results, limit);
    }
    Ok(render_search_page(&params, &results).into_response())
}

async fn ui_thread(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(thread_id): Path<String>,
) -> Result<Response, ApiError> {
    if !has_read_access(&headers, &state.read_token) {
        return Ok(Redirect::to("/ui/login").into_response());
    }
    let (thread, chunks) = fetch_thread_and_chunks(&state.db, &thread_id).await?;
    Ok(render_thread_page(&thread, &chunks).into_response())
}

async fn ingest_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(batch): Json<AgentBatch>,
) -> Result<Json<IngestResponse>, ApiError> {
    require_token(&headers, &state.ingest_token)?;
    if batch.lines.is_empty() {
        return Ok(Json(IngestResponse {
            accepted_lines: 0,
            indexed_chunks: 0,
            file_version: 1,
            quarantined_lines: 0,
        }));
    }

    let mut tx = state.db.begin().await?;
    upsert_machine(&mut tx, &batch).await?;

    let response = match batch.file.kind {
        FileKind::SessionIndex => ingest_session_index(&mut tx, &batch).await?,
        FileKind::ActiveRollout | FileKind::ArchivedRollout => {
            ingest_rollout(&mut tx, &batch).await?
        }
    };

    tx.commit().await?;
    Ok(Json(response))
}

async fn ingest_cursors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<CursorParams>,
) -> Result<Json<Vec<FileCursor>>, ApiError> {
    require_token(&headers, &state.ingest_token)?;
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (relative_path)
               relative_path, kind, archived, file_version, size_bytes, modified_at, file_hash, prefix_hash,
               import_byte_cursor, import_line_cursor
        FROM rollout_files
        WHERE machine_id = $1
        ORDER BY relative_path, file_version DESC
        "#,
    )
    .bind(&params.machine_id)
    .fetch_all(&state.db)
    .await?;
    let cursors = rows
        .into_iter()
        .map(|row| FileCursor {
            relative_path: row.get("relative_path"),
            kind: file_kind_from_db(row.get::<String, _>("kind").as_str()),
            archived: row.get("archived"),
            file_version: row.get("file_version"),
            size_bytes: row.get::<i64, _>("size_bytes") as u64,
            modified_at: row.get("modified_at"),
            file_hash: row.get("file_hash"),
            prefix_hash: row.get("prefix_hash"),
            import_byte_cursor: row.get::<i64, _>("import_byte_cursor") as u64,
            import_line_cursor: row.get("import_line_cursor"),
        })
        .collect();
    Ok(Json(cursors))
}

async fn sync_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<SyncStatusParams>,
) -> Result<Json<Vec<MachineSyncStatus>>, ApiError> {
    require_token(&headers, &state.read_token)?;
    let machines = if let Some(machine_id) = params.machine_id {
        sqlx::query(
            "SELECT machine_id, hostname, installation_id, first_seen_at, last_seen_at FROM machines WHERE machine_id = $1",
        )
        .bind(machine_id)
        .fetch_all(&state.db)
        .await?
    } else {
        sqlx::query(
            "SELECT machine_id, hostname, installation_id, first_seen_at, last_seen_at FROM machines ORDER BY last_seen_at DESC",
        )
        .fetch_all(&state.db)
        .await?
    };

    let mut statuses = Vec::new();
    for machine in machines {
        let machine_id: String = machine.get("machine_id");
        statuses.push(machine_sync_status(&state.db, &machine, &machine_id).await?);
    }
    Ok(Json(statuses))
}

async fn prioritize_embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PrioritizeRequest>,
) -> Result<Json<Value>, ApiError> {
    require_token(&headers, &state.read_token)?;
    let limit = request.limit.unwrap_or(200).clamp(1, 1000);
    let updated = if let Some(thread_id) = request.thread_id {
        prioritize_thread_embeddings(&state.db, &thread_id).await?
    } else if let Some(query) = request.query {
        let results =
            run_search(&state, &query, SearchMode::Hybrid, SearchScope::All, limit).await?;
        let mut updated = 0u64;
        for result in collapse_results(results, limit) {
            updated += prioritize_thread_embeddings(&state.db, &result.thread_id).await?;
        }
        updated
    } else {
        return Err(ApiError::BadRequest(
            "thread_id or query is required".to_string(),
        ));
    };
    Ok(Json(json!({ "prioritized_jobs": updated })))
}

async fn list_threads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ThreadListParams>,
) -> Result<Json<Vec<ThreadSummary>>, ApiError> {
    require_read_access(&headers, &state.read_token)?;
    Ok(Json(fetch_threads(&state.db, &params).await?))
}

async fn read_thread(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(thread_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_read_access(&headers, &state.read_token)?;
    let (thread, chunks) = fetch_thread_and_chunks(&state.db, &thread_id).await?;
    Ok(Json(json!({
        "thread": thread_to_json(&thread),
        "chunks": chunks.iter().map(chunk_to_json_ref).collect::<Vec<_>>(),
        "turns": normalized_turns(&chunks),
    })))
}

async fn read_thread_raw(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(thread_id): Path<String>,
    Query(params): Query<RawParams>,
) -> Result<Response, ApiError> {
    require_read_access(&headers, &state.read_token)?;
    let rows = raw_rows(
        &state.db,
        &thread_id,
        params.include_private_model_traces.unwrap_or(false),
    )
    .await?;
    if rows.is_empty() {
        return Err(ApiError::NotFound);
    }
    if params.format == Some(RawFormat::Jsonl) {
        let body = rows
            .into_iter()
            .map(|row| row.get::<Value, _>("raw").to_string())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        Ok((
            [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
            body,
        )
            .into_response())
    } else {
        let raw = rows
            .into_iter()
            .map(|row| row.get::<Value, _>("raw"))
            .collect::<Vec<_>>();
        Ok(Json(json!({ "thread_id": thread_id, "raw": raw })).into_response())
    }
}

async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<SearchResult>>, ApiError> {
    require_read_access(&headers, &state.read_token)?;
    if params.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q must not be empty".to_string()));
    }
    let mode = params.mode.unwrap_or(SearchMode::Hybrid);
    let scope = params.scope.unwrap_or(SearchScope::All);
    let limit = params.limit.unwrap_or(20);
    let results = run_search(&state, &params.q, mode, scope, limit * 3).await?;
    Ok(Json(collapse_results(results, limit)))
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<Value>, ApiError> {
    require_read_access(&headers, &state.read_token)?;
    if request.query.trim().is_empty() {
        return Err(ApiError::BadRequest("query must not be empty".to_string()));
    }
    let results = run_search(
        &state,
        &request.query,
        request.mode.unwrap_or(SearchMode::Hybrid),
        request.scope.unwrap_or(SearchScope::All),
        request.limit.unwrap_or(20) * 3,
    )
    .await?;
    let results = collapse_results(results, request.limit.unwrap_or(20));
    prioritize_result_embeddings(&state.db, &results).await?;
    let groups = group_results(&results);
    let raw = if request.include_raw.unwrap_or(false) {
        let mut raw = Vec::new();
        for result in &results {
            raw.extend(
                raw_rows(&state.db, &result.thread_id, false)
                    .await?
                    .into_iter()
                    .map(|row| row.get::<Value, _>("raw")),
            );
        }
        Some(raw)
    } else {
        None
    };
    Ok(Json(
        json!({ "results": results, "groups": groups, "raw": raw }),
    ))
}

async fn export_jsonl(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    require_read_access(&headers, &state.read_token)?;
    let rows = sqlx::query(
        r#"
        SELECT t.thread_id, t.name, t.cwd, t.source, t.model_provider, t.model,
               c.id AS chunk_id, c.chunk_kind, c.role, c.text, c.start_line, c.end_line, c.metadata
        FROM chunks c
        JOIN threads t ON t.thread_id = c.thread_id
        ORDER BY t.updated_at DESC NULLS LAST, c.start_line ASC
        LIMIT 10000
        "#,
    )
    .fetch_all(&state.db)
    .await?;
    let body = rows
        .into_iter()
        .map(|row| {
            json!({
                "thread_id": row.get::<String, _>("thread_id"),
                "thread_name": row.get::<Option<String>, _>("name"),
                "cwd": row.get::<Option<String>, _>("cwd"),
                "source": row.get::<Option<String>, _>("source"),
                "model_provider": row.get::<Option<String>, _>("model_provider"),
                "model": row.get::<Option<String>, _>("model"),
                "chunk_id": row.get::<i64, _>("chunk_id"),
                "chunk_kind": row.get::<String, _>("chunk_kind"),
                "role": row.get::<Option<String>, _>("role"),
                "text": row.get::<String, _>("text"),
                "start_line": row.get::<i64, _>("start_line"),
                "end_line": row.get::<i64, _>("end_line"),
                "metadata": row.get::<Value, _>("metadata"),
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    )
        .into_response())
}

async fn fetch_threads(
    db: &PgPool,
    params: &ThreadListParams,
) -> Result<Vec<ThreadSummary>, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let offset = params.offset.unwrap_or(0).max(0);
    let archived_filter = match params.archived {
        Some(true) => "t.archived_at IS NOT NULL",
        Some(false) => "t.archived_at IS NULL",
        None => "TRUE",
    };

    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT t.thread_id, t.name, preview.text AS preview, t.cwd, t.source,
               t.model_provider, t.model, t.git_branch, t.created_at, t.updated_at, t.archived_at
        FROM threads t
        LEFT JOIN LATERAL (
          SELECT c.text FROM chunks c
          WHERE c.thread_id = t.thread_id
          ORDER BY c.start_line ASC LIMIT 1
        ) preview ON TRUE
        LEFT JOIN rollout_lines rl ON rl.thread_id = t.thread_id
        LEFT JOIN rollout_files rf ON rf.id = rl.file_id
        WHERE {archived_filter}
          AND ($1::TEXT IS NULL OR t.cwd ILIKE '%' || $1 || '%')
          AND ($2::TEXT IS NULL OR t.source = $2)
          AND ($3::TEXT IS NULL OR rf.machine_id = $3)
          AND ($4::TEXT IS NULL OR t.model = $4 OR t.model_provider = $4)
          AND ($5::TEXT IS NULL OR t.git_branch = $5)
          AND ($6::TEXT IS NULL OR t.name ILIKE '%' || $6 || '%' OR preview.text ILIKE '%' || $6 || '%')
        ORDER BY t.updated_at DESC NULLS LAST, t.created_at DESC NULLS LAST
        LIMIT $7 OFFSET $8
        "#
    ))
    .bind(&params.cwd)
    .bind(&params.source)
    .bind(&params.machine)
    .bind(&params.model)
    .bind(&params.git_branch)
    .bind(&params.q)
    .bind(limit)
    .bind(offset)
    .fetch_all(db)
    .await?;

    Ok(rows.into_iter().map(thread_summary_from_row).collect())
}

async fn fetch_thread_and_chunks(
    db: &PgPool,
    thread_id: &str,
) -> Result<(sqlx::postgres::PgRow, Vec<sqlx::postgres::PgRow>), ApiError> {
    let thread = sqlx::query("SELECT * FROM threads WHERE thread_id = $1")
        .bind(thread_id)
        .fetch_optional(db)
        .await?
        .ok_or(ApiError::NotFound)?;
    let chunks = sqlx::query(
        r#"
        SELECT id, turn_id, chunk_kind, role, text, start_line, end_line, metadata
        FROM chunks WHERE thread_id = $1 ORDER BY start_line ASC, id ASC
        "#,
    )
    .bind(thread_id)
    .fetch_all(db)
    .await?;
    Ok((thread, chunks))
}

fn render_login_page(error: Option<&str>) -> Html<String> {
    let error_html = error
        .map(|message| format!("<p class=\"error\">{}</p>", html_escape(message)))
        .unwrap_or_default();
    render_page(
        "Codex Archive Login",
        &format!(
            r#"
            <section>
              <h1>Codex Archive</h1>
              <p class="muted">Enter the archive read token to browse threads in a plain old web page.</p>
              {error_html}
              <form method="post" action="/ui/login" class="stack">
                <label for="token">Read token</label>
                <input id="token" name="token" type="password" autocomplete="current-password" required />
                <button type="submit">Open archive</button>
              </form>
            </section>
            "#
        ),
    )
}

fn render_threads_page(params: &UiThreadListParams, threads: &[ThreadSummary]) -> Html<String> {
    let archived_value = params.archived.as_deref().unwrap_or("all");
    let mut items = String::new();
    for thread in threads {
        let title = thread
            .name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(&thread.thread_id);
        let preview = truncate_text(thread.preview.as_deref().unwrap_or(""), 220);
        let updated = fmt_ts(thread.updated_at.or(thread.created_at));
        let archived = if thread.archived_at.is_some() {
            "<span class=\"badge\">archived</span>"
        } else {
            ""
        };
        items.push_str(&format!(
            r#"<li>
              <a href="/ui/threads/{thread_id}"><strong>{title}</strong></a> {archived}
              <div class="meta">{updated} · {model} · {cwd}</div>
              <p>{preview}</p>
            </li>"#,
            thread_id = html_escape(&thread.thread_id),
            title = html_escape(title),
            archived = archived,
            updated = html_escape(&updated),
            model = html_escape(display_str(
                thread.model.as_deref().or(thread.model_provider.as_deref())
            )),
            cwd = html_escape(display_str(thread.cwd.as_deref())),
            preview = html_escape(&preview),
        ));
    }
    if items.is_empty() {
        items.push_str("<li><p class=\"muted\">No threads matched that filter.</p></li>");
    }
    render_page(
        "Codex Archive Threads",
        &format!(
            r#"
            {nav_html}
            <section>
              <h1>Threads</h1>
              <form method="get" action="/ui" class="stack compact">
                <label>Text
                  <input type="text" name="q" value="{q}" />
                </label>
                <label>Archived
                  <select name="archived">
                    <option value="all"{all_selected}>All</option>
                    <option value="active"{active_selected}>Active only</option>
                    <option value="only"{only_selected}>Archived only</option>
                  </select>
                </label>
                <label>Limit
                  <input type="number" min="1" max="200" name="limit" value="{limit}" />
                </label>
                <button type="submit">Filter threads</button>
              </form>
              <ul class="list">{items}</ul>
            </section>
            "#,
            nav_html = render_nav("/ui", None),
            q = html_escape(params.q.as_deref().unwrap_or("")),
            all_selected = selected_attr(archived_value == "all"),
            active_selected = selected_attr(archived_value == "active"),
            only_selected = selected_attr(archived_value == "only"),
            limit = params.limit.unwrap_or(50).clamp(1, 200),
            items = items,
        ),
    )
}

fn render_search_page(params: &UiSearchParams, results: &[SearchResult]) -> Html<String> {
    let mut items = String::new();
    for result in results {
        let text = truncate_text(&result.text, 360);
        items.push_str(&format!(
            r#"<li>
              <a href="/ui/threads/{thread_id}#chunk-{chunk_id}"><strong>{thread_id}</strong></a>
              <div class="meta">{chunk_kind} · {role} · lines {start_line}-{end_line} · score {score:.3}</div>
              <p>{text}</p>
            </li>"#,
            thread_id = html_escape(&result.thread_id),
            chunk_id = result.chunk_id,
            chunk_kind = html_escape(&result.chunk_kind),
            role = html_escape(display_str(result.role.as_deref())),
            start_line = result.citation.start_line,
            end_line = result.citation.end_line,
            score = result.score,
            text = html_escape(&text),
        ));
    }
    if params.q.as_deref().unwrap_or("").trim().is_empty() {
        items.push_str(
            "<li><p class=\"muted\">Search for a term, command, decision, or bug thread.</p></li>",
        );
    } else if items.is_empty() {
        items.push_str("<li><p class=\"muted\">No results for that query.</p></li>");
    }
    render_page(
        "Codex Archive Search",
        &format!(
            r#"
            {nav_html}
            <section>
              <h1>Search</h1>
              <form method="get" action="/ui/search" class="stack compact">
                <label>Query
                  <input type="text" name="q" value="{q}" required />
                </label>
                <label>Mode
                  <select name="mode">
                    <option value="hybrid"{hybrid}>Hybrid</option>
                    <option value="keyword"{keyword}>Keyword</option>
                    <option value="semantic"{semantic}>Semantic</option>
                  </select>
                </label>
                <label>Scope
                  <select name="scope">
                    {scope_options}
                  </select>
                </label>
                <label>Limit
                  <input type="number" min="1" max="100" name="limit" value="{limit}" />
                </label>
                <button type="submit">Search</button>
              </form>
              <ul class="list">{items}</ul>
            </section>
            "#,
            nav_html = render_nav("/ui/search", params.q.as_deref()),
            q = html_escape(params.q.as_deref().unwrap_or("")),
            hybrid = selected_attr(params.mode.unwrap_or(SearchMode::Hybrid) == SearchMode::Hybrid),
            keyword = selected_attr(params.mode == Some(SearchMode::Keyword)),
            semantic = selected_attr(params.mode == Some(SearchMode::Semantic)),
            scope_options = render_scope_options(params.scope.unwrap_or(SearchScope::All)),
            limit = params.limit.unwrap_or(20).clamp(1, 100),
            items = items,
        ),
    )
}

fn render_thread_page(
    thread: &sqlx::postgres::PgRow,
    chunks: &[sqlx::postgres::PgRow],
) -> Html<String> {
    let thread_id = thread.get::<String, _>("thread_id");
    let name = thread.get::<Option<String>, _>("name");
    let model = thread.get::<Option<String>, _>("model");
    let model_provider = thread.get::<Option<String>, _>("model_provider");
    let cwd = thread.get::<Option<String>, _>("cwd");
    let git_branch = thread.get::<Option<String>, _>("git_branch");
    let updated_at = thread.get::<Option<DateTime<Utc>>, _>("updated_at");
    let created_at = thread.get::<Option<DateTime<Utc>>, _>("created_at");
    let title = name
        .clone()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| thread_id.clone());
    let raw_json = format!("/v1/threads/{thread_id}/raw?format=json");
    let raw_jsonl = format!("/v1/threads/{thread_id}/raw?format=jsonl");
    let mut chunk_html = String::new();
    for chunk in chunks {
        let chunk_id = chunk.get::<i64, _>("id");
        let text = chunk.get::<String, _>("text");
        let role = chunk.get::<Option<String>, _>("role");
        let kind = chunk.get::<String, _>("chunk_kind");
        let start_line = chunk.get::<i64, _>("start_line");
        let end_line = chunk.get::<i64, _>("end_line");
        chunk_html.push_str(&format!(
            r#"<article id="chunk-{chunk_id}" class="chunk">
                <div class="meta">{kind} · {role} · lines {start_line}-{end_line}</div>
                <pre>{text}</pre>
              </article>"#,
            chunk_id = chunk_id,
            kind = html_escape(&kind),
            role = html_escape(display_str(role.as_deref())),
            start_line = start_line,
            end_line = end_line,
            text = html_escape(&text),
        ));
    }
    if chunk_html.is_empty() {
        chunk_html.push_str("<p class=\"muted\">No chunks indexed for this thread yet.</p>");
    }
    render_page(
        &format!("Codex Thread {title}"),
        &format!(
            r#"
            {nav_html}
            <section>
              <h1>{title}</h1>
              <dl class="facts">
                <dt>Thread ID</dt><dd>{thread_id}</dd>
                <dt>Updated</dt><dd>{updated}</dd>
                <dt>Model</dt><dd>{model}</dd>
                <dt>CWD</dt><dd>{cwd}</dd>
                <dt>Git branch</dt><dd>{git_branch}</dd>
              </dl>
              <p><a href="{raw_json}">Raw JSON</a> · <a href="{raw_jsonl}">Raw JSONL</a></p>
            </section>
            <section>
              <h2>Chunks</h2>
              {chunk_html}
            </section>
            "#,
            nav_html = render_nav("/ui", Some(&title)),
            title = html_escape(&title),
            thread_id = html_escape(&thread_id),
            updated = html_escape(&fmt_ts(updated_at.or(created_at))),
            model = html_escape(display_str(model.as_deref().or(model_provider.as_deref()))),
            cwd = html_escape(display_str(cwd.as_deref())),
            git_branch = html_escape(display_str(git_branch.as_deref())),
            raw_json = raw_json,
            raw_jsonl = raw_jsonl,
            chunk_html = chunk_html,
        ),
    )
}

fn render_page(title: &str, body: &str) -> Html<String> {
    Html(format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem auto; max-width: 72rem; padding: 0 1rem; line-height: 1.45; }}
    nav {{ display: flex; gap: 1rem; align-items: center; margin-bottom: 1.5rem; flex-wrap: wrap; }}
    nav form {{ margin: 0; }}
    h1, h2 {{ margin-bottom: 0.5rem; }}
    .muted, .meta {{ color: #666; }}
    .meta {{ font-size: 0.95rem; }}
    .stack {{ display: grid; gap: 0.75rem; max-width: 36rem; }}
    .compact {{ grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr)); align-items: end; }}
    label {{ display: grid; gap: 0.25rem; font-weight: 600; }}
    input, select, button {{ font: inherit; padding: 0.45rem 0.6rem; }}
    button {{ cursor: pointer; }}
    .list {{ list-style: none; padding: 0; display: grid; gap: 1rem; }}
    .list li {{ border-top: 1px solid #ccc; padding-top: 1rem; }}
    .badge {{ font-size: 0.8rem; border: 1px solid #888; padding: 0.1rem 0.4rem; border-radius: 999px; }}
    .facts {{ display: grid; grid-template-columns: max-content 1fr; gap: 0.35rem 1rem; }}
    .facts dt {{ font-weight: 700; }}
    .chunk {{ border-top: 1px solid #ccc; padding: 1rem 0; }}
    pre {{ white-space: pre-wrap; word-break: break-word; margin: 0.4rem 0 0; }}
    .error {{ color: #b00020; font-weight: 600; }}
  </style>
</head>
<body>
{body}
</body>
</html>"#,
        title = html_escape(title),
        body = body,
    ))
}

fn render_nav(active: &str, query_hint: Option<&str>) -> String {
    format!(
        r#"<nav>
            <a href="/ui"{threads_class}>Threads</a>
            <a href="/ui/search"{search_class}>Search</a>
            <form method="post" action="/ui/logout"><button type="submit">Log out</button></form>
            <span class="meta">{hint}</span>
          </nav>"#,
        threads_class = if active == "/ui" {
            " aria-current=\"page\""
        } else {
            ""
        },
        search_class = if active == "/ui/search" {
            " aria-current=\"page\""
        } else {
            ""
        },
        hint = html_escape(query_hint.unwrap_or("")),
    )
}

fn render_scope_options(selected: SearchScope) -> String {
    let scopes = [
        (SearchScope::All, "all"),
        (SearchScope::Decisions, "decisions"),
        (SearchScope::Problems, "problems"),
        (SearchScope::Commands, "commands"),
        (SearchScope::Today, "today"),
        (SearchScope::Recent, "recent"),
    ];
    scopes
        .into_iter()
        .map(|(scope, label)| {
            format!(
                "<option value=\"{label}\"{selected}>{label}</option>",
                label = label,
                selected = selected_attr(scope == selected)
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn selected_attr(selected: bool) -> &'static str {
    if selected {
        " selected"
    } else {
        ""
    }
}

fn display_str(value: Option<&str>) -> &str {
    value.filter(|text| !text.trim().is_empty()).unwrap_or("-")
}

fn fmt_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|timestamp| timestamp.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    let truncated = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

async fn upsert_machine(
    tx: &mut Transaction<'_, Postgres>,
    batch: &AgentBatch,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO machines(machine_id, hostname, installation_id)
        VALUES ($1, $2, $3)
        ON CONFLICT(machine_id) DO UPDATE SET
          hostname = EXCLUDED.hostname,
          installation_id = COALESCE(EXCLUDED.installation_id, machines.installation_id),
          last_seen_at = now()
        "#,
    )
    .bind(&batch.machine.machine_id)
    .bind(&batch.machine.hostname)
    .bind(&batch.machine.installation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn ingest_session_index(
    tx: &mut Transaction<'_, Postgres>,
    batch: &AgentBatch,
) -> Result<IngestResponse, ApiError> {
    let file_id = upsert_file(tx, batch).await?;
    let mut accepted = 0usize;
    for line in &batch.lines {
        if let Some(update) = parse_thread_name_update(&line.raw) {
            sqlx::query(
                r#"
                INSERT INTO threads(thread_id, name, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT(thread_id) DO UPDATE SET
                  name = EXCLUDED.name,
                  updated_at = COALESCE(EXCLUDED.updated_at, threads.updated_at),
                  last_seen_at = now()
                "#,
            )
            .bind(update.thread_id)
            .bind(update.thread_name)
            .bind(update.updated_at)
            .execute(&mut **tx)
            .await?;
            accepted += 1;
        }
    }
    update_file_cursor(tx, file_id, batch).await?;
    Ok(IngestResponse {
        accepted_lines: accepted,
        indexed_chunks: 0,
        file_version: current_file_version(tx, file_id).await?,
        quarantined_lines: 0,
    })
}

async fn ingest_rollout(
    tx: &mut Transaction<'_, Postgres>,
    batch: &AgentBatch,
) -> Result<IngestResponse, ApiError> {
    let file_id = upsert_file(tx, batch).await?;
    let file_version = current_file_version(tx, file_id).await?;
    let mut quarantined = 0usize;
    let mut parsed = Vec::new();
    for line in &batch.lines {
        match RolloutLine::parse(&line.raw) {
            Ok(rollout) => parsed.push((line, rollout)),
            Err(err) => {
                record_ingest_error(
                    tx,
                    batch,
                    file_version,
                    Some(line),
                    "parse_error",
                    &err.to_string(),
                )
                .await?;
                quarantined += 1;
            }
        }
    }
    let Some(thread_id) = parsed
        .iter()
        .find_map(|(_, line)| line.session_metadata().map(|meta| meta.thread_id))
    else {
        update_file_cursor(tx, file_id, batch).await?;
        return Ok(IngestResponse {
            accepted_lines: 0,
            indexed_chunks: 0,
            file_version,
            quarantined_lines: quarantined,
        });
    };

    for (_, line) in &parsed {
        if let Some(meta) = line.session_metadata() {
            upsert_thread_from_meta(tx, &meta, batch.file.kind == FileKind::ArchivedRollout)
                .await?;
        }
    }
    sqlx::query(
        "INSERT INTO threads(thread_id, updated_at) VALUES ($1, now()) ON CONFLICT(thread_id) DO UPDATE SET updated_at = now(), last_seen_at = now()",
    )
    .bind(&thread_id)
    .execute(&mut **tx)
    .await?;

    let mut accepted = 0usize;
    for (agent_line, line) in &parsed {
        let result = sqlx::query(
            r#"
            INSERT INTO rollout_lines(thread_id, file_id, file_version, line_number, byte_start, byte_end, timestamp, type, raw, payload, content_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(&thread_id)
        .bind(file_id)
        .bind(file_version)
        .bind(agent_line.line_number)
        .bind(agent_line.byte_start as i64)
        .bind(agent_line.byte_end as i64)
        .bind(line.timestamp)
        .bind(&line.item_type)
        .bind(postgres_jsonb(&line.raw))
        .bind(postgres_jsonb(&line.payload))
        .bind(&agent_line.content_hash)
        .execute(&mut **tx)
        .await?;
        accepted += result.rows_affected() as usize;
    }

    let chunk_input = parsed
        .into_iter()
        .map(|(agent_line, rollout)| (agent_line.line_number, rollout))
        .collect::<Vec<_>>();
    let chunks = chunk_rollout_lines(&chunk_input);
    let indexed = insert_chunks(tx, file_id, &thread_id, &chunks).await?;
    update_file_cursor(tx, file_id, batch).await?;

    Ok(IngestResponse {
        accepted_lines: accepted,
        indexed_chunks: indexed,
        file_version,
        quarantined_lines: quarantined,
    })
}

async fn upsert_file(
    tx: &mut Transaction<'_, Postgres>,
    batch: &AgentBatch,
) -> Result<i64, sqlx::Error> {
    let existing = sqlx::query(
        r#"
        SELECT id, file_version, size_bytes, prefix_hash
        FROM rollout_files
        WHERE machine_id = $1 AND relative_path = $2
        ORDER BY file_version DESC LIMIT 1
        "#,
    )
    .bind(&batch.machine.machine_id)
    .bind(&batch.file.relative_path)
    .fetch_optional(&mut **tx)
    .await?;

    let archived = batch.file.kind == FileKind::ArchivedRollout;
    let kind = match batch.file.kind {
        FileKind::ActiveRollout => "active_rollout",
        FileKind::ArchivedRollout => "archived_rollout",
        FileKind::SessionIndex => "session_index",
    };

    let version = existing
        .as_ref()
        .map(|row| {
            let previous_size: i64 = row.get("size_bytes");
            let previous_prefix: String = row.get("prefix_hash");
            let reset = previous_size > batch.file.size_bytes as i64
                || previous_prefix != batch.file.prefix_hash;
            row.get::<i32, _>("file_version") + i32::from(reset)
        })
        .unwrap_or(1);

    let row = sqlx::query(
        r#"
        INSERT INTO rollout_files(machine_id, relative_path, kind, archived, file_version, size_bytes, modified_at, file_hash, prefix_hash)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT(machine_id, relative_path, file_version) DO UPDATE SET
          kind = EXCLUDED.kind,
          archived = EXCLUDED.archived,
          size_bytes = EXCLUDED.size_bytes,
          modified_at = EXCLUDED.modified_at,
          file_hash = EXCLUDED.file_hash,
          prefix_hash = EXCLUDED.prefix_hash,
          last_imported_at = now()
        RETURNING id
        "#,
    )
    .bind(&batch.machine.machine_id)
    .bind(&batch.file.relative_path)
    .bind(kind)
    .bind(archived)
    .bind(version)
    .bind(batch.file.size_bytes as i64)
    .bind(batch.file.modified_at)
    .bind(&batch.file.file_hash)
    .bind(&batch.file.prefix_hash)
    .fetch_one(&mut **tx)
    .await?;
    Ok(row.get("id"))
}

async fn current_file_version(
    tx: &mut Transaction<'_, Postgres>,
    file_id: i64,
) -> Result<i32, sqlx::Error> {
    let row = sqlx::query("SELECT file_version FROM rollout_files WHERE id = $1")
        .bind(file_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.get("file_version"))
}

async fn update_file_cursor(
    tx: &mut Transaction<'_, Postgres>,
    file_id: i64,
    batch: &AgentBatch,
) -> Result<(), sqlx::Error> {
    let max_line = batch
        .lines
        .iter()
        .map(|line| line.line_number)
        .max()
        .unwrap_or(0);
    let max_byte = batch
        .lines
        .iter()
        .map(|line| line.byte_end)
        .max()
        .unwrap_or(0) as i64;
    sqlx::query(
        "UPDATE rollout_files SET import_byte_cursor = GREATEST(import_byte_cursor, $2), import_line_cursor = GREATEST(import_line_cursor, $3), last_imported_at = now() WHERE id = $1",
    )
    .bind(file_id)
    .bind(max_byte)
    .bind(max_line)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_thread_from_meta(
    tx: &mut Transaction<'_, Postgres>,
    meta: &archive_core::SessionMetadata,
    archived: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO threads(thread_id, cwd, source, thread_source, agent_nickname, agent_role, agent_path,
                            model_provider, cli_version, git_branch, git_sha, git_origin_url,
                            memory_mode, forked_from_id, created_at, updated_at, archived_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,COALESCE($15, now()), CASE WHEN $16 THEN now() ELSE NULL END)
        ON CONFLICT(thread_id) DO UPDATE SET
          cwd = COALESCE(EXCLUDED.cwd, threads.cwd),
          source = COALESCE(EXCLUDED.source, threads.source),
          thread_source = COALESCE(EXCLUDED.thread_source, threads.thread_source),
          agent_nickname = COALESCE(EXCLUDED.agent_nickname, threads.agent_nickname),
          agent_role = COALESCE(EXCLUDED.agent_role, threads.agent_role),
          agent_path = COALESCE(EXCLUDED.agent_path, threads.agent_path),
          model_provider = COALESCE(EXCLUDED.model_provider, threads.model_provider),
          cli_version = COALESCE(EXCLUDED.cli_version, threads.cli_version),
          git_branch = COALESCE(EXCLUDED.git_branch, threads.git_branch),
          git_sha = COALESCE(EXCLUDED.git_sha, threads.git_sha),
          git_origin_url = COALESCE(EXCLUDED.git_origin_url, threads.git_origin_url),
          memory_mode = COALESCE(EXCLUDED.memory_mode, threads.memory_mode),
          forked_from_id = COALESCE(EXCLUDED.forked_from_id, threads.forked_from_id),
          created_at = COALESCE(threads.created_at, EXCLUDED.created_at),
          updated_at = GREATEST(COALESCE(threads.updated_at, EXCLUDED.updated_at), COALESCE(EXCLUDED.updated_at, threads.updated_at)),
          archived_at = COALESCE(threads.archived_at, EXCLUDED.archived_at),
          last_seen_at = now()
        "#,
    )
    .bind(&meta.thread_id)
    .bind(&meta.cwd)
    .bind(&meta.source)
    .bind(postgres_jsonb_option(&meta.thread_source))
    .bind(&meta.agent_nickname)
    .bind(&meta.agent_role)
    .bind(&meta.agent_path)
    .bind(&meta.model_provider)
    .bind(&meta.cli_version)
    .bind(&meta.git_branch)
    .bind(&meta.git_sha)
    .bind(&meta.git_origin_url)
    .bind(&meta.memory_mode)
    .bind(&meta.forked_from_id)
    .bind(meta.created_at)
    .bind(archived)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_chunks(
    tx: &mut Transaction<'_, Postgres>,
    file_id: i64,
    thread_id: &str,
    chunks: &[Chunk],
) -> Result<usize, sqlx::Error> {
    let mut indexed = 0usize;
    for chunk in chunks {
        let row = sqlx::query(
            r#"
            INSERT INTO chunks(thread_id, file_id, turn_id, chunk_kind, role, text, start_line, end_line, metadata, content_hash)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT DO NOTHING
            RETURNING id
            "#,
        )
        .bind(thread_id)
        .bind(file_id)
        .bind(&chunk.turn_id)
        .bind(chunk.chunk_kind.as_str())
        .bind(&chunk.role)
        .bind(postgres_text(&chunk.text))
        .bind(chunk.start_line)
        .bind(chunk.end_line)
        .bind(postgres_jsonb(&chunk.metadata))
        .bind(&chunk.content_hash)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(row) = row {
            indexed += 1;
            let chunk_id: i64 = row.get("id");
            sqlx::query("INSERT INTO embedding_jobs(chunk_id) VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(chunk_id)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(indexed)
}

async fn record_ingest_error(
    tx: &mut Transaction<'_, Postgres>,
    batch: &AgentBatch,
    file_version: i32,
    line: Option<&archive_core::AgentLine>,
    error_kind: &str,
    error_message: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO ingest_errors(machine_id, relative_path, file_version, line_number, byte_start, byte_end,
                                  content_hash, error_kind, error_message, raw_preview)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&batch.machine.machine_id)
    .bind(&batch.file.relative_path)
    .bind(file_version)
    .bind(line.map(|line| line.line_number))
    .bind(line.map(|line| line.byte_start as i64))
    .bind(line.map(|line| line.byte_end as i64))
    .bind(line.map(|line| line.content_hash.as_str()))
    .bind(error_kind)
    .bind(error_message)
    .bind(line.map(|line| line.raw.chars().take(500).collect::<String>()))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn machine_sync_status(
    db: &PgPool,
    machine: &sqlx::postgres::PgRow,
    machine_id: &str,
) -> Result<MachineSyncStatus, ApiError> {
    let files = sqlx::query(
        r#"
        SELECT
          count(*) FILTER (WHERE kind = 'active_rollout') AS active_rollout,
          count(*) FILTER (WHERE kind = 'archived_rollout') AS archived_rollout,
          count(*) FILTER (WHERE kind = 'session_index') AS session_index,
          count(*) AS total,
          count(*) FILTER (WHERE relative_path LIKE 'sessions/' || to_char(now(), 'YYYY/MM/DD') || '/%'
                            OR relative_path LIKE 'archived_sessions/' || to_char(now(), 'YYYY/MM/DD') || '/%') AS today
        FROM rollout_files WHERE machine_id = $1
        "#,
    )
    .bind(machine_id)
    .fetch_one(db)
    .await?;
    let content = sqlx::query(
        r#"
        SELECT
          count(DISTINCT rl.thread_id) AS threads,
          count(DISTINCT rl.id) AS raw_lines,
          count(DISTINCT c.id) AS chunks,
          count(DISTINCT rl.id) FILTER (WHERE rf.relative_path LIKE 'sessions/' || to_char(now(), 'YYYY/MM/DD') || '/%'
                                         OR rf.relative_path LIKE 'archived_sessions/' || to_char(now(), 'YYYY/MM/DD') || '/%') AS today_raw_lines,
          count(DISTINCT c.id) FILTER (WHERE rf.relative_path LIKE 'sessions/' || to_char(now(), 'YYYY/MM/DD') || '/%'
                                        OR rf.relative_path LIKE 'archived_sessions/' || to_char(now(), 'YYYY/MM/DD') || '/%') AS today_chunks
        FROM rollout_files rf
        LEFT JOIN rollout_lines rl ON rl.file_id = rf.id
        LEFT JOIN chunks c ON c.file_id = rf.id
        WHERE rf.machine_id = $1
        "#,
    )
    .bind(machine_id)
    .fetch_one(db)
    .await?;
    let embeddings = status_counts(
        db,
        r#"
        SELECT ej.status AS status, count(*) AS count
        FROM embedding_jobs ej
        JOIN chunks c ON c.id = ej.chunk_id
        JOIN rollout_files rf ON rf.id = c.file_id
        WHERE rf.machine_id = $1
        GROUP BY ej.status ORDER BY ej.status
        "#,
        machine_id,
    )
    .await?;
    let ingest_errors = status_counts(
        db,
        "SELECT error_kind AS status, count(*) AS count FROM ingest_errors WHERE machine_id = $1 GROUP BY error_kind ORDER BY error_kind",
        machine_id,
    )
    .await?;
    Ok(MachineSyncStatus {
        machine_id: machine_id.to_string(),
        hostname: machine.get("hostname"),
        installation_id: machine.get("installation_id"),
        first_seen_at: machine.get("first_seen_at"),
        last_seen_at: machine.get("last_seen_at"),
        files: SyncFileCounts {
            active_rollout: files.get("active_rollout"),
            archived_rollout: files.get("archived_rollout"),
            session_index: files.get("session_index"),
            total: files.get("total"),
            today: files.get("today"),
        },
        content: SyncContentCounts {
            threads: content.get("threads"),
            raw_lines: content.get("raw_lines"),
            chunks: content.get("chunks"),
            today_raw_lines: content.get("today_raw_lines"),
            today_chunks: content.get("today_chunks"),
        },
        embeddings,
        ingest_errors,
    })
}

async fn status_counts(
    db: &PgPool,
    sql: &str,
    machine_id: &str,
) -> Result<Vec<StatusCount>, ApiError> {
    let rows = sqlx::query(sql).bind(machine_id).fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(|row| StatusCount {
            status: row.get("status"),
            count: row.get("count"),
        })
        .collect())
}

async fn run_search(
    state: &AppState,
    query: &str,
    mode: SearchMode,
    scope: SearchScope,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    let limit = limit.clamp(1, 100);
    match mode {
        SearchMode::Keyword => keyword_search(&state.db, query, scope, limit).await,
        SearchMode::Semantic => semantic_search(state, query, scope, limit).await,
        SearchMode::Hybrid => hybrid_search(state, query, scope, limit).await,
    }
}

async fn keyword_search(
    db: &PgPool,
    query: &str,
    scope: SearchScope,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    let rows = sqlx::query(
        r#"
        SELECT id, thread_id, turn_id, chunk_kind, role, text, start_line, end_line,
               ts_rank_cd(search_tsv, plainto_tsquery('simple', $1))::float8 AS score
        FROM chunks
        WHERE search_tsv @@ plainto_tsquery('simple', $1)
          AND search_scope_matches($2, chunk_kind, role, text, created_at)
        ORDER BY score DESC, id DESC
        LIMIT $3
        "#,
    )
    .bind(query)
    .bind(search_scope_name(scope))
    .bind(limit)
    .fetch_all(db)
    .await?;
    Ok(rows.into_iter().map(search_result_from_row).collect())
}

async fn semantic_search(
    state: &AppState,
    query: &str,
    scope: SearchScope,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    let embedding = embed_text(state, query).await?;
    let vector = vector_literal(&embedding);
    let rows = sqlx::query(
        r#"
        SELECT id, thread_id, turn_id, chunk_kind, role, text, start_line, end_line,
               (1 - (embedding <=> $1::vector))::float8 AS score
        FROM chunks
        WHERE embedding IS NOT NULL
          AND search_scope_matches($2, chunk_kind, role, text, created_at)
        ORDER BY embedding <=> $1::vector
        LIMIT $3
        "#,
    )
    .bind(vector)
    .bind(search_scope_name(scope))
    .bind(limit)
    .fetch_all(&state.db)
    .await?;
    Ok(rows.into_iter().map(search_result_from_row).collect())
}

async fn hybrid_search(
    state: &AppState,
    query: &str,
    scope: SearchScope,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    if state.openai_api_key.is_empty() {
        return keyword_search(&state.db, query, scope, limit).await;
    }
    let embedding = embed_text(state, query).await?;
    let vector = vector_literal(&embedding);
    let rows = sqlx::query(
        r#"
        WITH keyword AS (
          SELECT id, row_number() OVER (ORDER BY ts_rank_cd(search_tsv, plainto_tsquery('simple', $1)) DESC) AS rank
          FROM chunks
          WHERE search_tsv @@ plainto_tsquery('simple', $1)
            AND search_scope_matches($3, chunk_kind, role, text, created_at)
          LIMIT $4
        ),
        semantic AS (
          SELECT id, row_number() OVER (ORDER BY embedding <=> $2::vector) AS rank
          FROM chunks
          WHERE embedding IS NOT NULL
            AND search_scope_matches($3, chunk_kind, role, text, created_at)
          LIMIT $4
        ),
        fused AS (
          SELECT id, SUM(score)::float8 AS score
          FROM (
            SELECT id, 1.0 / (60 + rank) AS score FROM keyword
            UNION ALL
            SELECT id, 1.0 / (60 + rank) AS score FROM semantic
          ) scores
          GROUP BY id
        )
        SELECT c.id, c.thread_id, c.turn_id, c.chunk_kind, c.role, c.text, c.start_line, c.end_line, fused.score
        FROM fused
        JOIN chunks c ON c.id = fused.id
        ORDER BY fused.score DESC, c.id DESC
        LIMIT $4
        "#,
    )
    .bind(query)
    .bind(vector)
    .bind(search_scope_name(scope))
    .bind(limit)
    .fetch_all(&state.db)
    .await?;
    Ok(rows.into_iter().map(search_result_from_row).collect())
}

async fn embed_text(state: &AppState, text: &str) -> Result<Vec<f32>, ApiError> {
    if state.openai_api_key.is_empty() {
        return Err(ApiError::Upstream(
            "OPENAI_API_KEY is not configured".to_string(),
        ));
    }
    let response = state
        .http
        .post("https://api.openai.com/v1/embeddings")
        .bearer_auth(&state.openai_api_key)
        .json(&json!({
            "model": state.embedding_model,
            "input": text,
            "dimensions": state.embedding_dimensions
        }))
        .send()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::Upstream(format!(
            "OpenAI embeddings request failed: {}",
            response.status()
        )));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    let embedding = value
        .pointer("/data/0/embedding")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::Upstream("missing embedding in response".to_string()))?
        .iter()
        .filter_map(|value| value.as_f64().map(|number| number as f32))
        .collect::<Vec<_>>();
    Ok(embedding)
}

fn spawn_embedding_worker(state: AppState) {
    tokio::spawn(async move {
        if state.openai_api_key.is_empty() {
            info!("OPENAI_API_KEY is not set; embedding worker is idle");
            return;
        }
        let poll_seconds = match state.embedding_backend {
            EmbeddingBackend::Sync => 5,
            EmbeddingBackend::Batch => state.embedding_batch_poll_seconds,
        };
        loop {
            if let Err(err) = run_embedding_cycle(&state).await {
                error!(error = %err, "embedding worker failed");
            }
            tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
        }
    });
}

async fn run_embedding_cycle(state: &AppState) -> Result<(), ApiError> {
    match state.embedding_backend {
        EmbeddingBackend::Sync => run_sync_embedding_once(state).await,
        EmbeddingBackend::Batch => run_batch_embedding_once(state).await,
    }
}

async fn run_sync_embedding_once(state: &AppState) -> Result<(), ApiError> {
    let rows = sqlx::query(
        r#"
        UPDATE embedding_jobs
        SET status = 'running', locked_at = now(), attempts = attempts + 1, updated_at = now()
        WHERE chunk_id IN (
          SELECT chunk_id FROM embedding_jobs
          WHERE status IN ('pending', 'failed') AND attempts < 5
          ORDER BY created_at DESC, chunk_id ASC LIMIT 8
        )
        RETURNING chunk_id
        "#,
    )
    .fetch_all(&state.db)
    .await?;
    for row in rows {
        let chunk_id: i64 = row.get("chunk_id");
        let chunk = sqlx::query("SELECT text FROM chunks WHERE id = $1")
            .bind(chunk_id)
            .fetch_one(&state.db)
            .await?;
        let text: String = chunk.get("text");
        match embed_text(state, &text).await {
            Ok(embedding) => {
                let vector = vector_literal(&embedding);
                sqlx::query(
                    "UPDATE chunks SET embedding = $2::vector, embedding_model = $3 WHERE id = $1",
                )
                .bind(chunk_id)
                .bind(vector)
                .bind(&state.embedding_model)
                .execute(&state.db)
                .await?;

                sqlx::query(
                    "UPDATE embedding_jobs SET status = 'done', completed_at = now(), updated_at = now(), last_error = NULL WHERE chunk_id = $1",
                )
                .bind(chunk_id)
                .execute(&state.db)
                .await?;
            }
            Err(err) => {
                sqlx::query(
                    "UPDATE embedding_jobs SET status = 'failed', last_error = $2, updated_at = now() WHERE chunk_id = $1",
                )
                .bind(chunk_id)
                .bind(err.to_string())
                .execute(&state.db)
                .await?;
            }
        }
    }
    Ok(())
}

async fn run_batch_embedding_once(state: &AppState) -> Result<(), ApiError> {
    poll_openai_batches(state).await?;
    submit_openai_embedding_batch(state).await?;
    Ok(())
}

async fn submit_openai_embedding_batch(state: &AppState) -> Result<(), ApiError> {
    let jobs = claim_batch_embedding_jobs(state).await?;
    if jobs.is_empty() {
        return Ok(());
    }

    let mut lines = Vec::with_capacity(jobs.len());
    for job in &jobs {
        lines.push(
            serde_json::to_string(&OpenAiBatchRequestLine {
                custom_id: batch_custom_id(job.chunk_id, job.attempts),
                method: "POST",
                url: "/v1/embeddings",
                body: OpenAiEmbeddingBody {
                    model: &state.embedding_model,
                    input: &job.text,
                    dimensions: state.embedding_dimensions,
                },
            })
            .map_err(|err| ApiError::Upstream(err.to_string()))?,
        );
    }
    let payload = lines.join("\n") + "\n";

    match upload_openai_batch_file(state, payload.into_bytes()).await {
        Ok(file_id) => match create_openai_batch(state, &file_id).await {
            Ok(batch) => {
                let batch_row_id =
                    insert_openai_batch(state, &batch, &file_id, jobs.len() as i32).await?;
                assign_jobs_to_batch(state, batch_row_id, &jobs).await?;
            }
            Err(err) => {
                mark_batch_submission_failed(
                    &state.db,
                    &jobs,
                    &format!("create OpenAI batch: {err}"),
                )
                .await?;
                return Err(err);
            }
        },
        Err(err) => {
            mark_batch_submission_failed(&state.db, &jobs, &format!("upload batch input: {err}"))
                .await?;
            return Err(err);
        }
    }

    Ok(())
}

async fn claim_batch_embedding_jobs(state: &AppState) -> Result<Vec<BatchEmbeddingJob>, ApiError> {
    let rows = sqlx::query(
        r#"
        WITH picked AS (
          SELECT ej.chunk_id
          FROM embedding_jobs ej
          WHERE ej.status IN ('pending', 'failed')
            AND ej.attempts < 5
            AND ej.batch_id IS NULL
          ORDER BY ej.created_at DESC, ej.chunk_id ASC
          LIMIT $1
        )
        UPDATE embedding_jobs ej
        SET status = 'running',
            locked_at = now(),
            attempts = attempts + 1,
            updated_at = now()
        FROM picked, chunks c
        WHERE ej.chunk_id = picked.chunk_id
          AND c.id = picked.chunk_id
        RETURNING ej.chunk_id, c.text, ej.attempts
        "#,
    )
    .bind(state.embedding_batch_max_requests)
    .fetch_all(&state.db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BatchEmbeddingJob {
            chunk_id: row.get("chunk_id"),
            text: row.get("text"),
            attempts: row.get("attempts"),
        })
        .collect())
}

async fn upload_openai_batch_file(state: &AppState, payload: Vec<u8>) -> Result<String, ApiError> {
    let response = state
        .http
        .post("https://api.openai.com/v1/files")
        .bearer_auth(&state.openai_api_key)
        .multipart(
            MultipartForm::new().text("purpose", "batch").part(
                "file",
                Part::bytes(payload)
                    .file_name("archive-embeddings.jsonl")
                    .mime_str("application/jsonl")
                    .map_err(|err| ApiError::Upstream(err.to_string()))?,
            ),
        )
        .send()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::Upstream(format!(
            "OpenAI file upload failed: {}",
            response.status()
        )));
    }
    let file: OpenAiFileObject = response
        .json()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    Ok(file.id)
}

async fn create_openai_batch(
    state: &AppState,
    input_file_id: &str,
) -> Result<OpenAiBatchObject, ApiError> {
    let response = state
        .http
        .post("https://api.openai.com/v1/batches")
        .bearer_auth(&state.openai_api_key)
        .json(&json!({
            "input_file_id": input_file_id,
            "endpoint": "/v1/embeddings",
            "completion_window": "24h"
        }))
        .send()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::Upstream(format!(
            "OpenAI batch create failed: {}",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))
}

async fn insert_openai_batch(
    state: &AppState,
    batch: &OpenAiBatchObject,
    input_file_id: &str,
    request_count: i32,
) -> Result<i64, ApiError> {
    let row = sqlx::query(
        r#"
        INSERT INTO embedding_batches (
          openai_batch_id,
          openai_input_file_id,
          openai_output_file_id,
          openai_error_file_id,
          status,
          request_count,
          submitted_at,
          updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, now(), now())
        RETURNING id
        "#,
    )
    .bind(&batch.id)
    .bind(input_file_id)
    .bind(&batch.output_file_id)
    .bind(&batch.error_file_id)
    .bind(&batch.status)
    .bind(request_count)
    .fetch_one(&state.db)
    .await?;
    Ok(row.get("id"))
}

async fn assign_jobs_to_batch(
    state: &AppState,
    batch_id: i64,
    jobs: &[BatchEmbeddingJob],
) -> Result<(), ApiError> {
    for job in jobs {
        sqlx::query(
            "UPDATE embedding_jobs SET status = 'submitted', batch_id = $2, batch_custom_id = $3, updated_at = now() WHERE chunk_id = $1",
        )
        .bind(job.chunk_id)
        .bind(batch_id)
        .bind(batch_custom_id(job.chunk_id, job.attempts))
        .execute(&state.db)
        .await?;
    }
    Ok(())
}

async fn mark_batch_submission_failed(
    db: &PgPool,
    jobs: &[BatchEmbeddingJob],
    message: &str,
) -> Result<(), ApiError> {
    for job in jobs {
        sqlx::query(
            "UPDATE embedding_jobs SET status = 'failed', batch_id = NULL, batch_custom_id = NULL, last_error = $2, updated_at = now() WHERE chunk_id = $1",
        )
        .bind(job.chunk_id)
        .bind(message)
        .execute(db)
        .await?;
    }
    Ok(())
}

async fn poll_openai_batches(state: &AppState) -> Result<(), ApiError> {
    let rows = sqlx::query(
        r#"
        SELECT id, openai_batch_id, openai_output_file_id, openai_error_file_id, status
        FROM embedding_batches
        WHERE results_applied_at IS NULL
        ORDER BY submitted_at ASC
        LIMIT 4
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    for row in rows {
        let batch = EmbeddingBatchRow {
            id: row.get("id"),
            openai_batch_id: row.get("openai_batch_id"),
        };
        let remote = retrieve_openai_batch(state, &batch.openai_batch_id).await?;
        update_openai_batch_status(&state.db, batch.id, &remote).await?;
        match remote.status.as_str() {
            "completed" => apply_openai_batch_results(state, batch.id, &remote).await?,
            "failed" | "expired" | "cancelled" => {
                fail_openai_batch_jobs(
                    &state.db,
                    batch.id,
                    &format!("OpenAI batch {}", remote.status),
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn retrieve_openai_batch(
    state: &AppState,
    batch_id: &str,
) -> Result<OpenAiBatchObject, ApiError> {
    let response = state
        .http
        .get(format!("https://api.openai.com/v1/batches/{batch_id}"))
        .bearer_auth(&state.openai_api_key)
        .send()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::Upstream(format!(
            "OpenAI batch retrieve failed: {}",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))
}

async fn update_openai_batch_status(
    db: &PgPool,
    batch_row_id: i64,
    batch: &OpenAiBatchObject,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        UPDATE embedding_batches
        SET status = $2,
            openai_output_file_id = $3,
            openai_error_file_id = $4,
            completed_at = CASE
              WHEN $2 IN ('completed', 'failed', 'expired', 'cancelled') THEN coalesce(completed_at, now())
              ELSE completed_at
            END,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(batch_row_id)
    .bind(&batch.status)
    .bind(&batch.output_file_id)
    .bind(&batch.error_file_id)
    .execute(db)
    .await?;
    Ok(())
}

async fn apply_openai_batch_results(
    state: &AppState,
    batch_row_id: i64,
    batch: &OpenAiBatchObject,
) -> Result<(), ApiError> {
    let mut outcomes = HashMap::new();
    let output_file_id = if let Some(file_id) = batch.output_file_id.clone() {
        Some(file_id)
    } else {
        batch_row_for_output(batch_row_id, &state.db).await?
    };
    if let Some(output_file_id) = output_file_id.as_deref() {
        for line in fetch_openai_batch_file_lines(state, output_file_id).await? {
            outcomes.insert(
                parse_batch_chunk_id(&line.custom_id)?,
                batch_line_outcome(line)?,
            );
        }
    }
    let error_file_id = if let Some(file_id) = batch.error_file_id.clone() {
        Some(file_id)
    } else {
        batch_row_for_error(batch_row_id, &state.db).await?
    };
    if let Some(error_file_id) = error_file_id.as_deref() {
        for line in fetch_openai_batch_file_lines(state, error_file_id).await? {
            outcomes.insert(
                parse_batch_chunk_id(&line.custom_id)?,
                batch_line_outcome(line)?,
            );
        }
    }

    let jobs = sqlx::query(
        "SELECT chunk_id FROM embedding_jobs WHERE batch_id = $1 AND status = 'submitted'",
    )
    .bind(batch_row_id)
    .fetch_all(&state.db)
    .await?;

    for row in jobs {
        let chunk_id: i64 = row.get("chunk_id");
        match outcomes.remove(&chunk_id) {
            Some(BatchEmbeddingOutcome::Success(embedding)) => {
                sqlx::query(
                    "UPDATE chunks SET embedding = $2::vector, embedding_model = $3 WHERE id = $1",
                )
                .bind(chunk_id)
                .bind(vector_literal(&embedding))
                .bind(&state.embedding_model)
                .execute(&state.db)
                .await?;
                sqlx::query(
                    "UPDATE embedding_jobs SET status = 'done', completed_at = now(), updated_at = now(), last_error = NULL WHERE chunk_id = $1",
                )
                .bind(chunk_id)
                .execute(&state.db)
                .await?;
            }
            Some(BatchEmbeddingOutcome::Failure(message)) => {
                sqlx::query(
                    "UPDATE embedding_jobs SET status = 'failed', batch_id = NULL, batch_custom_id = NULL, last_error = $2, updated_at = now() WHERE chunk_id = $1",
                )
                .bind(chunk_id)
                .bind(message)
                .execute(&state.db)
                .await?;
            }
            None => {
                sqlx::query(
                    "UPDATE embedding_jobs SET status = 'failed', batch_id = NULL, batch_custom_id = NULL, last_error = 'OpenAI batch completed without a result row', updated_at = now() WHERE chunk_id = $1",
                )
                .bind(chunk_id)
                .execute(&state.db)
                .await?;
            }
        }
    }

    sqlx::query(
        "UPDATE embedding_batches SET results_applied_at = now(), updated_at = now() WHERE id = $1",
    )
    .bind(batch_row_id)
    .execute(&state.db)
    .await?;
    Ok(())
}

async fn batch_row_for_output(db_batch_id: i64, db: &PgPool) -> Result<Option<String>, ApiError> {
    let row = sqlx::query("SELECT openai_output_file_id FROM embedding_batches WHERE id = $1")
        .bind(db_batch_id)
        .fetch_one(db)
        .await?;
    Ok(row.get("openai_output_file_id"))
}

async fn batch_row_for_error(db_batch_id: i64, db: &PgPool) -> Result<Option<String>, ApiError> {
    let row = sqlx::query("SELECT openai_error_file_id FROM embedding_batches WHERE id = $1")
        .bind(db_batch_id)
        .fetch_one(db)
        .await?;
    Ok(row.get("openai_error_file_id"))
}

async fn fetch_openai_batch_file_lines(
    state: &AppState,
    file_id: &str,
) -> Result<Vec<OpenAiBatchResultLine>, ApiError> {
    let response = state
        .http
        .get(format!("https://api.openai.com/v1/files/{file_id}/content"))
        .bearer_auth(&state.openai_api_key)
        .send()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::Upstream(format!(
            "OpenAI file download failed: {}",
            response.status()
        )));
    }
    let body = response
        .text()
        .await
        .map_err(|err| ApiError::Upstream(err.to_string()))?;
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|err| ApiError::Upstream(err.to_string())))
        .collect()
}

async fn fail_openai_batch_jobs(
    db: &PgPool,
    batch_row_id: i64,
    message: &str,
) -> Result<(), ApiError> {
    sqlx::query(
        "UPDATE embedding_jobs SET status = 'failed', batch_id = NULL, batch_custom_id = NULL, last_error = $2, updated_at = now() WHERE batch_id = $1 AND status IN ('submitted', 'running')",
    )
    .bind(batch_row_id)
    .bind(message)
    .execute(db)
    .await?;
    sqlx::query(
        "UPDATE embedding_batches SET results_applied_at = now(), updated_at = now(), last_error = $2 WHERE id = $1",
    )
    .bind(batch_row_id)
    .bind(message)
    .execute(db)
    .await?;
    Ok(())
}

async fn prioritize_thread_embeddings(db: &PgPool, thread_id: &str) -> Result<u64, ApiError> {
    let result = sqlx::query(
        r#"
        UPDATE embedding_jobs ej
        SET status = CASE WHEN status = 'failed' THEN 'pending' ELSE status END,
            created_at = now() + interval '1 day',
            updated_at = now()
        FROM chunks c
        WHERE c.id = ej.chunk_id
          AND c.thread_id = $1
          AND ej.status IN ('pending', 'failed')
        "#,
    )
    .bind(thread_id)
    .execute(db)
    .await?;
    Ok(result.rows_affected())
}

async fn prioritize_result_embeddings(
    db: &PgPool,
    results: &[SearchResult],
) -> Result<(), ApiError> {
    let mut seen = HashSet::new();
    for result in results {
        if seen.insert(result.thread_id.clone()) {
            prioritize_thread_embeddings(db, &result.thread_id).await?;
        }
    }
    Ok(())
}

fn vector_literal(values: &[f32]) -> String {
    let inner = values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{inner}]")
}

fn batch_custom_id(chunk_id: i64, attempts: i32) -> String {
    format!("chunk:{chunk_id}:attempt:{attempts}")
}

fn parse_batch_chunk_id(custom_id: &str) -> Result<i64, ApiError> {
    let mut parts = custom_id.split(':');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("chunk"), Some(chunk_id), Some("attempt"), Some(_attempt)) => chunk_id
            .parse()
            .map_err(|_| ApiError::Upstream(format!("invalid batch custom_id: {custom_id}"))),
        _ => Err(ApiError::Upstream(format!(
            "invalid batch custom_id: {custom_id}"
        ))),
    }
}

fn batch_line_outcome(line: OpenAiBatchResultLine) -> Result<BatchEmbeddingOutcome, ApiError> {
    if let Some(response) = line.response {
        if response.status_code == 200 {
            let embedding = response
                .body
                .pointer("/data/0/embedding")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    ApiError::Upstream(format!(
                        "missing embedding in batch response for {}",
                        line.custom_id
                    ))
                })?
                .iter()
                .filter_map(|value| value.as_f64().map(|number| number as f32))
                .collect::<Vec<_>>();
            return Ok(BatchEmbeddingOutcome::Success(embedding));
        }
        return Ok(BatchEmbeddingOutcome::Failure(format!(
            "OpenAI batch request failed with {}: {}",
            response.status_code, response.body
        )));
    }
    Ok(BatchEmbeddingOutcome::Failure(
        line.error
            .map(|value| value.to_string())
            .unwrap_or_else(|| "OpenAI batch request failed without an error body".to_string()),
    ))
}

fn search_scope_name(scope: SearchScope) -> &'static str {
    match scope {
        SearchScope::All => "all",
        SearchScope::Decisions => "decisions",
        SearchScope::Problems => "problems",
        SearchScope::Commands => "commands",
        SearchScope::Today => "today",
        SearchScope::Recent => "recent",
    }
}

fn collapse_results(results: Vec<SearchResult>, limit: i64) -> Vec<SearchResult> {
    let mut seen = HashSet::new();
    let mut collapsed = Vec::new();
    for result in results {
        let normalized = result
            .text
            .split_whitespace()
            .take(80)
            .collect::<Vec<_>>()
            .join(" ");
        let key = (
            result.thread_id.clone(),
            result.chunk_kind.clone(),
            normalized,
        );
        if seen.insert(key) {
            collapsed.push(result);
        }
        if collapsed.len() >= limit as usize {
            break;
        }
    }
    collapsed
}

fn group_results(results: &[SearchResult]) -> Vec<SearchGroup> {
    let mut groups: Vec<SearchGroup> = Vec::new();
    let mut indexes: HashMap<String, usize> = HashMap::new();
    for result in results {
        if let Some(index) = indexes.get(&result.thread_id).copied() {
            groups[index].best_score = groups[index].best_score.max(result.score);
            groups[index].results.push(result.clone());
        } else {
            indexes.insert(result.thread_id.clone(), groups.len());
            groups.push(SearchGroup {
                thread_id: result.thread_id.clone(),
                best_score: result.score,
                results: vec![result.clone()],
            });
        }
    }
    groups.sort_by(|a, b| {
        b.best_score
            .partial_cmp(&a.best_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    groups
}

async fn raw_rows(
    db: &PgPool,
    thread_id: &str,
    include_private_model_traces: bool,
) -> Result<Vec<sqlx::postgres::PgRow>, ApiError> {
    let private_filter = if include_private_model_traces {
        "TRUE"
    } else {
        "NOT (type = 'event_msg' AND payload->>'type' = 'agent_reasoning_raw_content')"
    };
    let rows = sqlx::query(&format!(
        "SELECT raw FROM rollout_lines WHERE thread_id = $1 AND {private_filter} ORDER BY line_number ASC"
    ))
    .bind(thread_id)
    .fetch_all(db)
    .await?;
    Ok(rows)
}

fn require_token(headers: &HeaderMap, expected: &str) -> Result<(), ApiError> {
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return Err(ApiError::Unauthorized);
    };
    let Ok(value) = value.to_str() else {
        return Err(ApiError::Unauthorized);
    };
    if value.strip_prefix("Bearer ") == Some(expected) {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
}

fn require_read_access(headers: &HeaderMap, expected: &str) -> Result<(), ApiError> {
    if has_read_access(headers, expected) {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
}

fn has_read_access(headers: &HeaderMap, expected: &str) -> bool {
    authorization_matches(headers, expected)
        || cookie_value(headers, READ_COOKIE_NAME) == Some(expected)
}

fn authorization_matches(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        == Some(expected)
}

fn cookie_value<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie_header.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        if name == key {
            Some(value)
        } else {
            None
        }
    })
}

fn auth_cookie(token: &str) -> String {
    format!("{READ_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000")
}

fn postgres_jsonb(value: &Value) -> Value {
    let mut value = value.clone();
    sanitize_json_for_postgres(&mut value);
    value
}

fn postgres_jsonb_option(value: &Option<Value>) -> Option<Value> {
    value.as_ref().map(postgres_jsonb)
}

fn postgres_text(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

fn sanitize_json_for_postgres(value: &mut Value) {
    match value {
        Value::String(text) => {
            if text.contains('\0') {
                *text = postgres_text(text);
            }
        }
        Value::Array(items) => {
            for item in items {
                sanitize_json_for_postgres(item);
            }
        }
        Value::Object(object) => {
            for item in object.values_mut() {
                sanitize_json_for_postgres(item);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn file_kind_from_db(kind: &str) -> FileKind {
    match kind {
        "archived_rollout" => FileKind::ArchivedRollout,
        "session_index" => FileKind::SessionIndex,
        _ => FileKind::ActiveRollout,
    }
}

fn thread_summary_from_row(row: sqlx::postgres::PgRow) -> ThreadSummary {
    ThreadSummary {
        thread_id: row.get("thread_id"),
        name: row.get("name"),
        preview: row.get("preview"),
        cwd: row.get("cwd"),
        source: row.get("source"),
        model_provider: row.get("model_provider"),
        model: row.get("model"),
        git_branch: row.get("git_branch"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        archived_at: row.get("archived_at"),
    }
}

fn search_result_from_row(row: sqlx::postgres::PgRow) -> SearchResult {
    let thread_id: String = row.get("thread_id");
    let start_line: i64 = row.get("start_line");
    let end_line: i64 = row.get("end_line");
    SearchResult {
        chunk_id: row.get("id"),
        thread_id: thread_id.clone(),
        turn_id: row.get("turn_id"),
        chunk_kind: row.get("chunk_kind"),
        role: row.get("role"),
        text: row.get("text"),
        score: row.get("score"),
        citation: Citation {
            thread_id,
            start_line,
            end_line,
        },
    }
}

fn thread_to_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "thread_id": row.get::<String, _>("thread_id"),
        "name": row.get::<Option<String>, _>("name"),
        "cwd": row.get::<Option<String>, _>("cwd"),
        "source": row.get::<Option<String>, _>("source"),
        "thread_source": row.get::<Option<Value>, _>("thread_source"),
        "model_provider": row.get::<Option<String>, _>("model_provider"),
        "model": row.get::<Option<String>, _>("model"),
        "cli_version": row.get::<Option<String>, _>("cli_version"),
        "git_branch": row.get::<Option<String>, _>("git_branch"),
        "git_sha": row.get::<Option<String>, _>("git_sha"),
        "git_origin_url": row.get::<Option<String>, _>("git_origin_url"),
        "forked_from_id": row.get::<Option<String>, _>("forked_from_id"),
        "created_at": row.get::<Option<DateTime<Utc>>, _>("created_at"),
        "updated_at": row.get::<Option<DateTime<Utc>>, _>("updated_at"),
        "archived_at": row.get::<Option<DateTime<Utc>>, _>("archived_at"),
    })
}

fn chunk_to_json_ref(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "chunk_id": row.get::<i64, _>("id"),
        "turn_id": row.get::<Option<String>, _>("turn_id"),
        "chunk_kind": row.get::<String, _>("chunk_kind"),
        "role": row.get::<Option<String>, _>("role"),
        "text": row.get::<String, _>("text"),
        "start_line": row.get::<i64, _>("start_line"),
        "end_line": row.get::<i64, _>("end_line"),
        "metadata": row.get::<Value, _>("metadata"),
    })
}

fn normalized_turns(rows: &[sqlx::postgres::PgRow]) -> Vec<Value> {
    let mut turns: Vec<Value> = Vec::new();
    let mut indexes: HashMap<String, usize> = HashMap::new();
    for row in rows {
        let turn_id = row
            .get::<Option<String>, _>("turn_id")
            .unwrap_or_else(|| "unassigned".to_string());
        let chunk = chunk_to_json_ref(row);
        if let Some(index) = indexes.get(&turn_id).copied() {
            turns[index]["chunks"].as_array_mut().unwrap().push(chunk);
        } else {
            indexes.insert(turn_id.clone(), turns.len());
            turns.push(json!({
                "turn_id": turn_id,
                "start_line": row.get::<i64, _>("start_line"),
                "end_line": row.get::<i64, _>("end_line"),
                "chunks": [chunk],
            }));
        }
    }
    for turn in &mut turns {
        if let Some(chunks) = turn["chunks"].as_array() {
            let end_line = chunks
                .iter()
                .filter_map(|chunk| chunk["end_line"].as_i64())
                .max();
            if let Some(end_line) = end_line {
                turn["end_line"] = json!(end_line);
            }
        }
    }
    turns
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_literal_uses_pgvector_format() {
        assert_eq!(vector_literal(&[0.1, -0.2]), "[0.1,-0.2]");
    }

    #[test]
    fn config_defaults_match_plan() {
        assert_eq!(
            std::env::var("OPENAI_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".to_string()),
            "text-embedding-3-small"
        );
    }

    #[test]
    fn embedding_backend_defaults_to_batch() {
        assert_eq!(
            std::env::var("OPENAI_EMBEDDING_BACKEND").unwrap_or_else(|_| "batch".to_string()),
            "batch"
        );
    }

    #[test]
    fn batch_custom_id_round_trips_chunk_id() {
        let custom_id = batch_custom_id(42, 3);
        assert_eq!(custom_id, "chunk:42:attempt:3");
        assert_eq!(parse_batch_chunk_id(&custom_id).unwrap(), 42);
    }

    #[test]
    fn batch_line_outcome_extracts_embedding() {
        let line: OpenAiBatchResultLine = serde_json::from_value(json!({
            "custom_id": "chunk:7:attempt:1",
            "response": {
                "status_code": 200,
                "body": {
                    "data": [{ "embedding": [0.1, -0.2] }]
                }
            },
            "error": null
        }))
        .unwrap();

        match batch_line_outcome(line).unwrap() {
            BatchEmbeddingOutcome::Success(embedding) => {
                assert_eq!(embedding, vec![0.1_f32, -0.2_f32]);
            }
            BatchEmbeddingOutcome::Failure(message) => panic!("unexpected failure: {message}"),
        }
    }

    #[test]
    fn postgres_jsonb_escapes_nul_characters() {
        let value = json!({
            "payload": {
                "text": "before\u{0}after",
                "nested": ["ok", "\u{0}"]
            }
        });
        let sanitized = postgres_jsonb(&value);

        assert_eq!(sanitized["payload"]["text"], "before\\u0000after");
        assert_eq!(sanitized["payload"]["nested"][1], "\\u0000");
    }

    #[test]
    fn html_escape_handles_important_characters() {
        assert_eq!(
            html_escape("<tag attr='x'>&\""),
            "&lt;tag attr=&#39;x&#39;&gt;&amp;&quot;"
        );
    }

    #[test]
    fn cookie_value_extracts_named_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("theme=dark; archive_read_token=secret-token; other=1"),
        );

        assert_eq!(cookie_value(&headers, READ_COOKIE_NAME), Some("secret-token"));
        assert_eq!(cookie_value(&headers, "missing"), None);
    }
}
