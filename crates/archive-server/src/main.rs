use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use archive_core::{
    chunk_rollout_lines, parse_thread_name_update, AgentBatch, Chunk, FileKind, IngestResponse,
    RolloutLine,
};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

#[derive(Clone)]
struct AppState {
    db: PgPool,
    ingest_token: String,
    read_token: String,
    openai_api_key: String,
    embedding_model: String,
    embedding_dimensions: i32,
    http: Client,
}

#[derive(Debug, Clone)]
struct Config {
    database_url: String,
    ingest_token: String,
    read_token: String,
    openai_api_key: String,
    embedding_model: String,
    embedding_dimensions: i32,
    bind_addr: SocketAddr,
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
struct SearchParams {
    q: String,
    mode: Option<SearchMode>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SearchMode {
    Keyword,
    Semantic,
    Hybrid,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
struct Citation {
    thread_id: String,
    start_line: i64,
    end_line: i64,
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    query: String,
    mode: Option<SearchMode>,
    limit: Option<i64>,
    include_raw: Option<bool>,
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
        embedding_model: config.embedding_model,
        embedding_dimensions: config.embedding_dimensions,
        http: Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build HTTP client")?,
    };

    spawn_embedding_worker(state.clone());

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/ingest/batch", post(ingest_batch))
        .route("/v1/threads", get(list_threads))
        .route("/v1/threads/:thread_id", get(read_thread))
        .route("/v1/threads/:thread_id/raw", get(read_thread_raw))
        .route("/v1/search", get(search))
        .route("/v1/query", post(query))
        .route("/v1/export", get(export_jsonl))
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
            embedding_model: std::env::var("OPENAI_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".to_string()),
            embedding_dimensions: std::env::var("EMBEDDING_DIMENSIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1536),
            bind_addr: std::env::var("BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8787".to_string())
                .parse()
                .context("parse BIND_ADDR")?,
        })
    }
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> Result<&'static str, ApiError> {
    sqlx::query("SELECT 1").execute(&state.db).await?;
    Ok("ok")
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

async fn list_threads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ThreadListParams>,
) -> Result<Json<Vec<ThreadSummary>>, ApiError> {
    require_token(&headers, &state.read_token)?;
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
    .bind(params.cwd)
    .bind(params.source)
    .bind(params.machine)
    .bind(params.model)
    .bind(params.git_branch)
    .bind(params.q)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;

    Ok(Json(
        rows.into_iter().map(thread_summary_from_row).collect(),
    ))
}

async fn read_thread(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(thread_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_token(&headers, &state.read_token)?;
    let thread = sqlx::query("SELECT * FROM threads WHERE thread_id = $1")
        .bind(&thread_id)
        .fetch_optional(&state.db)
        .await?
        .ok_or(ApiError::NotFound)?;
    let chunks = sqlx::query(
        r#"
        SELECT id, turn_id, chunk_kind, role, text, start_line, end_line, metadata
        FROM chunks WHERE thread_id = $1 ORDER BY start_line ASC, id ASC
        "#,
    )
    .bind(&thread_id)
    .fetch_all(&state.db)
    .await?;
    Ok(Json(json!({
        "thread": thread_to_json(&thread),
        "chunks": chunks.into_iter().map(chunk_to_json).collect::<Vec<_>>()
    })))
}

async fn read_thread_raw(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(thread_id): Path<String>,
    Query(params): Query<RawParams>,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.read_token)?;
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
    require_token(&headers, &state.read_token)?;
    if params.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q must not be empty".to_string()));
    }
    let mode = params.mode.unwrap_or(SearchMode::Hybrid);
    let results = run_search(&state, &params.q, mode, params.limit.unwrap_or(20)).await?;
    Ok(Json(results))
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<Value>, ApiError> {
    require_token(&headers, &state.read_token)?;
    if request.query.trim().is_empty() {
        return Err(ApiError::BadRequest("query must not be empty".to_string()));
    }
    let results = run_search(
        &state,
        &request.query,
        request.mode.unwrap_or(SearchMode::Hybrid),
        request.limit.unwrap_or(20),
    )
    .await?;
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
    Ok(Json(json!({ "results": results, "raw": raw })))
}

async fn export_jsonl(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.read_token)?;
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
    })
}

async fn ingest_rollout(
    tx: &mut Transaction<'_, Postgres>,
    batch: &AgentBatch,
) -> Result<IngestResponse, ApiError> {
    let file_id = upsert_file(tx, batch).await?;
    let file_version = current_file_version(tx, file_id).await?;
    let parsed = batch
        .lines
        .iter()
        .filter_map(|line| {
            RolloutLine::parse(&line.raw)
                .ok()
                .map(|rollout| (line, rollout))
        })
        .collect::<Vec<_>>();
    let Some(thread_id) = parsed
        .iter()
        .find_map(|(_, line)| line.session_metadata().map(|meta| meta.thread_id))
    else {
        update_file_cursor(tx, file_id, batch).await?;
        return Ok(IngestResponse {
            accepted_lines: 0,
            indexed_chunks: 0,
            file_version,
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
        .bind(&line.raw)
        .bind(&line.payload)
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
    .bind(&meta.thread_source)
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
        .bind(&chunk.text)
        .bind(chunk.start_line)
        .bind(chunk.end_line)
        .bind(&chunk.metadata)
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

async fn run_search(
    state: &AppState,
    query: &str,
    mode: SearchMode,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    let limit = limit.clamp(1, 100);
    match mode {
        SearchMode::Keyword => keyword_search(&state.db, query, limit).await,
        SearchMode::Semantic => semantic_search(state, query, limit).await,
        SearchMode::Hybrid => hybrid_search(state, query, limit).await,
    }
}

async fn keyword_search(
    db: &PgPool,
    query: &str,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    let rows = sqlx::query(
        r#"
        SELECT id, thread_id, turn_id, chunk_kind, role, text, start_line, end_line,
               ts_rank_cd(search_tsv, plainto_tsquery('simple', $1))::float8 AS score
        FROM chunks
        WHERE search_tsv @@ plainto_tsquery('simple', $1)
        ORDER BY score DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(query)
    .bind(limit)
    .fetch_all(db)
    .await?;
    Ok(rows.into_iter().map(search_result_from_row).collect())
}

async fn semantic_search(
    state: &AppState,
    query: &str,
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
        ORDER BY embedding <=> $1::vector
        LIMIT $2
        "#,
    )
    .bind(vector)
    .bind(limit)
    .fetch_all(&state.db)
    .await?;
    Ok(rows.into_iter().map(search_result_from_row).collect())
}

async fn hybrid_search(
    state: &AppState,
    query: &str,
    limit: i64,
) -> Result<Vec<SearchResult>, ApiError> {
    if state.openai_api_key.is_empty() {
        return keyword_search(&state.db, query, limit).await;
    }
    let embedding = embed_text(state, query).await?;
    let vector = vector_literal(&embedding);
    let rows = sqlx::query(
        r#"
        WITH keyword AS (
          SELECT id, row_number() OVER (ORDER BY ts_rank_cd(search_tsv, plainto_tsquery('simple', $1)) DESC) AS rank
          FROM chunks
          WHERE search_tsv @@ plainto_tsquery('simple', $1)
          LIMIT $3
        ),
        semantic AS (
          SELECT id, row_number() OVER (ORDER BY embedding <=> $2::vector) AS rank
          FROM chunks
          WHERE embedding IS NOT NULL
          LIMIT $3
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
        LIMIT $3
        "#,
    )
    .bind(query)
    .bind(vector)
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
        loop {
            if let Err(err) = run_embedding_once(&state).await {
                error!(error = %err, "embedding worker failed");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn run_embedding_once(state: &AppState) -> Result<(), ApiError> {
    let rows = sqlx::query(
        r#"
        UPDATE embedding_jobs
        SET status = 'running', locked_at = now(), attempts = attempts + 1, updated_at = now()
        WHERE chunk_id IN (
          SELECT chunk_id FROM embedding_jobs
          WHERE status IN ('pending', 'failed') AND attempts < 5
          ORDER BY created_at ASC LIMIT 8
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

fn vector_literal(values: &[f32]) -> String {
    let inner = values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{inner}]")
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

fn chunk_to_json(row: sqlx::postgres::PgRow) -> Value {
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
}
