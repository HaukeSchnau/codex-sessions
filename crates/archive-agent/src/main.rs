use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use archive_core::{
    sha256_hex, AgentBatch, AgentFileMetadata, AgentLine, FileCursor, FileKind, IngestResponse,
    MachineIdentity,
};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use reqwest::Client;
use std::collections::HashMap;
use tracing::{info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

const PREFIX_HASH_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_LINES_PER_BATCH: usize = 5_000;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 600;
const DEFAULT_PRUNE_MIN_AGE_DAYS: u64 = 30;
const CURRENT_IMPORT_SCHEMA_VERSION: i32 = 3;

#[derive(Debug, Parser)]
#[command(name = "archive-agent")]
#[command(about = "Push local Codex rollout JSONL into codex-session archive-server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Scan(AgentOptions),
    Watch(WatchOptions),
    Prune(PruneOptions),
}

#[derive(Debug, Parser, Clone)]
struct CommonOptions {
    #[arg(long, env = "ARCHIVE_SERVER_URL")]
    server: String,
    #[arg(long, env = "ARCHIVE_INGEST_TOKEN", conflicts_with = "token_file")]
    token: Option<String>,
    #[arg(long, env = "ARCHIVE_INGEST_TOKEN_FILE", conflicts_with = "token")]
    token_file: Option<PathBuf>,
    #[arg(long, env = "CODEX_HOME", default_value = "~/.codex")]
    codex_home: PathBuf,
    #[arg(long, default_value_t = DEFAULT_REQUEST_TIMEOUT_SECONDS)]
    request_timeout_seconds: u64,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    quiet: bool,
}

#[derive(Debug, Parser, Clone)]
struct AgentOptions {
    #[command(flatten)]
    common: CommonOptions,
    #[arg(long, default_value_t = DEFAULT_MAX_LINES_PER_BATCH)]
    max_lines_per_batch: usize,
}

#[derive(Debug, Parser, Clone)]
struct WatchOptions {
    #[command(flatten)]
    agent: AgentOptions,
    #[arg(long, default_value_t = 30)]
    interval_seconds: u64,
}

#[derive(Debug, Parser, Clone)]
struct PruneOptions {
    #[command(flatten)]
    common: CommonOptions,
    #[arg(long, default_value_t = DEFAULT_PRUNE_MIN_AGE_DAYS)]
    min_age_days: u64,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    skip_archived_sessions: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Scan(options) => scan_once(options).await,
        Command::Watch(options) => watch(options).await,
        Command::Prune(options) => prune_once(options).await,
    }
}

async fn watch(options: WatchOptions) -> anyhow::Result<()> {
    loop {
        if let Err(err) = scan_once(options.agent.clone()).await {
            warn!(error = %err, "archive scan failed");
        }
        tokio::time::sleep(Duration::from_secs(options.interval_seconds)).await;
    }
}

async fn scan_once(options: AgentOptions) -> anyhow::Result<()> {
    if options.max_lines_per_batch == 0 {
        bail!("--max-lines-per-batch must be greater than zero");
    }
    if options.common.request_timeout_seconds == 0 {
        bail!("--request-timeout-seconds must be greater than zero");
    }
    let token = ingest_token(&options.common)?;
    let codex_home = expand_tilde(options.common.codex_home.clone());
    let machine = machine_identity(&codex_home)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(options.common.request_timeout_seconds))
        .build()
        .context("build HTTP client")?;
    let endpoint = format!(
        "{}/v1/ingest/batch",
        options.common.server.trim_end_matches('/')
    );
    let cursors =
        fetch_cursors(&client, &options.common.server, &token, &machine.machine_id).await?;

    let mut files = discover_files(&codex_home)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let total_files = files.len();
    let started = Instant::now();
    let mut stats = ScanStats {
        discovered_files: total_files,
        ..Default::default()
    };
    for (index, discovered) in files.into_iter().enumerate() {
        let relative_path = relative_path(&codex_home, &discovered.path);
        if let Some(cursor) = cursors.get(&relative_path) {
            if file_metadata_matches_cursor(&discovered.path, cursor)? {
                stats.skipped_files += 1;
                emit_progress(
                    &options.common,
                    "skipped",
                    index + 1,
                    total_files,
                    &relative_path,
                    &stats,
                );
                continue;
            }
        }
        let prepared = prepare_file(&codex_home, &discovered)?;
        let cursor = cursors.get(&prepared.metadata.relative_path);
        if prepared.complete_len == 0 {
            stats.empty_files += 1;
            continue;
        }
        let upload_lines = lines_to_upload(&prepared, cursor)?;
        if upload_lines.is_empty() {
            stats.skipped_files += 1;
            emit_progress(
                &options.common,
                "skipped",
                index + 1,
                total_files,
                &prepared.metadata.relative_path,
                &stats,
            );
            continue;
        }
        stats.uploaded_files += 1;
        stats.uploaded_lines += upload_lines.len();
        stats.uploaded_bytes += upload_lines
            .iter()
            .map(|line| line.byte_end.saturating_sub(line.byte_start))
            .sum::<u64>();
        for chunk in upload_lines.chunks(options.max_lines_per_batch) {
            let batch = AgentBatch {
                machine: machine.clone(),
                file: prepared.metadata.clone(),
                lines: chunk.to_vec(),
            };
            let response = client
                .post(&endpoint)
                .bearer_auth(&token)
                .json(&batch)
                .send()
                .await
                .with_context(|| format!("POST {endpoint}"))?;
            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                bail!(
                    "server rejected {} with {status}: {text}",
                    batch.file.relative_path
                );
            }
            let body: IngestResponse = response.json().await.context("decode ingest response")?;
            stats.accepted_lines += body.accepted_lines;
            stats.indexed_chunks += body.indexed_chunks;
            stats.quarantined_lines += body.quarantined_lines;
            info!(
                file = %batch.file.relative_path,
                accepted_lines = body.accepted_lines,
                indexed_chunks = body.indexed_chunks,
                quarantined_lines = body.quarantined_lines,
                file_version = body.file_version,
                "uploaded archive batch"
            );
        }
        emit_progress(
            &options.common,
            "uploaded",
            index + 1,
            total_files,
            &prepared.metadata.relative_path,
            &stats,
        );
    }
    stats.elapsed_ms = started.elapsed().as_millis() as u64;
    emit_summary(&options.common, &stats);
    Ok(())
}

async fn prune_once(options: PruneOptions) -> anyhow::Result<()> {
    if options.common.request_timeout_seconds == 0 {
        bail!("--request-timeout-seconds must be greater than zero");
    }
    if options.min_age_days == 0 {
        bail!("--min-age-days must be greater than zero");
    }
    let token = ingest_token(&options.common)?;
    let codex_home = expand_tilde(options.common.codex_home.clone());
    let machine = machine_identity(&codex_home)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(options.common.request_timeout_seconds))
        .build()
        .context("build HTTP client")?;
    let cursors =
        fetch_cursors(&client, &options.common.server, &token, &machine.machine_id).await?;

    let mut files = discover_files(&codex_home)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let total_files = files.len();
    let started = Instant::now();
    let mut stats = PruneStats {
        discovered_files: total_files,
        ..Default::default()
    };
    let now = Utc::now();

    for (index, discovered) in files.into_iter().enumerate() {
        if discovered.kind == FileKind::SessionIndex {
            stats.non_rollout_files += 1;
            continue;
        }
        if options.skip_archived_sessions && discovered.kind == FileKind::ArchivedRollout {
            stats.skipped_archived_files += 1;
            continue;
        }

        let prepared = prepare_file(&codex_home, &discovered)?;
        if prepared.complete_len == 0 {
            stats.empty_files += 1;
            continue;
        }

        let Some(modified_at) = prepared.metadata.modified_at else {
            stats.unconfirmed_files += 1;
            continue;
        };
        if !is_past_min_age(modified_at, now, options.min_age_days) {
            stats.recent_files += 1;
            continue;
        }

        let cursor = cursors.get(&prepared.metadata.relative_path);
        if !file_fully_archived(&prepared, cursor) {
            stats.unconfirmed_files += 1;
            continue;
        }

        stats.eligible_files += 1;
        stats.eligible_bytes += prepared.metadata.size_bytes;
        if options.dry_run {
            stats.dry_run_files += 1;
            emit_prune_progress(
                &options.common,
                "would_delete",
                index + 1,
                total_files,
                &prepared.metadata.relative_path,
                &stats,
            );
            continue;
        }

        fs::remove_file(&prepared.path)
            .with_context(|| format!("delete {}", prepared.path.display()))?;
        prune_empty_rollout_dirs(&codex_home, &prepared)?;
        stats.deleted_files += 1;
        stats.deleted_bytes += prepared.metadata.size_bytes;
        emit_prune_progress(
            &options.common,
            "deleted",
            index + 1,
            total_files,
            &prepared.metadata.relative_path,
            &stats,
        );
    }

    stats.elapsed_ms = started.elapsed().as_millis() as u64;
    emit_prune_summary(&options.common, &stats);
    Ok(())
}

fn ingest_token(options: &CommonOptions) -> anyhow::Result<String> {
    match (&options.token, &options.token_file) {
        (Some(token), None) => Ok(token.clone()),
        (None, Some(path)) => fs::read_to_string(path)
            .with_context(|| format!("read ingest token from {}", path.display()))
            .map(|token| token.trim().to_string())
            .and_then(|token| {
                if token.is_empty() {
                    bail!("ingest token file is empty")
                }
                Ok(token)
            }),
        (None, None) => bail!("provide --token or --token-file"),
        (Some(_), Some(_)) => bail!("provide only one of --token or --token-file"),
    }
}

#[derive(Debug, Default)]
struct ScanStats {
    discovered_files: usize,
    uploaded_files: usize,
    skipped_files: usize,
    empty_files: usize,
    uploaded_lines: usize,
    uploaded_bytes: u64,
    accepted_lines: usize,
    indexed_chunks: usize,
    quarantined_lines: usize,
    elapsed_ms: u64,
}

#[derive(Debug, Default)]
struct PruneStats {
    discovered_files: usize,
    eligible_files: usize,
    eligible_bytes: u64,
    dry_run_files: usize,
    deleted_files: usize,
    deleted_bytes: u64,
    recent_files: usize,
    unconfirmed_files: usize,
    empty_files: usize,
    non_rollout_files: usize,
    skipped_archived_files: usize,
    elapsed_ms: u64,
}

#[derive(Debug, Clone)]
struct DiscoveredFile {
    path: PathBuf,
    kind: FileKind,
}

#[derive(Debug, Clone)]
struct PreparedFile {
    path: PathBuf,
    metadata: AgentFileMetadata,
    complete_len: usize,
    bytes: Vec<u8>,
}

fn discover_files(codex_home: &Path) -> anyhow::Result<Vec<DiscoveredFile>> {
    let mut files = Vec::new();
    let sessions = codex_home.join("sessions");
    if sessions.exists() {
        collect_jsonl(&sessions, FileKind::ActiveRollout, &mut files)?;
    }
    let archived = codex_home.join("archived_sessions");
    if archived.exists() {
        collect_jsonl(&archived, FileKind::ArchivedRollout, &mut files)?;
    }
    let index = codex_home.join("session_index.jsonl");
    if index.exists() {
        files.push(DiscoveredFile {
            path: index,
            kind: FileKind::SessionIndex,
        });
    }
    Ok(files)
}

fn collect_jsonl(
    root: &Path,
    kind: FileKind,
    files: &mut Vec<DiscoveredFile>,
) -> anyhow::Result<()> {
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            files.push(DiscoveredFile {
                path: entry.path().to_path_buf(),
                kind: kind.clone(),
            });
        }
    }
    Ok(())
}

fn prepare_file(codex_home: &Path, discovered: &DiscoveredFile) -> anyhow::Result<PreparedFile> {
    let bytes = fs::read(&discovered.path)
        .with_context(|| format!("read {}", discovered.path.display()))?;
    let metadata = fs::metadata(&discovered.path)
        .with_context(|| format!("metadata {}", discovered.path.display()))?;
    let complete_len = complete_prefix_len(&bytes);
    let relative_path = relative_path(codex_home, &discovered.path);
    let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
    let file_hash = sha256_hex(&bytes);
    let prefix_hash = sha256_hex(&bytes[..bytes.len().min(PREFIX_HASH_BYTES)]);

    Ok(PreparedFile {
        path: discovered.path.clone(),
        metadata: AgentFileMetadata {
            relative_path,
            kind: discovered.kind.clone(),
            size_bytes: metadata.len(),
            modified_at,
            file_hash,
            prefix_hash,
        },
        complete_len,
        bytes,
    })
}

fn relative_path(codex_home: &Path, path: &Path) -> String {
    path.strip_prefix(codex_home)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

fn file_metadata_matches_cursor(path: &Path, cursor: &FileCursor) -> anyhow::Result<bool> {
    if cursor.import_schema_version < CURRENT_IMPORT_SCHEMA_VERSION {
        return Ok(false);
    }
    let metadata = fs::metadata(path).with_context(|| format!("metadata {}", path.display()))?;
    if metadata.len() != cursor.size_bytes {
        return Ok(false);
    }
    let Some(cursor_modified_at) = cursor.modified_at else {
        return Ok(false);
    };
    let Some(modified_at) = metadata.modified().ok().map(DateTime::<Utc>::from) else {
        return Ok(false);
    };
    Ok(modified_at.timestamp_micros() == cursor_modified_at.timestamp_micros())
}

async fn fetch_cursors(
    client: &Client,
    server: &str,
    token: &str,
    machine_id: &str,
) -> anyhow::Result<HashMap<String, FileCursor>> {
    let endpoint = format!(
        "{}/v1/ingest/cursors?machine_id={}",
        server.trim_end_matches('/'),
        machine_id
    );
    let response = client
        .get(&endpoint)
        .bearer_auth(token)
        .send()
        .await
        .with_context(|| format!("GET {endpoint}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        bail!("server rejected cursor request with {status}: {text}");
    }
    let cursors: Vec<FileCursor> = response.json().await.context("decode cursor response")?;
    Ok(cursors
        .into_iter()
        .map(|cursor| (cursor.relative_path.clone(), cursor))
        .collect())
}

fn lines_to_upload(
    prepared: &PreparedFile,
    cursor: Option<&FileCursor>,
) -> anyhow::Result<Vec<AgentLine>> {
    let Some(cursor) = cursor else {
        return complete_lines(&prepared.bytes[..prepared.complete_len]);
    };
    let append_only_match = cursor_prefix_matches(prepared, cursor)
        && cursor.size_bytes <= prepared.metadata.size_bytes
        && cursor.file_hash != prepared.metadata.file_hash;
    let fully_imported = file_fully_archived(prepared, Some(cursor));
    if fully_imported {
        return Ok(Vec::new());
    }
    let lines = complete_lines(&prepared.bytes[..prepared.complete_len])?;
    if append_only_match {
        return Ok(lines
            .iter()
            .filter(|line| line.byte_end > cursor.import_byte_cursor)
            .cloned()
            .collect());
    }
    Ok(lines)
}

fn emit_progress(
    options: &CommonOptions,
    event: &str,
    current: usize,
    total: usize,
    relative_path: &str,
    stats: &ScanStats,
) {
    if options.quiet {
        return;
    }
    if options.json {
        println!(
            "{}",
            serde_json::json!({
                "event": event,
                "current_file": current,
                "total_files": total,
                "relative_path": relative_path,
                "uploaded_files": stats.uploaded_files,
                "skipped_files": stats.skipped_files,
                "uploaded_lines": stats.uploaded_lines,
                "accepted_lines": stats.accepted_lines,
                "indexed_chunks": stats.indexed_chunks,
                "quarantined_lines": stats.quarantined_lines,
            })
        );
    } else if event == "uploaded" || current == total || current.is_multiple_of(100) {
        eprintln!(
            "[{current}/{total}] {event} {} (uploaded_files={}, skipped={}, lines={}, chunks={}, quarantined={})",
            relative_path,
            stats.uploaded_files,
            stats.skipped_files,
            stats.accepted_lines,
            stats.indexed_chunks,
            stats.quarantined_lines
        );
    }
}

fn emit_summary(options: &CommonOptions, stats: &ScanStats) {
    if options.quiet {
        return;
    }
    if options.json {
        println!(
            "{}",
            serde_json::json!({
                "event": "summary",
                "discovered_files": stats.discovered_files,
                "uploaded_files": stats.uploaded_files,
                "skipped_files": stats.skipped_files,
                "empty_files": stats.empty_files,
                "uploaded_lines": stats.uploaded_lines,
                "uploaded_bytes": stats.uploaded_bytes,
                "accepted_lines": stats.accepted_lines,
                "indexed_chunks": stats.indexed_chunks,
                "quarantined_lines": stats.quarantined_lines,
                "elapsed_ms": stats.elapsed_ms,
            })
        );
    } else {
        eprintln!(
            "scan complete: discovered={}, uploaded={}, skipped={}, empty={}, accepted_lines={}, chunks={}, quarantined={}, elapsed_ms={}",
            stats.discovered_files,
            stats.uploaded_files,
            stats.skipped_files,
            stats.empty_files,
            stats.accepted_lines,
            stats.indexed_chunks,
            stats.quarantined_lines,
            stats.elapsed_ms
        );
    }
}

fn emit_prune_progress(
    options: &CommonOptions,
    event: &str,
    current: usize,
    total: usize,
    relative_path: &str,
    stats: &PruneStats,
) {
    if options.quiet {
        return;
    }
    if options.json {
        println!(
            "{}",
            serde_json::json!({
                "event": event,
                "current_file": current,
                "total_files": total,
                "relative_path": relative_path,
                "eligible_files": stats.eligible_files,
                "dry_run_files": stats.dry_run_files,
                "deleted_files": stats.deleted_files,
                "deleted_bytes": stats.deleted_bytes,
                "recent_files": stats.recent_files,
                "unconfirmed_files": stats.unconfirmed_files,
            })
        );
    } else if event == "deleted"
        || event == "would_delete"
        || current == total
        || current.is_multiple_of(100)
    {
        eprintln!(
            "[{current}/{total}] {event} {} (eligible={}, dry_run={}, deleted={}, deleted_bytes={}, recent={}, unconfirmed={})",
            relative_path,
            stats.eligible_files,
            stats.dry_run_files,
            stats.deleted_files,
            stats.deleted_bytes,
            stats.recent_files,
            stats.unconfirmed_files
        );
    }
}

fn emit_prune_summary(options: &CommonOptions, stats: &PruneStats) {
    if options.quiet {
        return;
    }
    if options.json {
        println!(
            "{}",
            serde_json::json!({
                "event": "summary",
                "discovered_files": stats.discovered_files,
                "eligible_files": stats.eligible_files,
                "eligible_bytes": stats.eligible_bytes,
                "dry_run_files": stats.dry_run_files,
                "deleted_files": stats.deleted_files,
                "deleted_bytes": stats.deleted_bytes,
                "recent_files": stats.recent_files,
                "unconfirmed_files": stats.unconfirmed_files,
                "empty_files": stats.empty_files,
                "non_rollout_files": stats.non_rollout_files,
                "skipped_archived_files": stats.skipped_archived_files,
                "elapsed_ms": stats.elapsed_ms,
            })
        );
    } else {
        eprintln!(
            "prune complete: discovered={}, eligible={}, dry_run={}, deleted={}, deleted_bytes={}, recent={}, unconfirmed={}, empty={}, skipped_archived={}, elapsed_ms={}",
            stats.discovered_files,
            stats.eligible_files,
            stats.dry_run_files,
            stats.deleted_files,
            stats.deleted_bytes,
            stats.recent_files,
            stats.unconfirmed_files,
            stats.empty_files,
            stats.skipped_archived_files,
            stats.elapsed_ms
        );
    }
}

fn cursor_prefix_matches(prepared: &PreparedFile, cursor: &FileCursor) -> bool {
    let prefix_len = (cursor.size_bytes.min(prepared.metadata.size_bytes))
        .min(PREFIX_HASH_BYTES as u64) as usize;
    prepared
        .bytes
        .get(..prefix_len)
        .map(|prefix| sha256_hex(prefix) == cursor.prefix_hash)
        .unwrap_or(false)
}

fn file_fully_archived(prepared: &PreparedFile, cursor: Option<&FileCursor>) -> bool {
    let Some(cursor) = cursor else {
        return false;
    };
    cursor.import_schema_version >= CURRENT_IMPORT_SCHEMA_VERSION
        && cursor.file_hash == prepared.metadata.file_hash
        && cursor.size_bytes == prepared.metadata.size_bytes
        && cursor.import_byte_cursor >= prepared.complete_len as u64
}

fn is_past_min_age(modified_at: DateTime<Utc>, now: DateTime<Utc>, min_age_days: u64) -> bool {
    let Ok(days) = i64::try_from(min_age_days) else {
        return false;
    };
    modified_at <= now - chrono::Duration::days(days)
}

fn prune_empty_rollout_dirs(codex_home: &Path, prepared: &PreparedFile) -> anyhow::Result<()> {
    let Some(root) = rollout_root(codex_home, &prepared.metadata.kind) else {
        return Ok(());
    };
    let mut current = prepared.path.parent();
    while let Some(dir) = current {
        if dir == root {
            break;
        }
        if !dir.starts_with(&root) {
            break;
        }
        match fs::remove_dir(dir) {
            Ok(()) => current = dir.parent(),
            Err(err) if err.kind() == ErrorKind::NotFound => current = dir.parent(),
            Err(err) if err.kind() == ErrorKind::DirectoryNotEmpty => break,
            Err(err) => return Err(err).with_context(|| format!("remove dir {}", dir.display())),
        }
    }
    Ok(())
}

fn rollout_root(codex_home: &Path, kind: &FileKind) -> Option<PathBuf> {
    match kind {
        FileKind::ActiveRollout => Some(codex_home.join("sessions")),
        FileKind::ArchivedRollout => Some(codex_home.join("archived_sessions")),
        FileKind::SessionIndex => None,
    }
}

fn complete_prefix_len(bytes: &[u8]) -> usize {
    if bytes.last() == Some(&b'\n') {
        return bytes.len();
    }
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn complete_lines(bytes: &[u8]) -> anyhow::Result<Vec<AgentLine>> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        let end = index + 1;
        let mut raw = String::from_utf8(bytes[start..index].to_vec())
            .with_context(|| format!("line {} is not UTF-8", lines.len() + 1))?;
        if raw.ends_with('\r') {
            raw.pop();
        }
        if !raw.trim().is_empty() {
            lines.push(AgentLine {
                line_number: (lines.len() + 1) as i64,
                byte_start: start as u64,
                byte_end: end as u64,
                content_hash: sha256_hex(raw.as_bytes()),
                raw,
            });
        }
        start = end;
    }
    Ok(lines)
}

fn machine_identity(codex_home: &Path) -> anyhow::Result<MachineIdentity> {
    fs::create_dir_all(codex_home)?;
    let machine_id_path = codex_home.join(".archive-machine-id");
    let machine_id = match fs::read_to_string(&machine_id_path) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            let id = Uuid::new_v4().to_string();
            fs::write(&machine_id_path, format!("{id}\n"))?;
            id
        }
    };
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let installation_id = fs::read_to_string(codex_home.join("installation_id"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Ok(MachineIdentity {
        machine_id,
        hostname,
        installation_id,
    })
}

fn expand_tilde(path: PathBuf) -> PathBuf {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(text.trim_start_matches("~/"));
        }
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_partial_trailing_jsonl_line() {
        let bytes = br#"{"a":1}
{"b":2}"#;
        let complete = &bytes[..complete_prefix_len(bytes)];
        let lines = complete_lines(complete).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].raw, r#"{"a":1}"#);
    }

    #[test]
    fn keeps_byte_offsets_for_complete_lines() {
        let lines = complete_lines(b"one\ntwo\n").unwrap();
        assert_eq!(lines[0].byte_start, 0);
        assert_eq!(lines[0].byte_end, 4);
        assert_eq!(lines[1].byte_start, 4);
        assert_eq!(lines[1].byte_end, 8);
    }

    #[test]
    fn cursor_skips_fully_imported_file() {
        let prepared = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"one\ntwo\n");
        let cursor = cursor_for(&prepared, prepared.complete_len as u64);

        let upload = lines_to_upload(&prepared, Some(&cursor)).unwrap();

        assert!(upload.is_empty());
    }

    #[test]
    fn cursor_uploads_only_appended_complete_lines() {
        let original = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"one\n");
        let appended = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"one\ntwo\n");
        let cursor = cursor_for(&original, original.complete_len as u64);

        let upload = lines_to_upload(&appended, Some(&cursor)).unwrap();

        assert_eq!(upload.len(), 1);
        assert_eq!(upload[0].raw, "two");
    }

    #[test]
    fn cursor_reuploads_when_prefix_changes() {
        let original = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"one\n");
        let rewritten = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"zero\ntwo\n");
        let cursor = cursor_for(&original, original.complete_len as u64);

        let upload = lines_to_upload(&rewritten, Some(&cursor)).unwrap();

        assert_eq!(upload.len(), 2);
        assert_eq!(upload[0].raw, "zero");
        assert_eq!(upload[1].raw, "two");
    }

    #[test]
    fn fully_archived_requires_exact_hash_and_full_cursor() {
        let prepared = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"one\ntwo\n");
        let mut cursor = cursor_for(&prepared, prepared.complete_len as u64);
        assert!(file_fully_archived(&prepared, Some(&cursor)));

        cursor.file_hash = "different".to_string();
        assert!(!file_fully_archived(&prepared, Some(&cursor)));
    }

    #[test]
    fn stale_import_schema_forces_reupload() {
        let prepared = prepared_with_bytes("sessions/2026/05/27/rollout-a.jsonl", b"one\ntwo\n");
        let mut cursor = cursor_for(&prepared, prepared.complete_len as u64);
        cursor.import_schema_version = CURRENT_IMPORT_SCHEMA_VERSION - 1;

        let upload = lines_to_upload(&prepared, Some(&cursor)).unwrap();

        assert_eq!(upload.len(), 2);
        assert!(!file_fully_archived(&prepared, Some(&cursor)));
    }

    #[test]
    fn min_age_gate_uses_whole_days() {
        let now = DateTime::parse_from_rfc3339("2026-05-27T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let recent = DateTime::parse_from_rfc3339("2026-04-28T12:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let old_enough = DateTime::parse_from_rfc3339("2026-04-27T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert!(!is_past_min_age(recent, now, 30));
        assert!(is_past_min_age(old_enough, now, 30));
    }

    #[test]
    fn prune_removes_empty_rollout_dirs_without_touching_root() {
        let unique = Uuid::new_v4().to_string();
        let codex_home = std::env::temp_dir().join(format!("archive-agent-prune-{unique}"));
        let day_dir = codex_home.join("sessions/2026/05/27");
        fs::create_dir_all(&day_dir).unwrap();
        let file_path = day_dir.join("rollout-a.jsonl");
        fs::write(&file_path, b"one\n").unwrap();

        let prepared = PreparedFile {
            path: file_path.clone(),
            metadata: AgentFileMetadata {
                relative_path: "sessions/2026/05/27/rollout-a.jsonl".to_string(),
                kind: FileKind::ActiveRollout,
                size_bytes: 4,
                modified_at: None,
                file_hash: sha256_hex(b"one\n"),
                prefix_hash: sha256_hex(b"one\n"),
            },
            complete_len: 4,
            bytes: b"one\n".to_vec(),
        };

        fs::remove_file(&file_path).unwrap();
        prune_empty_rollout_dirs(&codex_home, &prepared).unwrap();

        assert!(codex_home.join("sessions").exists());
        assert!(!day_dir.exists());
        fs::remove_dir_all(&codex_home).unwrap();
    }

    fn prepared_with_bytes(relative_path: &str, bytes: &[u8]) -> PreparedFile {
        let complete_len = complete_prefix_len(bytes);
        PreparedFile {
            path: PathBuf::from(relative_path),
            metadata: AgentFileMetadata {
                relative_path: relative_path.to_string(),
                kind: FileKind::ActiveRollout,
                size_bytes: bytes.len() as u64,
                modified_at: None,
                file_hash: sha256_hex(bytes),
                prefix_hash: sha256_hex(&bytes[..bytes.len().min(PREFIX_HASH_BYTES)]),
            },
            complete_len,
            bytes: bytes.to_vec(),
        }
    }

    fn cursor_for(prepared: &PreparedFile, import_byte_cursor: u64) -> FileCursor {
        FileCursor {
            relative_path: prepared.metadata.relative_path.clone(),
            kind: prepared.metadata.kind.clone(),
            file_version: 1,
            import_schema_version: CURRENT_IMPORT_SCHEMA_VERSION,
            size_bytes: prepared.metadata.size_bytes,
            modified_at: prepared.metadata.modified_at,
            file_hash: prepared.metadata.file_hash.clone(),
            prefix_hash: prepared.metadata.prefix_hash.clone(),
            import_byte_cursor,
            import_line_cursor: complete_lines(&prepared.bytes[..prepared.complete_len])
                .unwrap()
                .len() as i64,
            archived: false,
        }
    }
}
