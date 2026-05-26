use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use archive_core::{
    sha256_hex, AgentBatch, AgentFileMetadata, AgentLine, FileKind, IngestResponse, MachineIdentity,
};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use reqwest::Client;
use tracing::{info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

const PREFIX_HASH_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_LINES_PER_BATCH: usize = 5_000;

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
}

#[derive(Debug, Parser, Clone)]
struct AgentOptions {
    #[arg(long, env = "ARCHIVE_SERVER_URL")]
    server: String,
    #[arg(long, env = "ARCHIVE_INGEST_TOKEN")]
    token: String,
    #[arg(long, env = "CODEX_HOME", default_value = "~/.codex")]
    codex_home: PathBuf,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Scan(options) => scan_once(options).await,
        Command::Watch(options) => watch(options).await,
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
    let codex_home = expand_tilde(options.codex_home);
    let machine = machine_identity(&codex_home)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("build HTTP client")?;
    let endpoint = format!("{}/v1/ingest/batch", options.server.trim_end_matches('/'));

    let mut files = discover_files(&codex_home)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    for discovered in files {
        let prepared = prepare_file(&codex_home, &discovered)?;
        if prepared.lines.is_empty() {
            continue;
        }
        for chunk in prepared.lines.chunks(options.max_lines_per_batch) {
            let batch = AgentBatch {
                machine: machine.clone(),
                file: prepared.metadata.clone(),
                lines: chunk.to_vec(),
            };
            let response = client
                .post(&endpoint)
                .bearer_auth(&options.token)
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
            info!(
                file = %batch.file.relative_path,
                accepted_lines = body.accepted_lines,
                indexed_chunks = body.indexed_chunks,
                file_version = body.file_version,
                "uploaded archive batch"
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct DiscoveredFile {
    path: PathBuf,
    kind: FileKind,
}

#[derive(Debug, Clone)]
struct PreparedFile {
    metadata: AgentFileMetadata,
    lines: Vec<AgentLine>,
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
    let complete = &bytes[..complete_len];
    let relative_path = discovered
        .path
        .strip_prefix(codex_home)
        .unwrap_or(&discovered.path)
        .to_string_lossy()
        .to_string();
    let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
    let file_hash = sha256_hex(&bytes);
    let prefix_hash = sha256_hex(&bytes[..bytes.len().min(PREFIX_HASH_BYTES)]);
    let lines = complete_lines(complete)?;

    Ok(PreparedFile {
        metadata: AgentFileMetadata {
            relative_path,
            kind: discovered.kind.clone(),
            size_bytes: metadata.len(),
            modified_at,
            file_hash,
            prefix_hash,
        },
        lines,
    })
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
}
