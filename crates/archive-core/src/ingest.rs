use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    ActiveRollout,
    ArchivedRollout,
    SessionIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineIdentity {
    pub machine_id: String,
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentFileMetadata {
    pub relative_path: String,
    pub kind: FileKind,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<DateTime<Utc>>,
    pub file_hash: String,
    pub prefix_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentLine {
    pub line_number: i64,
    pub byte_start: u64,
    pub byte_end: u64,
    pub raw: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentBatch {
    pub machine: MachineIdentity,
    pub file: AgentFileMetadata,
    pub lines: Vec<AgentLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestResponse {
    pub accepted_lines: usize,
    pub indexed_chunks: usize,
    pub file_version: i32,
    #[serde(default)]
    pub quarantined_lines: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileCursor {
    pub relative_path: String,
    pub kind: FileKind,
    pub file_version: i32,
    pub import_schema_version: i32,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<DateTime<Utc>>,
    pub file_hash: String,
    pub prefix_hash: String,
    pub import_byte_cursor: u64,
    pub import_line_cursor: i64,
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineSyncStatus {
    pub machine_id: String,
    pub hostname: String,
    pub installation_id: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub files: SyncFileCounts,
    pub content: SyncContentCounts,
    pub embeddings: Vec<StatusCount>,
    pub ingest_errors: Vec<StatusCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncFileCounts {
    pub active_rollout: i64,
    pub archived_rollout: i64,
    pub session_index: i64,
    pub total: i64,
    pub today: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncContentCounts {
    pub threads: i64,
    pub raw_lines: i64,
    pub chunks: i64,
    pub today_raw_lines: i64,
    pub today_chunks: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}
