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
}
