use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RolloutLineParseError {
    #[error("invalid JSON line: {0}")]
    Json(#[from] serde_json::Error),
    #[error("line is not a JSON object")]
    NotObject,
    #[error("missing or invalid rollout line type")]
    MissingType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RolloutLine {
    pub timestamp: Option<DateTime<Utc>>,
    pub item_type: String,
    pub payload: Value,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionMetadata {
    pub thread_id: String,
    pub forked_from_id: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub cwd: Option<String>,
    pub source: Option<String>,
    pub thread_source: Option<Value>,
    pub agent_nickname: Option<String>,
    pub agent_role: Option<String>,
    pub agent_path: Option<String>,
    pub model_provider: Option<String>,
    pub cli_version: Option<String>,
    pub git_branch: Option<String>,
    pub git_sha: Option<String>,
    pub git_origin_url: Option<String>,
    pub memory_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadNameUpdate {
    pub thread_id: String,
    pub thread_name: String,
    pub updated_at: Option<DateTime<Utc>>,
}

impl RolloutLine {
    pub fn parse(raw_line: &str) -> Result<Self, RolloutLineParseError> {
        let raw: Value = serde_json::from_str(raw_line.trim())?;
        let object = raw.as_object().ok_or(RolloutLineParseError::NotObject)?;
        let item_type = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or(RolloutLineParseError::MissingType)?
            .to_string();
        let timestamp = object
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|ts| ts.with_timezone(&Utc));
        let payload = object.get("payload").cloned().unwrap_or(Value::Null);
        Ok(Self {
            timestamp,
            item_type,
            payload,
            raw,
        })
    }

    pub fn session_metadata(&self) -> Option<SessionMetadata> {
        if self.item_type != "session_meta" {
            return None;
        }
        let payload = self.payload.as_object()?;
        let thread_id = string_at(payload.get("id"))?;
        let git = payload.get("git").and_then(Value::as_object);
        Some(SessionMetadata {
            thread_id,
            forked_from_id: string_at(payload.get("forked_from_id")),
            created_at: payload
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                .map(|ts| ts.with_timezone(&Utc))
                .or(self.timestamp),
            cwd: string_at(payload.get("cwd")),
            source: source_to_string(payload.get("source")),
            thread_source: payload.get("thread_source").cloned(),
            agent_nickname: string_at(payload.get("agent_nickname")),
            agent_role: string_at(payload.get("agent_role")),
            agent_path: string_at(payload.get("agent_path")),
            model_provider: string_at(payload.get("model_provider")),
            cli_version: string_at(payload.get("cli_version")),
            git_branch: git.and_then(|git| string_at(git.get("branch"))),
            git_sha: git.and_then(|git| git.get("commit_hash")).and_then(|sha| {
                sha.as_str()
                    .map(ToOwned::to_owned)
                    .or_else(|| sha.get("0").and_then(Value::as_str).map(ToOwned::to_owned))
            }),
            git_origin_url: git.and_then(|git| string_at(git.get("repository_url"))),
            memory_mode: string_at(payload.get("memory_mode")),
        })
    }
}

pub fn parse_thread_name_update(raw_line: &str) -> Option<ThreadNameUpdate> {
    let value: Value = serde_json::from_str(raw_line.trim()).ok()?;
    let object = value.as_object()?;
    Some(ThreadNameUpdate {
        thread_id: string_at(object.get("id"))?,
        thread_name: string_at(object.get("thread_name"))?,
        updated_at: object
            .get("updated_at")
            .and_then(Value::as_str)
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|ts| ts.with_timezone(&Utc)),
    })
}

fn source_to_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(source) => Some(source.clone()),
        other => Some(other.to_string()),
    }
}

fn string_at(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Object(object) => object
            .get("value")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_session_meta_without_requiring_full_codex_schema() {
        let raw = r#"{"timestamp":"2026-01-01T00:00:00.000Z","type":"session_meta","payload":{"id":"thread-1","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp/project","source":"cli","cli_version":"1.2.3","model_provider":"openai","git":{"branch":"main","commit_hash":"abc","repository_url":"git@example"}}}"#;
        let line = RolloutLine::parse(raw).unwrap();
        let meta = line.session_metadata().unwrap();
        assert_eq!(meta.thread_id, "thread-1");
        assert_eq!(meta.cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(meta.git_branch.as_deref(), Some("main"));
    }

    #[test]
    fn parses_thread_name_update() {
        let raw =
            r#"{"id":"thread-1","thread_name":"Decision log","updated_at":"2026-01-01T00:00:00Z"}"#;
        let update = parse_thread_name_update(raw).unwrap();
        assert_eq!(update.thread_id, "thread-1");
        assert_eq!(update.thread_name, "Decision log");
    }
}
