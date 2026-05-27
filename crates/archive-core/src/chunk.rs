use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::hash::sha256_hex;
use crate::rollout::RolloutLine;

const TARGET_CHARS: usize = 4_800;
const OVERLAP_CHARS: usize = 600;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChunkKind {
    UserMessage,
    AssistantMessage,
    Compaction,
    Command,
    Tool,
    Patch,
    Goal,
    Lifecycle,
    Reasoning,
    Warning,
    Error,
}

impl ChunkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::AssistantMessage => "assistant_message",
            Self::Compaction => "compaction",
            Self::Command => "command",
            Self::Tool => "tool",
            Self::Patch => "patch",
            Self::Goal => "goal",
            Self::Lifecycle => "lifecycle",
            Self::Reasoning => "reasoning",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chunk {
    pub chunk_kind: ChunkKind,
    pub role: Option<String>,
    pub turn_id: Option<String>,
    pub text: String,
    pub metadata: Value,
    pub start_line: i64,
    pub end_line: i64,
    pub content_hash: String,
}

pub fn chunk_rollout_lines(lines: &[(i64, RolloutLine)]) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut current_turn_id: Option<String> = None;

    for (line_number, line) in lines {
        if line.item_type == "turn_context" {
            current_turn_id = line
                .payload
                .get("turn_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(current_turn_id);
            continue;
        }

        let extracted = extract_chunk_text(line);
        let Some((kind, role, text, metadata)) = extracted else {
            continue;
        };
        for text in split_large_text(text) {
            let content_hash = sha256_hex(format!(
                "{}\n{}\n{}\n{}",
                line_number,
                kind.as_str(),
                role.as_deref().unwrap_or_default(),
                text
            ));
            chunks.push(Chunk {
                chunk_kind: kind,
                role: role.clone(),
                turn_id: current_turn_id.clone(),
                text,
                metadata: metadata.clone(),
                start_line: *line_number,
                end_line: *line_number,
                content_hash,
            });
        }
    }

    chunks
}

fn extract_chunk_text(line: &RolloutLine) -> Option<(ChunkKind, Option<String>, String, Value)> {
    match line.item_type.as_str() {
        "event_msg" => extract_event_msg(&line.payload),
        "response_item" => extract_response_item(&line.payload),
        "compacted" => line
            .payload
            .get("message")
            .and_then(Value::as_str)
            .map(|text| {
                (
                    ChunkKind::Compaction,
                    Some("assistant".to_string()),
                    text.trim().to_string(),
                    metadata_for(line, None),
                )
            })
            .filter(|(_, _, text, _)| !text.is_empty()),
        _ => None,
    }
}

fn extract_event_msg(payload: &Value) -> Option<(ChunkKind, Option<String>, String, Value)> {
    let event_type = payload.get("type")?.as_str()?;
    match event_type {
        "user_message" => payload
            .get("message")
            .and_then(Value::as_str)
            .map(strip_codex_user_prefix)
            .filter(|text| !text.is_empty())
            .map(|text| {
                (
                    ChunkKind::UserMessage,
                    Some("user".to_string()),
                    text,
                    metadata_for_payload(payload),
                )
            }),
        "agent_message" => payload
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(|text| {
                (
                    ChunkKind::AssistantMessage,
                    Some("assistant".to_string()),
                    text.to_string(),
                    metadata_for_payload(payload),
                )
            }),
        "agent_reasoning_raw_content" => None,
        "task_started" => Some((
            ChunkKind::Lifecycle,
            None,
            summarize_task_started(payload),
            metadata_for_payload(payload),
        )),
        "task_complete" => Some((
            ChunkKind::Lifecycle,
            None,
            summarize_task_complete(payload),
            metadata_for_payload(payload),
        )),
        "thread_rolled_back" => Some((
            ChunkKind::Lifecycle,
            None,
            summarize_thread_rolled_back(payload),
            metadata_for_payload(payload),
        )),
        "turn_aborted" => Some((
            ChunkKind::Lifecycle,
            None,
            summarize_turn_aborted(payload),
            metadata_for_payload(payload),
        )),
        "item_completed" => summarize_item_completed(payload).map(|text| {
            (
                ChunkKind::Lifecycle,
                None,
                text,
                metadata_for_payload(payload),
            )
        }),
        "thread_name_updated" => payload
            .get("thread_name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(|text| {
                (
                    ChunkKind::Lifecycle,
                    None,
                    format!("Thread renamed: {text}"),
                    metadata_for_payload(payload),
                )
            }),
        "view_image_tool_call"
        | "collab_agent_spawn_end"
        | "collab_waiting_end"
        | "collab_close_end" => {
            let text = summarize_event_with_preferred_fields(
                event_type,
                payload,
                &["path", "prompt", "/status/completed", "thread_name"],
            );
            Some((ChunkKind::Tool, None, text, metadata_for_payload(payload)))
        }
        "exec_command_begin" => {
            let command = payload
                .get("command")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .filter(|command| !command.is_empty())?;
            let cwd = payload
                .get("cwd")
                .and_then(Value::as_str)
                .unwrap_or_default();
            Some((
                ChunkKind::Command,
                None,
                format!("Command started in {cwd}: {command}"),
                metadata_for_payload(payload),
            ))
        }
        "exec_command_end" => {
            let command = payload
                .get("command")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            let formatted = payload
                .get("formatted_output")
                .and_then(Value::as_str)
                .or_else(|| payload.get("aggregated_output").and_then(Value::as_str))
                .or_else(|| payload.get("stdout").and_then(Value::as_str))
                .unwrap_or_default()
                .trim();
            let status = payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("completed");
            let text = if formatted.is_empty() {
                format!("Command {status}: {command}")
            } else {
                format!("Command {status}: {command}\n{formatted}")
            };
            Some((
                ChunkKind::Command,
                None,
                text,
                metadata_for_payload(payload),
            ))
        }
        "mcp_tool_call_begin"
        | "mcp_tool_call_end"
        | "dynamic_tool_call_request"
        | "dynamic_tool_call_response"
        | "web_search_begin"
        | "web_search_end"
        | "image_generation_begin"
        | "image_generation_end" => {
            let text = compact_json_summary(event_type, payload);
            Some((ChunkKind::Tool, None, text, metadata_for_payload(payload)))
        }
        "patch_apply_begin"
        | "patch_apply_updated"
        | "patch_apply_end"
        | "apply_patch_approval_request" => {
            let text = compact_json_summary(event_type, payload);
            Some((ChunkKind::Patch, None, text, metadata_for_payload(payload)))
        }
        "thread_goal_updated" | "plan_update" | "plan_delta" => {
            let text = payload
                .pointer("/goal/objective")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| compact_json_summary(event_type, payload));
            Some((ChunkKind::Goal, None, text, metadata_for_payload(payload)))
        }
        "warning" | "guardian_warning" | "deprecation_notice" | "stream_error" => payload
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(|text| {
                (
                    ChunkKind::Warning,
                    None,
                    text.to_string(),
                    metadata_for_payload(payload),
                )
            }),
        "error" => payload
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(|text| {
                (
                    ChunkKind::Error,
                    None,
                    text.to_string(),
                    metadata_for_payload(payload),
                )
            }),
        _ => None,
    }
}

fn extract_response_item(payload: &Value) -> Option<(ChunkKind, Option<String>, String, Value)> {
    match payload.get("type").and_then(Value::as_str)? {
        "message" => {
            let role = payload.get("role").and_then(Value::as_str)?.to_string();
            if role != "user" && role != "assistant" {
                return None;
            }
            let text = payload
                .get("content")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            item.get("text")
                                .and_then(Value::as_str)
                                .or_else(|| item.get("input_text").and_then(Value::as_str))
                                .or_else(|| item.get("output_text").and_then(Value::as_str))
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })?
                .trim()
                .to_string();
            if text.is_empty() {
                return None;
            }
            let kind = if role == "user" {
                ChunkKind::UserMessage
            } else {
                ChunkKind::AssistantMessage
            };
            Some((kind, Some(role), text, metadata_for_payload(payload)))
        }
        "function_call" | "custom_tool_call" => summarize_response_tool_call(payload)
            .map(|text| (ChunkKind::Tool, None, text, metadata_for_payload(payload))),
        "function_call_output" | "custom_tool_call_output" => {
            summarize_response_tool_output(payload)
                .map(|text| (ChunkKind::Tool, None, text, metadata_for_payload(payload)))
        }
        "web_search_call" => summarize_web_search_call(payload)
            .map(|text| (ChunkKind::Tool, None, text, metadata_for_payload(payload))),
        "reasoning" => summarize_reasoning(payload).map(|text| {
            (
                ChunkKind::Reasoning,
                None,
                text,
                metadata_for_payload(payload),
            )
        }),
        _ => None,
    }
}

fn split_large_text(text: String) -> Vec<String> {
    if text.chars().count() <= TARGET_CHARS {
        return vec![text];
    }
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + TARGET_CHARS).min(chars.len());
        out.push(chars[start..end].iter().collect::<String>());
        if end == chars.len() {
            break;
        }
        start = end.saturating_sub(OVERLAP_CHARS);
    }
    out
}

fn strip_codex_user_prefix(text: &str) -> String {
    const PREFIX: &str = "## My request for Codex:";
    match text.find(PREFIX) {
        Some(index) => text[index + PREFIX.len()..].trim().to_string(),
        None => text.trim().to_string(),
    }
}

fn compact_json_summary(event_type: &str, payload: &Value) -> String {
    let mut text = String::new();
    text.push_str(event_type);
    if let Some(invocation) = payload.get("invocation") {
        text.push(' ');
        text.push_str(&invocation.to_string());
    } else if let Some(tool) = payload.get("tool").and_then(Value::as_str) {
        text.push(' ');
        text.push_str(tool);
    }
    if let Some(result) = payload.get("result") {
        text.push('\n');
        text.push_str(&result.to_string());
    } else if let Some(status) = payload.get("status").and_then(Value::as_str) {
        text.push_str(": ");
        text.push_str(status);
    }
    text
}

fn summarize_task_started(payload: &Value) -> String {
    let turn_id = payload
        .get("turn_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mode = payload
        .get("collaboration_mode_kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match payload.get("model_context_window").and_then(Value::as_i64) {
        Some(window) => {
            format!("Task started for turn {turn_id} in {mode} mode (context window {window})")
        }
        None => format!("Task started for turn {turn_id} in {mode} mode"),
    }
}

fn summarize_task_complete(payload: &Value) -> String {
    let message = payload
        .get("last_agent_message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty());
    match message {
        Some(message) => format!("Task completed\n{message}"),
        None => "Task completed".to_string(),
    }
}

fn summarize_thread_rolled_back(payload: &Value) -> String {
    let turns = payload
        .get("num_turns")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    format!("Thread rolled back by {turns} turn(s)")
}

fn summarize_turn_aborted(payload: &Value) -> String {
    let reason = payload
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match payload.get("duration_ms").and_then(Value::as_i64) {
        Some(duration_ms) => format!("Turn aborted: {reason} after {duration_ms} ms"),
        None => format!("Turn aborted: {reason}"),
    }
}

fn summarize_item_completed(payload: &Value) -> Option<String> {
    let item = payload.get("item")?.as_object()?;
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("item");
    let text = item.get("text").and_then(Value::as_str).map(str::trim);
    Some(match text.filter(|text| !text.is_empty()) {
        Some(text) => format!("{item_type} completed\n{text}"),
        None => format!("{item_type} completed"),
    })
}

fn summarize_event_with_preferred_fields(
    event_type: &str,
    payload: &Value,
    preferred_fields: &[&str],
) -> String {
    for field in preferred_fields {
        let text = if field.starts_with('/') {
            payload.pointer(field).and_then(value_to_string)
        } else {
            payload.get(*field).and_then(value_to_string)
        };
        if let Some(text) = text.filter(|text| !text.trim().is_empty()) {
            return format!("{event_type}\n{text}");
        }
    }
    compact_json_summary(event_type, payload)
}

fn summarize_response_tool_call(payload: &Value) -> Option<String> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("tool_call");
    let name = payload
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let input = payload
        .get("arguments")
        .and_then(Value::as_str)
        .map(compact_json_or_text)
        .or_else(|| {
            payload
                .get("input")
                .and_then(Value::as_str)
                .map(compact_json_or_text)
        })
        .filter(|text| !text.is_empty());
    Some(match input {
        Some(input) => format!("{kind} {name}\n{input}"),
        None => format!("{kind} {name}"),
    })
}

fn summarize_response_tool_output(payload: &Value) -> Option<String> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("tool_output");
    let call_id = payload
        .get("call_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let output = payload
        .get("output")
        .and_then(Value::as_str)
        .map(compact_json_or_text)
        .filter(|text| !text.is_empty());
    Some(match output {
        Some(output) => format!("{kind} {call_id}\n{output}"),
        None => format!("{kind} {call_id}"),
    })
}

fn summarize_web_search_call(payload: &Value) -> Option<String> {
    let action = payload.get("action")?;
    let query = action
        .get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty());
    let queries = action
        .get("queries")
        .and_then(Value::as_array)
        .map(|queries| {
            queries
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let body = if let Some(query) = query {
        let mut body = vec![query.to_string()];
        for extra in queries {
            if extra != query {
                body.push(extra.to_string());
            }
        }
        body.join("\n")
    } else if queries.is_empty() {
        String::new()
    } else {
        queries.join("\n")
    };
    Some(if body.is_empty() {
        "web_search_call".to_string()
    } else {
        format!("web_search_call\n{body}")
    })
}

fn summarize_reasoning(payload: &Value) -> Option<String> {
    let parts = payload
        .get("summary")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(reasoning_summary_entry)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if parts.is_empty() {
        None
    } else {
        Some(format!("Reasoning summary\n{}", parts.join("\n")))
    }
}

fn reasoning_summary_entry(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let text = text.trim();
            (!text.is_empty()).then(|| text.to_string())
        }
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("summary"))
            .or_else(|| map.get("content"))
            .and_then(value_to_string)
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty()),
        _ => None,
    }
}

fn compact_json_or_text(text: &str) -> String {
    serde_json::from_str::<Value>(text)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| text.trim().to_string())
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn metadata_for(line: &RolloutLine, extra: Option<Value>) -> Value {
    let mut metadata = serde_json::Map::new();
    metadata.insert("type".to_string(), Value::String(line.item_type.clone()));
    if let Some(extra) = extra {
        metadata.insert("extra".to_string(), extra);
    }
    Value::Object(metadata)
}

fn metadata_for_payload(payload: &Value) -> Value {
    let mut metadata = serde_json::Map::new();
    if let Some(event_type) = payload.get("type").and_then(Value::as_str) {
        metadata.insert(
            "event_type".to_string(),
            Value::String(event_type.to_string()),
        );
        metadata.insert(
            "payload_type".to_string(),
            Value::String(event_type.to_string()),
        );
    }
    if let Some(call_id) = payload.get("call_id").and_then(Value::as_str) {
        metadata.insert("call_id".to_string(), Value::String(call_id.to_string()));
    }
    if let Some(name) = payload.get("name").and_then(Value::as_str) {
        metadata.insert("name".to_string(), Value::String(name.to_string()));
    }
    if let Some(thread_id) = payload.get("thread_id").and_then(Value::as_str) {
        metadata.insert(
            "thread_id".to_string(),
            Value::String(thread_id.to_string()),
        );
    }
    if let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) {
        metadata.insert("turn_id".to_string(), Value::String(turn_id.to_string()));
    }
    Value::Object(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rollout::RolloutLine;

    #[test]
    fn chunks_user_agent_command_and_skips_raw_reasoning() {
        let raw = [
            r#"{"timestamp":"2026-01-01T00:00:00.000Z","type":"turn_context","payload":{"turn_id":"turn-1","cwd":"/tmp","model":"gpt"}}"#,
            r###"{"timestamp":"2026-01-01T00:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"## My request for Codex:\nFind this decision"}}"###,
            r#"{"timestamp":"2026-01-01T00:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Decision found"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:03.000Z","type":"event_msg","payload":{"type":"agent_reasoning_raw_content","text":"secret-ish trace"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:04.000Z","type":"event_msg","payload":{"type":"exec_command_end","command":["rg","decision"],"cwd":"/tmp","status":"completed","formatted_output":"hit"}}"#,
        ];
        let parsed = raw
            .iter()
            .enumerate()
            .map(|(idx, raw)| ((idx + 1) as i64, RolloutLine::parse(raw).unwrap()))
            .collect::<Vec<_>>();
        let chunks = chunk_rollout_lines(&parsed);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "Find this decision");
        assert_eq!(chunks[0].turn_id.as_deref(), Some("turn-1"));
        assert!(chunks
            .iter()
            .any(|chunk| chunk.text.contains("rg decision")));
        assert!(!chunks.iter().any(|chunk| chunk.text.contains("secret-ish")));
    }

    #[test]
    fn chunks_response_tool_calls_and_reasoning_summaries() {
        let raw = [
            r#"{"timestamp":"2026-01-01T00:00:00.000Z","type":"turn_context","payload":{"turn_id":"turn-1"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}","call_id":"call-1"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"{\"output\":\"/tmp\"}"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:03.000Z","type":"response_item","payload":{"type":"reasoning","summary":[{"text":"Check the workspace first."}]}}"#,
            r#"{"timestamp":"2026-01-01T00:00:04.000Z","type":"event_msg","payload":{"type":"thread_rolled_back","num_turns":2}}"#,
        ];
        let parsed = raw
            .iter()
            .enumerate()
            .map(|(idx, raw)| ((idx + 1) as i64, RolloutLine::parse(raw).unwrap()))
            .collect::<Vec<_>>();

        let chunks = chunk_rollout_lines(&parsed);

        assert!(chunks
            .iter()
            .any(|chunk| chunk.text.contains("function_call exec_command")));
        assert!(chunks
            .iter()
            .any(|chunk| chunk.text.contains("function_call_output call-1")));
        assert!(chunks
            .iter()
            .any(|chunk| chunk.chunk_kind == ChunkKind::Reasoning
                && chunk.text.contains("Check the workspace first.")));
        assert!(chunks
            .iter()
            .any(|chunk| chunk.chunk_kind == ChunkKind::Lifecycle
                && chunk.text.contains("rolled back by 2 turn")));
    }

    #[test]
    fn splits_large_outputs_with_overlap() {
        let text = "a".repeat(TARGET_CHARS + 100);
        let chunks = split_large_text(text);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].len() >= TARGET_CHARS);
        assert!(chunks[1].len() >= OVERLAP_CHARS);
    }
}
