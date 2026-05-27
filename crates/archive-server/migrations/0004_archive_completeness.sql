ALTER TABLE rollout_files
  ADD COLUMN IF NOT EXISTS thread_id TEXT REFERENCES threads(thread_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS import_schema_version INTEGER NOT NULL DEFAULT 1;

UPDATE rollout_files rf
SET thread_id = lines.thread_id
FROM (
  SELECT file_id, MIN(thread_id) AS thread_id
  FROM rollout_lines
  GROUP BY file_id
) lines
WHERE rf.id = lines.file_id
  AND rf.thread_id IS NULL;

CREATE INDEX IF NOT EXISTS rollout_files_thread_idx
  ON rollout_files(thread_id, relative_path, file_version DESC);

CREATE TABLE IF NOT EXISTS session_index_lines (
  id BIGSERIAL PRIMARY KEY,
  file_id BIGINT NOT NULL REFERENCES rollout_files(id) ON DELETE CASCADE,
  file_version INTEGER NOT NULL,
  line_number BIGINT NOT NULL,
  byte_start BIGINT NOT NULL,
  byte_end BIGINT NOT NULL,
  thread_id TEXT,
  thread_name TEXT,
  updated_at TIMESTAMPTZ,
  raw TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (file_id, file_version, line_number, content_hash)
);

CREATE INDEX IF NOT EXISTS session_index_lines_thread_idx
  ON session_index_lines(thread_id, updated_at DESC NULLS LAST, line_number DESC);
