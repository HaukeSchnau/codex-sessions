CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS machines (
  machine_id TEXT PRIMARY KEY,
  hostname TEXT NOT NULL,
  installation_id TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rollout_files (
  id BIGSERIAL PRIMARY KEY,
  machine_id TEXT NOT NULL REFERENCES machines(machine_id) ON DELETE CASCADE,
  relative_path TEXT NOT NULL,
  kind TEXT NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT false,
  file_version INTEGER NOT NULL DEFAULT 1,
  size_bytes BIGINT NOT NULL,
  modified_at TIMESTAMPTZ,
  file_hash TEXT NOT NULL,
  prefix_hash TEXT NOT NULL,
  import_byte_cursor BIGINT NOT NULL DEFAULT 0,
  import_line_cursor BIGINT NOT NULL DEFAULT 0,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (machine_id, relative_path, file_version)
);

CREATE TABLE IF NOT EXISTS threads (
  thread_id TEXT PRIMARY KEY,
  name TEXT,
  cwd TEXT,
  source TEXT,
  thread_source JSONB,
  agent_nickname TEXT,
  agent_role TEXT,
  agent_path TEXT,
  model_provider TEXT,
  model TEXT,
  cli_version TEXT,
  git_branch TEXT,
  git_sha TEXT,
  git_origin_url TEXT,
  memory_mode TEXT,
  forked_from_id TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rollout_lines (
  id BIGSERIAL PRIMARY KEY,
  thread_id TEXT NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
  file_id BIGINT NOT NULL REFERENCES rollout_files(id) ON DELETE CASCADE,
  file_version INTEGER NOT NULL,
  line_number BIGINT NOT NULL,
  byte_start BIGINT NOT NULL,
  byte_end BIGINT NOT NULL,
  timestamp TIMESTAMPTZ,
  type TEXT NOT NULL,
  raw JSONB NOT NULL,
  payload JSONB NOT NULL,
  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (file_id, file_version, line_number, content_hash)
);

CREATE TABLE IF NOT EXISTS chunks (
  id BIGSERIAL PRIMARY KEY,
  thread_id TEXT NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
  file_id BIGINT NOT NULL REFERENCES rollout_files(id) ON DELETE CASCADE,
  turn_id TEXT,
  chunk_kind TEXT NOT NULL,
  role TEXT,
  text TEXT NOT NULL,
  start_line BIGINT NOT NULL,
  end_line BIGINT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  search_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', coalesce(text, ''))) STORED,
  embedding vector(1536),
  embedding_model TEXT,
  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (thread_id, file_id, content_hash)
);

CREATE TABLE IF NOT EXISTS embedding_jobs (
  chunk_id BIGINT PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  locked_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rollout_files_machine_path_idx ON rollout_files(machine_id, relative_path);
CREATE INDEX IF NOT EXISTS rollout_lines_thread_idx ON rollout_lines(thread_id, line_number);
CREATE INDEX IF NOT EXISTS threads_updated_idx ON threads(updated_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS threads_cwd_idx ON threads(cwd);
CREATE INDEX IF NOT EXISTS chunks_thread_idx ON chunks(thread_id, start_line);
CREATE INDEX IF NOT EXISTS chunks_search_idx ON chunks USING GIN(search_tsv);
CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
