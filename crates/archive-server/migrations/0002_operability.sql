CREATE TABLE IF NOT EXISTS ingest_errors (
  id BIGSERIAL PRIMARY KEY,
  machine_id TEXT NOT NULL REFERENCES machines(machine_id) ON DELETE CASCADE,
  relative_path TEXT NOT NULL,
  file_version INTEGER NOT NULL,
  line_number BIGINT,
  byte_start BIGINT,
  byte_end BIGINT,
  content_hash TEXT,
  error_kind TEXT NOT NULL,
  error_message TEXT NOT NULL,
  raw_preview TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (machine_id, relative_path, file_version, line_number, content_hash, error_kind)
);

CREATE INDEX IF NOT EXISTS ingest_errors_machine_path_idx ON ingest_errors(machine_id, relative_path, created_at DESC);
CREATE INDEX IF NOT EXISTS embedding_jobs_status_created_idx ON embedding_jobs(status, created_at);

CREATE OR REPLACE FUNCTION search_scope_matches(scope TEXT, chunk_kind TEXT, role TEXT, text TEXT, created_at TIMESTAMPTZ)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $$
  SELECT CASE scope
    WHEN 'decisions' THEN text ILIKE ANY (ARRAY['%decision%', '%decided%', '%chosen%', '%tradeoff%', '%because%'])
    WHEN 'problems' THEN text ILIKE ANY (ARRAY['%error%', '%failed%', '%failure%', '%debug%', '%fix%', '%issue%', '%blocked%'])
    WHEN 'commands' THEN chunk_kind IN ('command', 'command_output') OR text ILIKE ANY (ARRAY['%cargo %', '%nix %', '%docker %', '%jj %', '%git %'])
    WHEN 'today' THEN created_at >= date_trunc('day', now())
    WHEN 'recent' THEN created_at >= now() - interval '14 days'
    ELSE TRUE
  END
$$;
