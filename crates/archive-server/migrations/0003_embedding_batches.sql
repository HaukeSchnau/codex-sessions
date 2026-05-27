CREATE TABLE IF NOT EXISTS embedding_batches (
  id BIGSERIAL PRIMARY KEY,
  openai_batch_id TEXT NOT NULL UNIQUE,
  openai_input_file_id TEXT NOT NULL,
  openai_output_file_id TEXT,
  openai_error_file_id TEXT,
  status TEXT NOT NULL,
  request_count INTEGER NOT NULL,
  last_error TEXT,
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  results_applied_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE embedding_jobs
  ADD COLUMN IF NOT EXISTS batch_id BIGINT REFERENCES embedding_batches(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS batch_custom_id TEXT;

CREATE INDEX IF NOT EXISTS embedding_batches_status_idx
  ON embedding_batches(status, submitted_at DESC);

CREATE INDEX IF NOT EXISTS embedding_batches_results_idx
  ON embedding_batches(results_applied_at, submitted_at DESC);

CREATE INDEX IF NOT EXISTS embedding_jobs_batch_idx
  ON embedding_jobs(batch_id, status, created_at DESC);
