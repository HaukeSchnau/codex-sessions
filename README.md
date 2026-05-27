# Codex Session Archive

Central archive/search service for Codex rollout JSONL.

## Components

- `archive-core`: permissive rollout parsing, metadata extraction, chunking, and hashing.
- `archive-server`: Axum HTTP API backed by Postgres, full-text search, and `pgvector`.
- `codex-session-archive-agent`: local push agent that scans `CODEX_HOME` and uploads complete JSONL records.

## Server

## Local Stack

This repo can run its required services with Docker Compose. The archive service image is built by Nix; there is no Dockerfile build path.

```sh
./scripts/dev-up
```

`scripts/dev-up` decrypts `secrets/dev.enc.yaml` with SOPS into a local ignored `.env`, builds and loads the `archive-server` Docker image with Nix, starts Postgres with `pgvector`, runs migrations on boot, and exposes:

- archive-server: `http://127.0.0.1:8787`
- Postgres: `127.0.0.1:55432`

Stop services:

```sh
./scripts/dev-down
```

Run the end-to-end fixture import and search/export checks:

```sh
./scripts/e2e-test
```

The SOPS age private key is expected at `.sops/age.key` by default and is intentionally ignored by version control. Set `SOPS_AGE_KEY_FILE` to use a different key.

Build the regular binaries with Nix:

```sh
nix build .#archive-server
nix build .#codex-session-archive-agent
```

Build the Docker image tarball with Nix and load it into Docker on Linux:

```sh
nix build .#archive-server-image
docker load --input result
```

On macOS, `./scripts/load-nix-image` builds `.#packages.aarch64-linux.archive-server-image` by default, so a configured ARM Linux Nix builder can produce the image without passing `--system`. Set `ARCHIVE_IMAGE_SYSTEM=x86_64-linux` only when you intentionally want the x86_64 Linux image. For a direct manual ARM image build, use:

```sh
nix build .#packages.aarch64-linux.archive-server-image
docker load --input result
```

Required environment:

```sh
export DATABASE_URL=postgres://user:pass@host:5432/codex_archive
export ARCHIVE_INGEST_TOKEN=change-me-ingest
export ARCHIVE_READ_TOKEN=change-me-read
export OPENAI_API_KEY=sk-...
```

For local agent runs, prefer storing the ingest token in a file and passing
`--token-file` so it does not appear in process listings.

Optional:

```sh
export OPENAI_EMBEDDING_BACKEND=batch
export OPENAI_EMBEDDING_MODEL=text-embedding-3-small
export OPENAI_EMBEDDING_BATCH_MAX_REQUESTS=512
export OPENAI_EMBEDDING_BATCH_POLL_SECONDS=30
export EMBEDDING_DIMENSIONS=1536
export ARCHIVE_MAX_INGEST_BODY_BYTES=67108864
export BIND_ADDR=127.0.0.1:8787
```

Chunk embeddings use the OpenAI Batch API by default for both fresh ingests and backlog/reindex work. Search-time query embeddings remain synchronous so semantic and hybrid searches still answer immediately.

Run:

```sh
cargo run -p archive-server
```

## Agent

One-shot import:

```sh
cargo run -p codex-session-archive-agent -- scan \
  --server http://127.0.0.1:8787 \
  --token-file ./ingest-token \
  --codex-home ~/.codex \
  --json
```

Continuous import:

```sh
cargo run -p codex-session-archive-agent -- watch \
  --server http://127.0.0.1:8787 \
  --token-file ./ingest-token \
  --codex-home ~/.codex \
  --interval-seconds 30
```

The agent asks the server for per-file cursors before each scan, skips already-imported files, and uploads only new complete JSONL records for append-only files. Cursor state now carries an ingest schema version, so when chunking or completeness rules improve the agent can do a one-time resend and let the server backfill missing raw lines, `session_index` history, and new searchable chunks. Use `--json` for agent-readable progress events or `--quiet` for silence.

Prune local rollout files that are already fully archived on the server:

```sh
cargo run -p codex-session-archive-agent -- prune \
  --server http://127.0.0.1:8787 \
  --token-file ./ingest-token \
  --codex-home ~/.codex \
  --min-age-days 30 \
  --dry-run \
  --json
```

`prune` only deletes rollout files when the server confirms the exact file hash and byte cursor have already been imported. By default it considers both `sessions/` and `archived_sessions/`; pass `--skip-archived-sessions` to keep local archived rollouts around.

## HTTP

- `GET /ui` for a tiny server-rendered browser UI
- `POST /v1/ingest/batch`
- `GET /v1/ingest/cursors?machine_id=...`
- `GET /v1/sync/status`
- `POST /v1/embeddings/prioritize`
- `GET /v1/threads`
- `GET /v1/threads/{thread_id}` including indexed chunks and archived thread-name history
- `GET /v1/threads/{thread_id}/raw` (`include_private_model_traces=true` required for raw reasoning traces)
- `GET /v1/search?q=...&mode=hybrid&scope=decisions`
- `POST /v1/query` with optional `scope`: `decisions`, `problems`, `commands`, `today`, or `recent`
- `GET /v1/export`
- `GET /healthz`
- `GET /readyz`
