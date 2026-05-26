# Codex Session Archive

Central archive/search service for Codex rollout JSONL.

## Components

- `archive-core`: permissive rollout parsing, metadata extraction, chunking, and hashing.
- `archive-server`: Axum HTTP API backed by Postgres, full-text search, and `pgvector`.
- `archive-agent`: local push agent that scans `CODEX_HOME` and uploads complete JSONL records.

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
nix build .#archive-agent
```

Build the Docker image tarball with Nix and load it into Docker:

```sh
nix build .#archive-server-image
docker load --input result
```

On macOS, `./scripts/load-nix-image` automatically builds the Linux image attribute for the architecture used by the local Docker daemon. Set `ARCHIVE_IMAGE_SYSTEM=aarch64-linux` or `ARCHIVE_IMAGE_SYSTEM=x86_64-linux` to override that detection. For a direct manual build of a specific Linux image, use `nix build .#packages.aarch64-linux.archive-server-image` or `nix build .#packages.x86_64-linux.archive-server-image`.

Required environment:

```sh
export DATABASE_URL=postgres://user:pass@host:5432/codex_archive
export ARCHIVE_INGEST_TOKEN=change-me-ingest
export ARCHIVE_READ_TOKEN=change-me-read
export OPENAI_API_KEY=sk-...
```

Optional:

```sh
export OPENAI_EMBEDDING_MODEL=text-embedding-3-small
export EMBEDDING_DIMENSIONS=1536
export BIND_ADDR=127.0.0.1:8787
```

Run:

```sh
cargo run -p archive-server
```

## Agent

One-shot import:

```sh
cargo run -p archive-agent -- scan \
  --server http://127.0.0.1:8787 \
  --token "$ARCHIVE_INGEST_TOKEN" \
  --codex-home ~/.codex
```

Continuous import:

```sh
cargo run -p archive-agent -- watch \
  --server http://127.0.0.1:8787 \
  --token "$ARCHIVE_INGEST_TOKEN" \
  --codex-home ~/.codex \
  --interval-seconds 30
```

## HTTP

- `POST /v1/ingest/batch`
- `GET /v1/threads`
- `GET /v1/threads/{thread_id}`
- `GET /v1/threads/{thread_id}/raw`
- `GET /v1/search?q=...&mode=hybrid`
- `POST /v1/query`
- `GET /v1/export`
- `GET /healthz`
- `GET /readyz`
