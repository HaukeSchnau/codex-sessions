{
  lib,
  nix-infra-modules,
  pkgs,
}:
let
  projectRuntime = nix-infra-modules.lib.projectRuntime;
  postgres = pkgs.postgresql_16.withPackages (extensions: [ extensions.pgvector ]);

  cargoEnvironment = ''
    export CARGO_HOME="$PROJECT_CACHE_DIR/cargo-home"
    export CARGO_TARGET_DIR="$PROJECT_CACHE_DIR/cargo-target"
    export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
    install -d -m 0700 "$CARGO_HOME" "$CARGO_TARGET_DIR"
    cd "$PROJECT_CHECKOUT"
  '';

  prepareAction = pkgs.writeShellApplication {
    name = "codex-sessions-prepare";
    runtimeInputs = [
      pkgs.cargo
      pkgs.coreutils
      pkgs.findutils
      pkgs.gnused
      pkgs.rustc
    ];
    text = ''
      ${cargoEnvironment}

      preparation_state="$PROJECT_CACHE_DIR/preparation"
      stamp_file="$preparation_state/dependencies.sha256"
      host_target="$(rustc -vV | sed -n 's/^host: //p')"
      dependency_key=$(
        {
          printf '%s\n' "$host_target"
          sha256sum Cargo.lock Cargo.toml
          find crates -type f -name Cargo.toml -print0 \
            | sort -z \
            | xargs -0 -r sha256sum
        } | sha256sum | cut -d ' ' -f 1
      )

      if [[ -d "$CARGO_HOME/registry" && -f "$stamp_file" ]] \
        && [[ "$(<"$stamp_file")" == "$dependency_key" ]]; then
        echo "Codex Sessions dependencies are already prepared ($dependency_key)"
        exit 0
      fi

      cargo fetch --locked --target "$host_target"
      install -d -m 0700 "$preparation_state"
      printf '%s\n' "$dependency_key" > "$stamp_file.next"
      mv "$stamp_file.next" "$stamp_file"
    '';
  };

  postgresAction = pkgs.writeShellApplication {
    name = "codex-sessions-postgres";
    runtimeInputs = [
      pkgs.coreutils
      postgres
    ];
    text = ''
      listen_host="$($PROJECT_RUNTIME_QUERY endpoint postgres listen-host)"
      listen_port="$($PROJECT_RUNTIME_QUERY endpoint postgres listen-port)"
      data_dir="$PROJECT_STATE_DIR/postgres"
      socket_dir="$PROJECT_RUNTIME_DIR/postgres"
      bootstrap_socket_dir="$PROJECT_RUNTIME_DIR/postgres-bootstrap"
      bootstrap_marker="$data_dir/.project-initialized"
      log_file="$PROJECT_STATE_DIR/postgres.log"

      install -d -m 0700 "$PROJECT_STATE_DIR" "$PROJECT_RUNTIME_DIR" "$socket_dir"

      if [[ ! -s "$data_dir/PG_VERSION" ]]; then
        initdb \
          --pgdata="$data_dir" \
          --username=codex_archive \
          --auth-local=trust \
          --auth-host=trust \
          --encoding=UTF8 \
          --no-locale
      fi

      if [[ ! -e "$bootstrap_marker" ]]; then
        install -d -m 0700 "$bootstrap_socket_dir"
        pg_ctl \
          --pgdata="$data_dir" \
          --log="$log_file" \
          --options="-c listen_addresses= -k $bootstrap_socket_dir" \
          --wait \
          start

        stop_bootstrap() {
          pg_ctl --pgdata="$data_dir" --mode=fast --wait stop >/dev/null 2>&1 || true
        }
        trap stop_bootstrap EXIT

        database_exists="$(
          psql \
            --host="$bootstrap_socket_dir" \
            --username=codex_archive \
            --dbname=postgres \
            --tuples-only \
            --no-align \
            --command="SELECT 1 FROM pg_database WHERE datname = 'codex_archive'"
        )"
        if [[ "$database_exists" != 1 ]]; then
          createdb \
            --host="$bootstrap_socket_dir" \
            --username=codex_archive \
            codex_archive
        fi

        stop_bootstrap
        trap - EXIT
        touch "$bootstrap_marker"
      fi

      exec postgres \
        -D "$data_dir" \
        --listen_addresses="$listen_host" \
        --port="$listen_port" \
        --unix_socket_directories="$socket_dir"
    '';
  };

  webAction = pkgs.writeShellApplication {
    name = "codex-sessions-web";
    runtimeInputs = [
      pkgs.cargo
      pkgs.cargo-watch
      pkgs.coreutils
      pkgs.pkg-config
      pkgs.rustc
      pkgs.stdenv.cc
    ];
    text = ''
      ${cargoEnvironment}

      secret_value() {
        local semantic_name="$1"
        local environment_name="$2"
        local secret_file
        local environment_value

        if secret_file="$($PROJECT_RUNTIME_QUERY secret-file "$semantic_name")"; then
          if [[ ! -s "$secret_file" ]]; then
            echo "Project Secret is empty: $semantic_name" >&2
            return 66
          fi
          cat "$secret_file"
          return
        fi

        environment_value="$(printenv "$environment_name" || true)"
        if [[ -z "$environment_value" ]]; then
          echo "Project Secret is unavailable: $semantic_name (or $environment_name)" >&2
          return 66
        fi
        printf '%s' "$environment_value"
      }

      format_host() {
        case "$1" in
          *:*) printf '[%s]' "$1" ;;
          *) printf '%s' "$1" ;;
        esac
      }

      database_host="$($PROJECT_RUNTIME_QUERY endpoint postgres listen-host)"
      database_port="$($PROJECT_RUNTIME_QUERY endpoint postgres listen-port)"
      web_host="$($PROJECT_RUNTIME_QUERY endpoint web listen-host)"
      web_port="$($PROJECT_RUNTIME_QUERY endpoint web listen-port)"

      DATABASE_URL="postgresql://codex_archive@$(format_host "$database_host"):$database_port/codex_archive"
      BIND_ADDR="$(format_host "$web_host"):$web_port"
      ARCHIVE_INGEST_TOKEN="$(secret_value ingest-token ARCHIVE_INGEST_TOKEN)"
      ARCHIVE_READ_TOKEN="$(secret_value read-token ARCHIVE_READ_TOKEN)"
      OPENAI_API_KEY="$(secret_value openai-api-key OPENAI_API_KEY)"
      OPENAI_EMBEDDING_BACKEND="$($PROJECT_RUNTIME_QUERY parameter embedding-backend)"
      OPENAI_EMBEDDING_MODEL="$($PROJECT_RUNTIME_QUERY parameter embedding-model)"
      OPENAI_EMBEDDING_BATCH_MAX_REQUESTS="$($PROJECT_RUNTIME_QUERY parameter embedding-batch-max-requests)"
      OPENAI_EMBEDDING_BATCH_POLL_SECONDS="$($PROJECT_RUNTIME_QUERY parameter embedding-batch-poll-seconds)"
      EMBEDDING_DIMENSIONS="$($PROJECT_RUNTIME_QUERY parameter embedding-dimensions)"
      ARCHIVE_MAX_INGEST_BODY_BYTES="$($PROJECT_RUNTIME_QUERY parameter max-ingest-body-bytes)"
      RUST_LOG="''${RUST_LOG:-archive_server=info,tower_http=info}"
      export \
        ARCHIVE_INGEST_TOKEN \
        ARCHIVE_MAX_INGEST_BODY_BYTES \
        ARCHIVE_READ_TOKEN \
        BIND_ADDR \
        DATABASE_URL \
        EMBEDDING_DIMENSIONS \
        OPENAI_API_KEY \
        OPENAI_EMBEDDING_BACKEND \
        OPENAI_EMBEDDING_BATCH_MAX_REQUESTS \
        OPENAI_EMBEDDING_BATCH_POLL_SECONDS \
        OPENAI_EMBEDDING_MODEL \
        RUST_LOG

      exec cargo watch \
        --watch Cargo.toml \
        --watch Cargo.lock \
        --watch crates/archive-core \
        --watch crates/archive-server \
        --exec 'run --locked -p archive-server'
    '';
  };
in
projectRuntime.mkDevelopment {
  inherit pkgs;
  descriptorPath = ../project.json;
  actions = {
    prepare = lib.getExe prepareAction;
    postgres = lib.getExe postgresAction;
    web = lib.getExe webAction;
  };
}
