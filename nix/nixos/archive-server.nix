{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.codexSessionArchive;
  system = pkgs.stdenv.hostPlatform.system;
  localDatabaseUrl = "postgresql:///${cfg.database.name}?host=/run/postgresql&user=${cfg.database.user}";
in
{
  options.services.codexSessionArchive = {
    enable = lib.mkEnableOption "Codex session archive server";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.archive-server;
      defaultText = lib.literalExpression "codex-sessions.packages.\${system}.archive-server";
      description = "Package providing the archive-server binary.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "codex_archive";
      description = "User that runs the archive server.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "codex_archive";
      description = "Group that runs the archive server.";
    };

    bindAddr = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:8787";
      example = "0.0.0.0:8787";
      description = "Address and port the HTTP server binds to.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Whether to open the configured TCP port in the firewall.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8787;
      description = "TCP port to open when openFirewall is enabled.";
    };

    database = {
      local = {
        enable = lib.mkOption {
          type = lib.types.bool;
          default = true;
          description = "Whether to provision a local PostgreSQL database with pgvector.";
        };
      };

      name = lib.mkOption {
        type = lib.types.str;
        default = "codex_archive";
        description = "PostgreSQL database name.";
      };

      user = lib.mkOption {
        type = lib.types.str;
        default = cfg.user;
        defaultText = lib.literalExpression "config.services.codexSessionArchive.user";
        description = "PostgreSQL role used by the archive server.";
      };

      url = lib.mkOption {
        type = lib.types.str;
        default = localDatabaseUrl;
        defaultText = lib.literalExpression ''"postgresql:///$name?host=/run/postgresql&user=$user"'';
        description = "DATABASE_URL passed to the archive server.";
      };
    };

    secrets = {
      ingestTokenFile = lib.mkOption {
        type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
        default = null;
        description = "Path to a file containing the bearer token accepted for ingestion.";
      };

      readTokenFile = lib.mkOption {
        type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
        default = null;
        description = "Path to a file containing the bearer token accepted for read/query APIs.";
      };

      openaiApiKeyFile = lib.mkOption {
        type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
        default = null;
        description = "Path to a file containing OPENAI_API_KEY.";
      };
    };

    embeddingModel = lib.mkOption {
      type = lib.types.str;
      default = "text-embedding-3-small";
      description = "OpenAI embedding model used for semantic search.";
    };

    embeddingBackend = lib.mkOption {
      type = lib.types.enum [
        "batch"
        "sync"
      ];
      default = "batch";
      description = "Backend used for chunk embeddings. Query-time search embeddings remain synchronous.";
    };

    embeddingDimensions = lib.mkOption {
      type = lib.types.ints.positive;
      default = 1536;
      description = "Embedding vector dimensions expected by the database schema.";
    };

    embeddingBatchMaxRequests = lib.mkOption {
      type = lib.types.ints.positive;
      default = 512;
      description = "Maximum chunk embedding requests per OpenAI Batch submission.";
    };

    embeddingBatchPollSeconds = lib.mkOption {
      type = lib.types.ints.positive;
      default = 30;
      description = "Polling interval for OpenAI Batch status and result processing.";
    };

    maxIngestBodyBytes = lib.mkOption {
      type = lib.types.ints.positive;
      default = 67108864;
      description = "Maximum accepted ingest request body size.";
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "archive_server=info,tower_http=info";
      description = "RUST_LOG value for the archive server.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.secrets.ingestTokenFile != null;
        message = "services.codexSessionArchive.secrets.ingestTokenFile must be set.";
      }
      {
        assertion = cfg.secrets.readTokenFile != null;
        message = "services.codexSessionArchive.secrets.readTokenFile must be set.";
      }
      {
        assertion = cfg.secrets.openaiApiKeyFile != null;
        message = "services.codexSessionArchive.secrets.openaiApiKeyFile must be set.";
      }
    ];

    users.groups.${cfg.group} = { };
    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
    };

    services.postgresql = lib.mkIf cfg.database.local.enable {
      enable = true;
      package = pkgs.postgresql_16.withPackages (ps: [ ps.pgvector ]);
      extensions = ps: [ ps.pgvector ];
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
      authentication = lib.mkAfter ''
        local ${cfg.database.name} ${cfg.database.user} peer
      '';
    };

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ cfg.port ];

    systemd.services.codex-session-archive-db-init = lib.mkIf cfg.database.local.enable {
      description = "Prepare Codex Session Archive database";
      wantedBy = [ "multi-user.target" ];
      requires = [ "postgresql.service" ];
      after = [
        "postgresql.service"
        "postgresql-setup.service"
      ];
      before = [ "codex-session-archive.service" ];

      script = ''
        ${config.services.postgresql.package}/bin/psql ${lib.escapeShellArg cfg.database.name} \
          --command 'CREATE EXTENSION IF NOT EXISTS vector'
      '';

      serviceConfig = {
        Type = "oneshot";
        User = "postgres";
        Group = "postgres";
      };
    };

    systemd.services.codex-session-archive = {
      description = "Codex Session Archive Server";
      wantedBy = [ "multi-user.target" ];
      wants = [
        "network-online.target"
      ]
      ++ lib.optionals cfg.database.local.enable [
        "codex-session-archive-db-init.service"
        "postgresql.service"
        "postgresql-setup.service"
      ];
      after = [
        "network-online.target"
      ]
      ++ lib.optionals cfg.database.local.enable [
        "codex-session-archive-db-init.service"
        "postgresql.service"
        "postgresql-setup.service"
      ];

      environment = {
        ARCHIVE_MAX_INGEST_BODY_BYTES = toString cfg.maxIngestBodyBytes;
        BIND_ADDR = cfg.bindAddr;
        DATABASE_URL = cfg.database.url;
        EMBEDDING_DIMENSIONS = toString cfg.embeddingDimensions;
        OPENAI_EMBEDDING_BACKEND = cfg.embeddingBackend;
        OPENAI_EMBEDDING_BATCH_MAX_REQUESTS = toString cfg.embeddingBatchMaxRequests;
        OPENAI_EMBEDDING_BATCH_POLL_SECONDS = toString cfg.embeddingBatchPollSeconds;
        OPENAI_EMBEDDING_MODEL = cfg.embeddingModel;
        RUST_LOG = cfg.logLevel;
        SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
      };

      script = ''
        export ARCHIVE_INGEST_TOKEN="$(cat ${lib.escapeShellArg cfg.secrets.ingestTokenFile})"
        export ARCHIVE_READ_TOKEN="$(cat ${lib.escapeShellArg cfg.secrets.readTokenFile})"
        export OPENAI_API_KEY="$(cat ${lib.escapeShellArg cfg.secrets.openaiApiKeyFile})"
        exec ${cfg.package}/bin/archive-server
      '';

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        Restart = "on-failure";
        RestartSec = "5s";
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
      };
    };
  };
}
