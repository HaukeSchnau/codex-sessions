{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.codexSessionArchiveAgent;
  system = pkgs.stdenv.hostPlatform.system;
  logDir = "${config.home.homeDirectory}/Library/Logs/codex-session-archive";
  runAgentArgs = lib.escapeShellArgs [
    "watch"
    "--server"
    cfg.serverUrl
    "--token-file"
    (toString cfg.ingestTokenFile)
    "--codex-home"
    cfg.codexHome
    "--max-lines-per-batch"
    (toString cfg.maxLinesPerBatch)
    "--request-timeout-seconds"
    (toString cfg.requestTimeoutSeconds)
    "--interval-seconds"
    (toString cfg.intervalSeconds)
    "--quiet"
  ];
  runPruneArgs = lib.escapeShellArgs (
    [
      "prune"
      "--server"
      cfg.serverUrl
      "--token-file"
      (toString cfg.ingestTokenFile)
      "--codex-home"
      cfg.codexHome
      "--request-timeout-seconds"
      (toString cfg.requestTimeoutSeconds)
      "--min-age-days"
      (toString cfg.prune.minAgeDays)
    ]
    ++ lib.optionals cfg.prune.dryRun [ "--dry-run" ]
    ++ lib.optionals cfg.prune.skipArchivedSessions [ "--skip-archived-sessions" ]
  );
  runAgent = pkgs.writeShellScript "codex-session-archive-agent" ''
    set -euo pipefail
    exec ${cfg.package}/bin/codex-session-archive-agent ${runAgentArgs}
  '';
  runPrune = pkgs.writeShellScript "codex-session-archive-prune" ''
    set -euo pipefail
    exec ${cfg.package}/bin/codex-session-archive-agent ${runPruneArgs}
  '';
in
{
  options.services.codexSessionArchiveAgent = {
    enable = lib.mkEnableOption "Codex session archive local push agent";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.codex-session-archive-agent;
      defaultText = lib.literalExpression "codex-sessions.packages.\${system}.codex-session-archive-agent";
      description = "Package providing the codex-session-archive-agent binary.";
    };

    serverUrl = lib.mkOption {
      type = lib.types.str;
      example = "http://srv-2:8787";
      description = "Base URL for the central archive server.";
    };

    ingestTokenFile = lib.mkOption {
      type = lib.types.either lib.types.path lib.types.str;
      description = "Path to a file containing the ingestion bearer token.";
    };

    codexHome = lib.mkOption {
      type = lib.types.str;
      default = "${config.home.homeDirectory}/.codex";
      description = "CODEX_HOME directory to scan.";
    };

    intervalSeconds = lib.mkOption {
      type = lib.types.ints.positive;
      default = 30;
      description = "Scan interval for watch mode.";
    };

    maxLinesPerBatch = lib.mkOption {
      type = lib.types.ints.positive;
      default = 5000;
      description = "Maximum complete JSONL records to upload per ingest request.";
    };

    requestTimeoutSeconds = lib.mkOption {
      type = lib.types.ints.positive;
      default = 600;
      description = "HTTP request timeout for cursor and ingest requests.";
    };

    prune = {
      enable = lib.mkEnableOption "scheduled pruning of fully archived local Codex rollout files";

      minAgeDays = lib.mkOption {
        type = lib.types.ints.positive;
        default = 30;
        description = "Minimum file age in days before pruning a fully archived rollout file.";
      };

      intervalSeconds = lib.mkOption {
        type = lib.types.ints.positive;
        default = 86400;
        description = "How often to run prune.";
      };

      dryRun = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Whether scheduled prune should log candidates without deleting them.";
      };

      skipArchivedSessions = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Skip files under archived_sessions when pruning.";
      };
    };
  };

  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      {
        home.packages = [ cfg.package ];
      }

      (lib.mkIf pkgs.stdenv.hostPlatform.isDarwin {
        home.file."Library/Logs/codex-session-archive/.keep".text = "";

        launchd.agents.codex-session-archive-agent = {
          enable = true;
          config = {
            ProgramArguments = [ (toString runAgent) ];
            RunAtLoad = true;
            KeepAlive = {
              Crashed = true;
              SuccessfulExit = false;
            };
            StandardOutPath = "${logDir}/agent.log";
            StandardErrorPath = "${logDir}/agent-error.log";
            WorkingDirectory = config.home.homeDirectory;
            ProcessType = "Background";
          };
        };

        launchd.agents.codex-session-archive-prune = lib.mkIf cfg.prune.enable {
          enable = true;
          config = {
            ProgramArguments = [ (toString runPrune) ];
            RunAtLoad = true;
            StartInterval = cfg.prune.intervalSeconds;
            StandardOutPath = "${logDir}/prune.log";
            StandardErrorPath = "${logDir}/prune-error.log";
            WorkingDirectory = config.home.homeDirectory;
            ProcessType = "Background";
          };
        };
      })

      (lib.mkIf pkgs.stdenv.hostPlatform.isLinux {
        systemd.user.services.codex-session-archive-agent = {
          Unit = {
            Description = "Codex Session Archive Agent";
            After = [ "network-online.target" ];
          };
          Service = {
            ExecStart = toString runAgent;
            Restart = "on-failure";
            RestartSec = "5s";
            WorkingDirectory = config.home.homeDirectory;
          };
          Install.WantedBy = [ "default.target" ];
        };

        systemd.user.services.codex-session-archive-prune = lib.mkIf cfg.prune.enable {
          Unit = {
            Description = "Codex Session Archive Prune";
            After = [ "network-online.target" ];
          };
          Service = {
            ExecStart = toString runPrune;
            Type = "oneshot";
            WorkingDirectory = config.home.homeDirectory;
          };
        };

        systemd.user.timers.codex-session-archive-prune = lib.mkIf cfg.prune.enable {
          Unit.Description = "Run Codex Session Archive prune";
          Timer = {
            OnBootSec = "5m";
            OnUnitActiveSec = "${toString cfg.prune.intervalSeconds}s";
            Unit = "codex-session-archive-prune.service";
          };
          Install.WantedBy = [ "timers.target" ];
        };
      })
    ]
  );
}
