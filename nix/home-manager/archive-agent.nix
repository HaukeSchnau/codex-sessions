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
  runAgent = pkgs.writeShellScript "codex-session-archive-agent" ''
    set -euo pipefail
    token="$(cat ${lib.escapeShellArg cfg.ingestTokenFile})"
    exec ${cfg.package}/bin/archive-agent watch \
      --server ${lib.escapeShellArg cfg.serverUrl} \
      --token "$token" \
      --codex-home ${lib.escapeShellArg cfg.codexHome} \
      --max-lines-per-batch ${toString cfg.maxLinesPerBatch} \
      --request-timeout-seconds ${toString cfg.requestTimeoutSeconds} \
      --interval-seconds ${toString cfg.intervalSeconds} \
      --quiet
  '';
in
{
  options.services.codexSessionArchiveAgent = {
    enable = lib.mkEnableOption "Codex session archive local push agent";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.archive-agent;
      defaultText = lib.literalExpression "codex-sessions.packages.\${system}.archive-agent";
      description = "Package providing the archive-agent binary.";
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
      })
    ]
  );
}
