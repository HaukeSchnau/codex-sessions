{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    nix-infra-modules = {
      url = "github:HaukeSchnau/nix-infra-modules";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      nix-infra-modules,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
        projectDescriptor = nix-infra-modules.lib.projectDescriptor.load {
          path = ./project.json;
          expectedProject = "codex-sessions";
        };
        archiveSource = lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            let
              rel = lib.removePrefix ((toString ./.) + "/") (toString path);
            in
            rel == "Cargo.lock" || rel == "Cargo.toml" || rel == "crates" || lib.hasPrefix "crates/" rel;
        };
        mkArchiveCrate =
          targetPkgs: crateName:
          targetPkgs.rustPlatform.buildRustPackage {
            pname = crateName;
            version = "0.1.0";
            src = archiveSource;
            cargoLock.lockFile = ./Cargo.lock;
            cargoBuildFlags = [
              "-p"
              crateName
            ];
            cargoTestFlags = [
              "-p"
              crateName
            ];
            doCheck = true;
          };
        archiveServerPackage = mkArchiveCrate pkgs "archive-server";
        codexSessionArchiveAgentPackage = mkArchiveCrate pkgs "codex-session-archive-agent";
        archivePackage = pkgs.symlinkJoin {
          name = "codex-session-archive-0.1.0";
          paths = [
            archiveServerPackage
            codexSessionArchiveAgentPackage
          ];
        };
        mkArchiveServerImage =
          targetPkgs:
          let
            targetArchivePackage = mkArchiveCrate targetPkgs "archive-server";
            imageRoot = targetPkgs.buildEnv {
              name = "codex-session-archive-image-root";
              paths = [
                targetArchivePackage
                targetPkgs.cacert
                targetPkgs.coreutils
                targetPkgs.curl
              ];
              pathsToLink = [
                "/bin"
                "/etc"
              ];
            };
          in
          if targetPkgs.stdenv.isLinux then
            targetPkgs.dockerTools.buildLayeredImage {
              name = "codex-sessions-archive-server";
              tag = "nix";
              contents = [ imageRoot ];
              config = {
                Cmd = [ "/bin/archive-server" ];
                Env = [
                  "PATH=/bin"
                  "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
                ];
                ExposedPorts = {
                  "8787/tcp" = { };
                };
                User = "65532:65532";
                WorkingDir = "/";
              };
            }
          else
            targetPkgs.runCommand "archive-server-image-linux-only" { } ''
              cat > "$out" <<'EOF'
              archive-server-image can only be built by Nix for a Linux system.
              On non-Linux hosts, use a Linux Nix builder or run one of:
                nix build .#packages.aarch64-linux.archive-server-image
                nix build .#packages.x86_64-linux.archive-server-image
              EOF
              exit 1
            '';
        archiveServerImage = mkArchiveServerImage pkgs;
        developmentRuntime = import ./nix/project-runtime.nix {
          inherit
            lib
            nix-infra-modules
            pkgs
            ;
        };
      in
      {
        packages = {
          default = archivePackage;
          codex-session-archive = archivePackage;
          archive-server = archiveServerPackage;
          codex-session-archive-agent = codexSessionArchiveAgentPackage;
          archive-agent = codexSessionArchiveAgentPackage;
          archive-server-image = archiveServerImage;
          projectRuntime = developmentRuntime.package;
        };

        apps = developmentRuntime.apps // {
          archive-server = {
            type = "app";
            program = "${archiveServerPackage}/bin/archive-server";
            meta.description = "Run the Codex session archive HTTP server";
          };
          codex-session-archive-agent = {
            type = "app";
            program = "${codexSessionArchiveAgentPackage}/bin/codex-session-archive-agent";
            meta.description = "Run the Codex session archive local import agent";
          };
          archive-agent = {
            type = "app";
            program = "${codexSessionArchiveAgentPackage}/bin/codex-session-archive-agent";
            meta.description = "Compatibility alias for codex-session-archive-agent";
          };
        };

        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [
            cargo
            cargo-nextest
            cargo-watch
            clippy
            age
            docker-compose
            jq
            openssl
            pkg-config
            postgresql
            rustc
            rustfmt
            sops
            sqlx-cli
          ];
        };

        checks = developmentRuntime.checks // {
          projectDescriptor =
            pkgs.runCommand "codex-sessions-project-descriptor"
              {
                descriptor = pkgs.writeText "project.json" (builtins.toJSON projectDescriptor);
                nativeBuildInputs = [ pkgs.jq ];
              }
              ''
                jq -e '
                  .schemaVersion == 2
                  and .project == "codex-sessions"
                  and .development.workloads.postgres.action == "postgres"
                  and .development.workloads.web.action == "web"
                  and .development.workloads.web.dependsOn == ["postgres"]
                  and .development.workloads.web.secrets == ["ingest-token", "openai-api-key", "read-token"]
                  and .development.endpoints.postgres.protocol == "tcp"
                  and (.development.endpoints.postgres.health | has("paths") | not)
                  and .development.endpoints.web.protocol == "http"
                  and .development.endpoints.web.health.paths == ["/readyz"]
                  and .release.package == "archive-server"
                  and .release.executable == "archive-server"
                  and .release.health.paths == ["/healthz"]
                  and (.secrets | keys) == ["ingest-token", "openai-api-key", "read-token"]
                ' "$descriptor" >/dev/null
                jq -e '.release == {
                  package: "archive-server",
                  executable: "archive-server",
                  health: {paths: ["/healthz"]}
                }' ${./project.json} >/dev/null
                touch "$out"
              '';
        };
      }
    )
    // {
      lib.project = builtins.fromJSON (builtins.readFile ./project.json);
      nixosModules.archive-server = import ./nix/nixos/archive-server.nix { inherit self; };
      homeManagerModules.codex-session-archive-agent = import ./nix/home-manager/archive-agent.nix {
        inherit self;
      };
      homeManagerModules.archive-agent = self.homeManagerModules.codex-session-archive-agent;
    };
}
