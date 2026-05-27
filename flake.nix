{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
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
        archiveAgentPackage = mkArchiveCrate pkgs "archive-agent";
        archivePackage = pkgs.symlinkJoin {
          name = "codex-session-archive-0.1.0";
          paths = [
            archiveServerPackage
            archiveAgentPackage
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
      in
      {
        packages = {
          default = archivePackage;
          codex-session-archive = archivePackage;
          archive-server = archiveServerPackage;
          archive-agent = archiveAgentPackage;
          archive-server-image = archiveServerImage;
        };

        apps = {
          archive-server = {
            type = "app";
            program = "${archiveServerPackage}/bin/archive-server";
            meta.description = "Run the Codex session archive HTTP server";
          };
          archive-agent = {
            type = "app";
            program = "${archiveAgentPackage}/bin/archive-agent";
            meta.description = "Run the Codex session archive local import agent";
          };
        };

        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [
            cargo
            cargo-nextest
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
      }
    )
    // {
      nixosModules.archive-server = import ./nix/nixos/archive-server.nix { inherit self; };
      homeManagerModules.archive-agent = import ./nix/home-manager/archive-agent.nix { inherit self; };
    };
}
