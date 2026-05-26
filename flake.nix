{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
        imageSystem =
          if lib.hasSuffix "-darwin" system then
            lib.replaceStrings [ "-darwin" ] [ "-linux" ] system
          else
            system;
        imagePkgs = import nixpkgs { system = imageSystem; };
        archiveSource = lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            let
              rel = lib.removePrefix ((toString ./.) + "/") (toString path);
            in
            !(
              rel == ".env"
              || rel == "Dockerfile"
              || rel == ".dockerignore"
              || rel == "result"
              || lib.hasPrefix "result-" rel
              || lib.hasPrefix ".git/" rel
              || lib.hasPrefix ".jj/" rel
              || lib.hasPrefix ".sops/" rel
              || lib.hasPrefix ".direnv/" rel
              || lib.hasPrefix "target/" rel
              || lib.hasPrefix "tmp/" rel
            );
        };
        mkArchivePackage =
          targetPkgs:
          targetPkgs.rustPlatform.buildRustPackage {
            pname = "codex-session-archive";
            version = "0.1.0";
            src = archiveSource;
            cargoLock.lockFile = ./Cargo.lock;
            doCheck = true;
          };
        archivePackage = mkArchivePackage pkgs;
        mkArchiveServerImage =
          targetPkgs:
          let
            targetArchivePackage = mkArchivePackage targetPkgs;
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
        archiveServerImage = mkArchiveServerImage imagePkgs;
      in
      {
        packages = {
          default = archivePackage;
          codex-session-archive = archivePackage;
          archive-server = archivePackage;
          archive-agent = archivePackage;
          archive-server-image = archiveServerImage;
        };

        apps = {
          archive-server = {
            type = "app";
            program = "${archivePackage}/bin/archive-server";
          };
          archive-agent = {
            type = "app";
            program = "${archivePackage}/bin/archive-agent";
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
    );
}
