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
      in
      {
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
