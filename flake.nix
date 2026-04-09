{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";

    # Pre-built Skia binaries for vykar-gui (must match skia-bindings version in Cargo.lock).
    #
    # To update after a skia-bindings bump:
    #   1. Find the new version tag at https://github.com/rust-skia/skia-binaries/releases that matches the skia-bindings version in Cargo.lock.
    #   2. Update the version (e.g. 0.90.0) and binary hash in each URL below.
    #      The hash component in the filename corresponds to the crate source hash and changes with each skia-bindings release.
    #   3. Run `nix flake update` to re-fetch and update the locks.
    skia-binaries-x86_64-linux = {
      type = "file";
      url = "https://github.com/rust-skia/skia-binaries/releases/download/0.90.0/skia-binaries-da4579b39b75fa2187c5-x86_64-unknown-linux-gnu-gl-pdf-textlayout-vulkan.tar.gz";
      flake = false;
    };
    skia-binaries-aarch64-linux = {
      type = "file";
      url = "https://github.com/rust-skia/skia-binaries/releases/download/0.90.0/skia-binaries-da4579b39b75fa2187c5-aarch64-unknown-linux-gnu-gl-pdf-textlayout-vulkan.tar.gz";
      flake = false;
    };
    skia-binaries-aarch64-darwin = {
      type = "file";
      url = "https://github.com/rust-skia/skia-binaries/releases/download/0.90.0/skia-binaries-da4579b39b75fa2187c5-aarch64-apple-darwin-gl-pdf-textlayout-vulkan.tar.gz";
      flake = false;
    };
  };

  outputs =
    { self, ... }@inputs:
    inputs.flake-utils.lib.eachSystem
      [
        inputs.flake-utils.lib.system.x86_64-linux
        inputs.flake-utils.lib.system.aarch64-linux
        inputs.flake-utils.lib.system.aarch64-darwin
      ]
      (
        system:
        let
          pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [ inputs.rust-overlay.overlays.default ];
          };

          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "rustfmt"
            ];
          };

          craneLib = (inputs.crane.mkLib pkgs).overrideToolchain rustToolchain;

          common = import ./.nix/common.nix {
            inherit pkgs craneLib;
            skiaBinaries = inputs."skia-binaries-${system}";
          };
        in
        {
          packages = {
            vykar = import ./.nix/vykar-cli.nix { inherit pkgs craneLib common; };
            vykar-server = import ./.nix/vykar-server.nix { inherit pkgs craneLib common; };
            vykar-gui = import ./.nix/vykar-gui.nix { inherit pkgs craneLib common; };
            default = self.packages.${system}.vykar;
          };

          devShells.default = import ./.nix/devshell.nix {
            inherit pkgs rustToolchain common;
          };
        }
      );
}
