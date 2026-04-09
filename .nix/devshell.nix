{
  pkgs,
  rustToolchain,
  common,
}:
pkgs.mkShell {
  nativeBuildInputs =
    common.nativeBuildInputsCore
    ++ common.nativeBuildInputsGui
    ++ [
      rustToolchain

      # docs
      pkgs.mdbook
      pkgs.mdbook-mermaid

      # fuzzing
      pkgs.cargo-fuzz
    ];

  buildInputs = common.buildInputsCore ++ common.buildInputsGui;

  env = common.envCore // common.envGui;

  shellHook = ''
    echo "vykar ${(pkgs.lib.importTOML ../crates/vykar-core/Cargo.toml).package.version} dev shell — $(rustc --version)"
  '';
}
