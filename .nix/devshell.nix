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
      pkgs.nodejs_24
      pkgs.pnpm

      # fuzzing
      pkgs.cargo-fuzz
    ];

  buildInputs = common.buildInputsCore ++ common.buildInputsGui;

  env = common.envCore // common.envGui;

  shellHook = ''
    echo "vykar ${common.version} dev shell — $(rustc --version)"
  '';
}
