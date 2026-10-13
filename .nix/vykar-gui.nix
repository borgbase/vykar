{
  pkgs,
  craneLib,
  common,
}:
craneLib.buildPackage (
  common.argsGui
  // {
    cargoArtifacts = common.cargoArtifactsGui;
    pname = "vykar-gui";
    inherit (common) version;

    cargoExtraArgs = "--package vykar-gui";

    nativeBuildInputs =
      common.nativeBuildInputsCore
      ++ common.nativeBuildInputsGui
      ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.makeWrapper ];

    postInstall = pkgs.lib.optionalString pkgs.stdenv.isLinux ''
      wrapProgram $out/bin/vykar-gui \
        --prefix LD_LIBRARY_PATH : ${
          pkgs.lib.makeLibraryPath [
            pkgs.libGL
            pkgs.wayland
            pkgs.libxkbcommon
            pkgs.fontconfig
          ]
        } \
        --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.zenity ]}
    '';

    meta = {
      description = "Vykar backup tool GUI application";
      homepage = "https://vykar.borgbase.com";
      license = pkgs.lib.licenses.gpl3Only;
      mainProgram = "vykar-gui";
    };
  }
)
