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
    version = (pkgs.lib.importTOML ../crates/vykar-gui/Cargo.toml).package.version;

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
            pkgs.gtk3
            pkgs.libayatana-appindicator
          ]
        } \
        --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.zenity ]} \
        --prefix XDG_DATA_DIRS : "${pkgs.gtk3}/share/gsettings-schemas/${pkgs.gtk3.name}:${pkgs.gsettings-desktop-schemas}/share/gsettings-schemas/${pkgs.gsettings-desktop-schemas.name}"
    '';

    meta = {
      description = "Vykar backup tool GUI application";
      homepage = "https://vykar.borgbase.com";
      license = pkgs.lib.licenses.gpl3Only;
      mainProgram = "vykar-gui";
    };
  }
)
