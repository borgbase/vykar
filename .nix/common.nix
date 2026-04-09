{
  pkgs,
  craneLib,
  skiaBinaries,
}:
let
  src = pkgs.lib.cleanSourceWith {
    src = craneLib.path ./..;
    filter =
      path: type:
      (builtins.match ".*\\.(slint|png)$" path != null) || (craneLib.filterCargoSources path type);
  };

  nativeBuildInputsCore = [
    pkgs.pkg-config
    pkgs.cmake
    pkgs.clang
    pkgs.perl
  ];

  buildInputsCore = [ pkgs.zstd ];

  nativeBuildInputsGui = [
    pkgs.python3
    pkgs.ninja
  ];

  buildInputsGui =
    with pkgs;
    [
      # Skia renderer deps
      fontconfig
      freetype
      expat
      libGL
    ]
    ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
      # Wayland (winit + Slint)
      wayland
      wayland-protocols
      libxkbcommon

      # X11 (winit)
      libx11
      libxcursor
      libxrandr
      libxi
      libxcb

      # GTK3 stack
      gtk3
      glib
      pango
      cairo
      gdk-pixbuf
      atk

      # Tray icon
      xdotool
      libayatana-appindicator
    ];

  envCore = {
    # Tell aws-lc-rs (used by russh/SFTP) to use cmake
    AWS_LC_SYS_CMAKE_BUILDER = "1";
  };

  envGui = envCore // {
    SKIA_NINJA_COMMAND = "${pkgs.ninja}/bin/ninja";
    SKIA_BINARIES_URL = "file://${skiaBinaries}";
  };

  argsCore = {
    inherit src;
    strictDeps = true;
    nativeBuildInputs = nativeBuildInputsCore;
    buildInputs = buildInputsCore;
    env = envCore;
  };

  argsGui = {
    inherit src;
    strictDeps = true;
    nativeBuildInputs = nativeBuildInputsCore ++ nativeBuildInputsGui;
    buildInputs = buildInputsCore ++ buildInputsGui;
    env = envGui;
  };

  cargoArtifactsCore = craneLib.buildDepsOnly (
    argsCore
    // {
      pname = "vykar-core-deps";
      version = (pkgs.lib.importTOML ../crates/vykar-core/Cargo.toml).package.version;
      cargoExtraArgs = "--workspace --exclude vykar-gui";
    }
  );

  cargoArtifactsGui = craneLib.buildDepsOnly (
    argsGui
    // {
      pname = "vykar-gui-deps";
      version = (pkgs.lib.importTOML ../crates/vykar-gui/Cargo.toml).package.version;
    }
  );
in
{
  inherit
    src

    nativeBuildInputsCore
    nativeBuildInputsGui

    buildInputsCore
    buildInputsGui

    envCore
    envGui

    argsCore
    argsGui

    cargoArtifactsCore
    cargoArtifactsGui
    ;
}
