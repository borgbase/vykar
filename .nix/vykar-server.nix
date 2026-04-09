{
  pkgs,
  craneLib,
  common,
}:
craneLib.buildPackage (
  common.argsCore
  // {
    cargoArtifacts = common.cargoArtifactsCore;
    pname = "vykar-server";
    version = (pkgs.lib.importTOML ../crates/vykar-server/Cargo.toml).package.version;

    cargoExtraArgs = "--package vykar-server";

    meta = {
      description = "Vykar backup tool server";
      homepage = "https://vykar.borgbase.com";
      license = pkgs.lib.licenses.gpl3Only;
      mainProgram = "vykar-server";
    };
  }
)
