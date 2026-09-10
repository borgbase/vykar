{
  pkgs,
  craneLib,
  common,
}:
craneLib.buildPackage (
  common.argsCore
  // {
    cargoArtifacts = common.cargoArtifactsCore;
    pname = "vykar";
    inherit (common) version;

    cargoExtraArgs = "--package vykar-cli";

    meta = {
      description = "Fast, encrypted, deduplicated backup tool";
      homepage = "https://vykar.borgbase.com";
      license = pkgs.lib.licenses.gpl3Only;
      mainProgram = "vykar";
    };
  }
)
