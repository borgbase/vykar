param(
    [Parameter(Mandatory = $true)]
    [string]$Version
)

$ErrorActionPreference = "Stop"

$target = "x86_64-pc-windows-msvc"
$distDir = "dist"
$archive = "vykar-$Version-$target.zip"

rustup target add $target

if (Test-Path $distDir) {
    Remove-Item $distDir -Recurse -Force
}
New-Item -ItemType Directory -Path $distDir | Out-Null

cargo build --release --target $target -p vykar-cli
Compress-Archive -Path "target/$target/release/vykar.exe" -DestinationPath "$distDir/$archive" -Force

$hash = (Get-FileHash -Algorithm SHA256 "$distDir/$archive").Hash.ToLower()
"$hash  $archive" | Set-Content "$distDir/$archive.sha256"
"$hash  $archive" | Set-Content "$distDir/SHA256SUMS"

Write-Host ""
Write-Host "Created $distDir/$archive"
Write-Host "Next steps:"
Write-Host "  git tag -a $Version -m `"Release $Version`""
Write-Host "  git push origin main --follow-tags"
Write-Host "  gh release create $Version --title `"vykar $Version`" --notes `"Initial release`" $distDir/*"
