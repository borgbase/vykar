#!/bin/bash
# Build an AppImage for vykar-gui.
#
# Usage: ./scripts/build-appimage.sh <path-to-vykar-gui-binary> [output-dir]
#
# Requirements on PATH: rsvg-convert (librsvg), linuxdeploy
# In CI, set APPIMAGE_EXTRACT_AND_RUN=1 to avoid the FUSE requirement.
set -euo pipefail

BINARY="${1:?Usage: build-appimage.sh <vykar-gui-binary> [output-dir]}"
OUTDIR="$(mkdir -p "${2:-.}" && cd "${2:-.}" && pwd)"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

APPDIR="$(mktemp -d)/AppDir"

# --- AppDir skeleton ---
mkdir -p "${APPDIR}/usr/bin"
mkdir -p "${APPDIR}/usr/share/icons/hicolor/256x256/apps"
mkdir -p "${APPDIR}/usr/share/applications"

cp "${BINARY}" "${APPDIR}/usr/bin/vykar-gui"
chmod +x "${APPDIR}/usr/bin/vykar-gui"

# --- Generate 256x256 icon from SVG (same source as macOS .icns) ---
SVG_SRC="${REPO_ROOT}/docs/public/images/logo-colored-gradient.svg"
ICON_OUT="${APPDIR}/usr/share/icons/hicolor/256x256/apps/vykar-gui.png"
rsvg-convert --width=256 --height=256 \
    "${SVG_SRC}" -o "${ICON_OUT}"

# --- Desktop file ---
cp "${REPO_ROOT}/crates/vykar-gui/linux/vykar-gui.desktop" \
   "${APPDIR}/usr/share/applications/vykar-gui.desktop"

# --- Package with linuxdeploy ---
# linuxdeploy handles: library bundling via ldd, AppRun creation,
# root-level .desktop/icon symlinks, .DirIcon, and rpath patching.
#
# Exclude GPU/driver libs that must come from the host system.
# linuxdeploy's built-in list already excludes glibc, libX11, libxcb,
# libwayland, etc.
#
# Run from a scratch directory so the output .AppImage is never already
# inside OUTDIR (mv onto itself is a fatal error).
WORKDIR="$(mktemp -d)"
pushd "${WORKDIR}" > /dev/null

linuxdeploy \
    --appdir "${APPDIR}" \
    --desktop-file "${APPDIR}/usr/share/applications/vykar-gui.desktop" \
    --icon-file "${ICON_OUT}" \
    --executable "${APPDIR}/usr/bin/vykar-gui" \
    --exclude-library "libGL*" \
    --exclude-library "libEGL*" \
    --exclude-library "libGLX*" \
    --exclude-library "libGLdispatch*" \
    --exclude-library "libvulkan*" \
    --exclude-library "libdrm*" \
    --exclude-library "libgbm*" \
    --exclude-library "libglapi*" \
    --output appimage

mv ./*.AppImage "${OUTDIR}/"
popd > /dev/null
rm -rf "${WORKDIR}"
