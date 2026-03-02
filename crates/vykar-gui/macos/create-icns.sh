#!/bin/bash
# Generate AppIcon.icns from a source image (SVG or PNG) using macOS built-in tools.
# SVG input requires rsvg-convert (brew install librsvg).
# Usage: ./create-icns.sh <source> <output.icns>
set -euo pipefail

SRC="${1:?Usage: create-icns.sh <source.svg|png> <output.icns>}"
OUT="${2:?Usage: create-icns.sh <source.svg|png> <output.icns>}"

TMPDIR=$(mktemp -d)
ICONSET="$TMPDIR/AppIcon.iconset"
mkdir -p "$ICONSET"

# If SVG, rasterise to a 1024x1024 PNG with margin for macOS icon guidelines
if [[ "$SRC" == *.svg ]]; then
    if ! command -v rsvg-convert &>/dev/null; then
        echo "Error: rsvg-convert not found. Install with: brew install librsvg" >&2
        exit 1
    fi
    SRC_PNG="$TMPDIR/source.png"
    CANVAS=1024
    MARGIN_PCT=10
    RENDER_SIZE=$(( CANVAS * (100 - 2 * MARGIN_PCT) / 100 ))
    OFFSET=$(( (CANVAS - RENDER_SIZE) / 2 ))
    rsvg-convert \
        --page-width "$CANVAS" --page-height "$CANVAS" \
        -w "$RENDER_SIZE" -h "$RENDER_SIZE" \
        --top "$OFFSET" --left "$OFFSET" \
        "$SRC" -o "$SRC_PNG"
    SRC="$SRC_PNG"
fi

for SIZE in 16 32 128 256 512; do
    sips -z $SIZE $SIZE "$SRC" --out "$ICONSET/icon_${SIZE}x${SIZE}.png" >/dev/null
    DOUBLE=$((SIZE * 2))
    sips -z $DOUBLE $DOUBLE "$SRC" --out "$ICONSET/icon_${SIZE}x${SIZE}@2x.png" >/dev/null
done

iconutil -c icns "$ICONSET" -o "$OUT"
rm -rf "$TMPDIR"
echo "Created $OUT"
