#!/usr/bin/env bash
# Sign and notarize macOS binaries (app bundles and standalone CLI tools).
#
# Required environment variables:
#   CERTIFICATE_NAME    - Developer ID Application identity (for codesign)
#   APPLE_API_KEY       - Path to App Store Connect .p8 key file (for notarization)
#   APPLE_API_KEY_ID    - API key ID
#   APPLE_API_ISSUER_ID - Issuer ID from App Store Connect
#
# Usage: ./scripts/macos-sign.sh "path/to/App.app" path/to/vykar path/to/vykar-server

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <item> [item ...]"
    echo "  Items can be .app bundles or standalone binaries."
    exit 1
fi

# --- Sign each item ---
for item in "$@"; do
    if [[ -d "$item" ]]; then
        echo "==> Signing app bundle ${item}..."
        codesign --verbose --force --sign "$CERTIFICATE_NAME" \
            --timestamp --deep --options runtime \
            --entitlements "${SCRIPT_DIR}/macos-entitlements.plist" \
            "$item"
    elif [[ -f "$item" ]]; then
        echo "==> Signing binary ${item}..."
        codesign --verbose --force --sign "$CERTIFICATE_NAME" \
            --timestamp --options runtime \
            --entitlements "${SCRIPT_DIR}/macos-entitlements.plist" \
            "$item"
    else
        echo "Error: ${item} does not exist"
        exit 1
    fi

    echo "==> Verifying ${item}..."
    codesign --verify --verbose "$item"
done

# --- Notarize (single submission for all items) ---
ZIP_PATH="$(mktemp -t vykar-notarize-XXXXXX).zip"
STAGE_DIR="$(mktemp -d -t vykar-notarize-stage-XXXXXX)"
echo "==> Creating zip for notarization..."
for item in "$@"; do
    cp -R "$item" "$STAGE_DIR/"
done
ditto -c -k --keepParent "$STAGE_DIR" "$ZIP_PATH"
rm -rf "$STAGE_DIR"

echo "==> Submitting for notarization..."
xcrun notarytool submit "$ZIP_PATH" \
    --key "$APPLE_API_KEY" \
    --key-id "$APPLE_API_KEY_ID" \
    --issuer "$APPLE_API_ISSUER_ID" \
    --wait --timeout 10m

rm -f "$ZIP_PATH"

# --- Staple (only .app bundles support stapling) ---
for item in "$@"; do
    if [[ -d "$item" && "$item" == *.app ]]; then
        echo "==> Stapling notarization ticket to ${item}..."
        xcrun stapler staple "$item"
    fi
done

echo "==> Done. All items signed and notarized."
