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

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <item> [item ...]"
    echo "  Items can be .app bundles or standalone binaries."
    exit 1
fi

# No --entitlements: nothing in vykar needs to weaken the hardened runtime.
# The Slint/Skia GUI was verified to run with an empty entitlement set.
sign() {
    codesign --verbose --force --sign "$CERTIFICATE_NAME" \
        --timestamp --options runtime "$1"
}

# --- Sign each item ---
for item in "$@"; do
    if [[ -d "$item" ]]; then
        echo "==> Signing app bundle ${item}..."
        # Nested code must be signed before the bundle that seals it. --deep is
        # deprecated by Apple and skips binaries it does not recognize as helpers,
        # so walk Contents/MacOS explicitly and sign the main executable last.
        main_exe="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleExecutable' "${item}/Contents/Info.plist")"
        for nested in "${item}/Contents/MacOS/"*; do
            if [[ -f "$nested" && "$(basename "$nested")" != "$main_exe" ]]; then
                echo "  -> nested $(basename "$nested")"
                sign "$nested"
            fi
        done
        sign "$item"
        echo "==> Verifying ${item}..."
        codesign --verify --deep --strict --verbose "$item"
    elif [[ -f "$item" ]]; then
        echo "==> Signing binary ${item}..."
        sign "$item"
        echo "==> Verifying ${item}..."
        codesign --verify --strict --verbose "$item"
    else
        echo "Error: ${item} does not exist"
        exit 1
    fi
done

# --- Notarize (single submission for all items) ---
ZIP_PATH="$(mktemp -t vykar-notarize-XXXXXX).zip"
STAGE_DIR="$(mktemp -d -t vykar-notarize-stage-XXXXXX)"
SUBMIT_LOG="$(mktemp -t vykar-notarize-log-XXXXXX)"
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
    --wait --timeout 10m 2>&1 | tee "$SUBMIT_LOG"

# notarytool can exit 0 on a submission that finished processing but was
# rejected. Only "Accepted" means the binaries will run on a user's Mac.
if ! grep -q "status: Accepted" "$SUBMIT_LOG"; then
    echo "Error: notarization did not reach 'Accepted' status; refusing to ship." >&2
    rm -f "$ZIP_PATH" "$SUBMIT_LOG"
    exit 1
fi

rm -f "$ZIP_PATH" "$SUBMIT_LOG"

# --- Staple (only .app bundles support stapling) ---
# Bare CLI binaries cannot carry a stapled ticket; Gatekeeper checks those
# online. This is one reason the CLI is also shipped inside the app bundle.
for item in "$@"; do
    if [[ -d "$item" && "$item" == *.app ]]; then
        echo "==> Stapling notarization ticket to ${item}..."
        xcrun stapler staple "$item"
    fi
done

echo "==> Done. All items signed and notarized."
