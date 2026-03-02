#!/bin/sh
set -eu

REPO="borgbase/vykar"
GITHUB_API="https://api.github.com/repos/${REPO}/releases/latest"
GITHUB_DOWNLOAD="https://github.com/${REPO}/releases/download"
DEFAULT_INSTALL_DIR="/usr/local/bin"
BINARY_NAME="vykar"
TMPDIR_CLEANUP=""

# Minimum glibc required by the GNU build.
# Bump this when the CI runner changes (pinned in .github/workflows/release.yml).
# Current: Ubuntu 24.04 ships glibc 2.39.
MIN_GLIBC_MAJOR=2
MIN_GLIBC_MINOR=39

# --- Utilities -----------------------------------------------------------

log() { printf '%s\n' "$*"; }

die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

find_sha_tool() {
    if command -v sha256sum >/dev/null 2>&1; then
        SHA_TOOL="sha256sum"
    elif command -v shasum >/dev/null 2>&1; then
        SHA_TOOL="shasum"
    elif command -v openssl >/dev/null 2>&1; then
        SHA_TOOL="openssl"
    else
        die "no SHA-256 tool found (need sha256sum, shasum, or openssl)"
    fi
}

sha256_of() {
    local file="$1"
    case "$SHA_TOOL" in
        sha256sum) sha256sum "$file" | awk '{print $1}' ;;
        shasum)    shasum -a 256 "$file" | awk '{print $1}' ;;
        openssl)   openssl dgst -sha256 "$file" | awk '{print $NF}' ;;
    esac
}

# --- Libc detection (Linux only) ------------------------------------------

detect_linux_libc() {
    # Default to gnu; downgrade to musl if needed.
    LIBC="gnu"

    # 1. Detect musl-based systems (Alpine, Void, etc.)
    #    ldd may not exist (busybox-only), so guard the check.
    if command -v ldd >/dev/null 2>&1; then
        case "$(ldd --version 2>&1 || true)" in
            *musl*)
                LIBC="musl"
                log "Detected musl libc, using musl build"
                return
                ;;
        esac
    fi

    # 2. Probe glibc version — two methods, both guarded.
    glibc_ver=""
    if command -v ldd >/dev/null 2>&1; then
        glibc_ver=$(ldd --version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+$' || true)
    fi
    if [ -z "$glibc_ver" ]; then
        glibc_ver=$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}' || true)
    fi

    # 3. Compare with integer arithmetic (POSIX-safe, no lexical traps).
    if [ -n "$glibc_ver" ]; then
        maj=$(echo "$glibc_ver" | cut -d. -f1)
        min=$(echo "$glibc_ver" | cut -d. -f2)

        if [ "$maj" -ge 0 ] 2>/dev/null && [ "$min" -ge 0 ] 2>/dev/null; then
            if [ "$maj" -gt "$MIN_GLIBC_MAJOR" ] || \
               { [ "$maj" -eq "$MIN_GLIBC_MAJOR" ] && [ "$min" -ge "$MIN_GLIBC_MINOR" ]; }; then
                log "Detected glibc ${glibc_ver} (>= ${MIN_GLIBC_MAJOR}.${MIN_GLIBC_MINOR}), using GNU build"
                return
            else
                LIBC="musl"
                log "Detected glibc ${glibc_ver} (< ${MIN_GLIBC_MAJOR}.${MIN_GLIBC_MINOR}), using statically-linked musl build"
                return
            fi
        fi
    fi

    # 4. Could not determine version — safe fallback.
    LIBC="musl"
    log "Could not detect glibc version, using statically-linked musl build"
}

# --- Platform detection ---------------------------------------------------

detect_platform() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux)
            case "$arch" in
                x86_64)
                    detect_linux_libc
                    TARGET="x86_64-unknown-linux-${LIBC}"
                    ;;
                *)       die "unsupported Linux architecture: $arch (only x86_64 builds are available)" ;;
            esac
            ;;
        Darwin)
            case "$arch" in
                arm64|aarch64) TARGET="aarch64-apple-darwin" ;;
                *)             die "unsupported macOS architecture: $arch (only Apple Silicon builds are available)" ;;
            esac
            ;;
        *)
            die "unsupported operating system: $os (only Linux and macOS are supported)"
            ;;
    esac
}

# --- Version fetch --------------------------------------------------------

fetch_latest_version() {
    VERSION=$(curl -fsSL "$GITHUB_API" | grep '"tag_name"' | sed 's/.*"tag_name" *: *"\([^"]*\)".*/\1/')
    if [ -z "$VERSION" ]; then
        die "could not determine latest version from GitHub API"
    fi
}

# --- Interactive prompts --------------------------------------------------

prompt_install_dir() {
    INSTALL_DIR="$DEFAULT_INSTALL_DIR"

    if [ -t 0 ]; then
        printf 'Install location [%s]: ' "$DEFAULT_INSTALL_DIR"
        read -r user_dir || true
        if [ -n "$user_dir" ]; then
            INSTALL_DIR="$user_dir"
        fi
    fi

    # Expand ~ manually (shell does not expand ~ in variables)
    case "$INSTALL_DIR" in
        "~/"*) INSTALL_DIR="$HOME/${INSTALL_DIR#"~/"}" ;;
        "~")   INSTALL_DIR="$HOME" ;;
    esac

    # Ensure the directory exists
    if [ ! -d "$INSTALL_DIR" ]; then
        mkdir -p "$INSTALL_DIR" 2>/dev/null || \
            sudo mkdir -p "$INSTALL_DIR" || \
            die "could not create directory: $INSTALL_DIR"
    fi
}

prompt_config() {
    if [ -t 0 ]; then
        printf 'Run "vykar config" to create a starter configuration? [Y/n]: '
        read -r answer || true
        case "$answer" in
            [nN]*) ;;
            *)     "${INSTALL_DIR}/${BINARY_NAME}" config ;;
        esac
    fi
}

# --- Download & verify ----------------------------------------------------

download_and_verify() {
    local archive checksum_file expected actual

    archive="vykar-${VERSION}-${TARGET}.tar.gz"
    TMPDIR_CLEANUP="$(mktemp -d)"
    tmpdir="$TMPDIR_CLEANUP"
    trap 'rm -rf "$TMPDIR_CLEANUP"' EXIT

    log "Downloading ${archive}..."
    curl -fSL -o "${tmpdir}/${archive}" \
        "${GITHUB_DOWNLOAD}/${VERSION}/${archive}"

    log "Downloading checksums..."
    checksum_file="${tmpdir}/SHA256SUMS"
    curl -fsSL -o "$checksum_file" \
        "${GITHUB_DOWNLOAD}/${VERSION}/SHA256SUMS"

    expected=$(grep "$archive" "$checksum_file" | awk '{print $1}')
    if [ -z "$expected" ]; then
        die "checksum for ${archive} not found in SHA256SUMS"
    fi

    log "Verifying checksum..."
    actual=$(sha256_of "${tmpdir}/${archive}")
    if [ "$expected" != "$actual" ]; then
        die "checksum mismatch!\n  expected: ${expected}\n  got:      ${actual}"
    fi

    log "Extracting ${BINARY_NAME}..."
    tar xzf "${tmpdir}/${archive}" -C "$tmpdir" "$BINARY_NAME"

    EXTRACTED="${tmpdir}/${BINARY_NAME}"
}

# --- Install --------------------------------------------------------------

install_binary() {
    local dest="${INSTALL_DIR}/${BINARY_NAME}"

    if [ -w "$INSTALL_DIR" ]; then
        cp "$EXTRACTED" "$dest"
        chmod 755 "$dest"
    else
        log "Installing to ${INSTALL_DIR} requires elevated permissions. Using sudo..."
        sudo cp "$EXTRACTED" "$dest"
        sudo chmod 755 "$dest"
    fi

    log ""
    log "Installed: $("$dest" --version)"
}

# --- Main -----------------------------------------------------------------

main() {
    log "vykar installer"
    log ""

    require_cmd curl
    require_cmd tar
    require_cmd mktemp
    find_sha_tool

    detect_platform
    log "Platform: ${TARGET}"

    fetch_latest_version
    log "Latest version: ${VERSION}"
    log ""

    prompt_install_dir

    log "Installing vykar ${VERSION} to ${INSTALL_DIR}"
    log ""

    download_and_verify
    install_binary
    log ""

    prompt_config

    log ""
    log "Done. Run 'vykar config' to create a starter configuration."
}

main
