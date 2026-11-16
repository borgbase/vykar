#!/bin/sh
set -eu

REPO="borgbase/vykar"
GITHUB_API="https://api.github.com/repos/${REPO}/releases/latest"
GITHUB_DOWNLOAD="https://github.com/${REPO}/releases/download"
DEFAULT_INSTALL_DIR="/usr/local/bin"
BINARY_NAME="vykar"
TMPDIR_CLEANUP=""

# Minimum glibc required by the GNU build.
# Bump these when the CI runner changes (pinned in .github/workflows/release.yml).
# Per-architecture: x86_64 uses Ubuntu 24.04 (glibc 2.39),
#                   aarch64 uses Ubuntu 22.04 (glibc 2.35).
# MIN_GLIBC_MINOR is set per-architecture in detect_platform().
MIN_GLIBC_MAJOR=2
MIN_GLIBC_MINOR=39

# --- Utilities -----------------------------------------------------------

log() { printf '%s\n' "$*"; }

die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
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
                    MIN_GLIBC_MINOR=39    # Ubuntu 24.04
                    detect_linux_libc
                    TARGET="x86_64-unknown-linux-${LIBC}"
                    ;;
                aarch64|arm64)
                    MIN_GLIBC_MINOR=35    # Ubuntu 22.04
                    detect_linux_libc
                    TARGET="aarch64-unknown-linux-${LIBC}"
                    ;;
                *)       die "unsupported Linux architecture: $arch (only x86_64 and aarch64 builds are available)" ;;
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

# --- Download & extract ---------------------------------------------------

download_and_extract() {
    local archive

    archive="vykar-${VERSION}-${TARGET}.tar.gz"
    TMPDIR_CLEANUP="$(mktemp -d)"
    tmpdir="$TMPDIR_CLEANUP"
    trap 'rm -rf "$TMPDIR_CLEANUP"' EXIT

    log "Downloading ${archive}..."
    curl -fSL -o "${tmpdir}/${archive}" \
        "${GITHUB_DOWNLOAD}/${VERSION}/${archive}"

    log "Extracting ${BINARY_NAME}..."
    tar xzf "${tmpdir}/${archive}" -C "$tmpdir" "$BINARY_NAME"

    EXTRACTED="${tmpdir}/${BINARY_NAME}"
}

# --- Code signature check (macOS) -----------------------------------------

# macOS kills binaries whose signature does not validate, with no diagnostic
# beyond "Killed: 9". Official builds are signed and notarized, so a failure
# here means the file changed between GitHub and this machine.
verify_signature() {
    [ "$(uname -s)" = "Darwin" ] || return 0
    command -v codesign >/dev/null 2>&1 || return 0
    codesign --verify --strict "$1" >/dev/null 2>&1 && return 0

    cat >&2 <<EOF

Error: the vykar binary at $1 has an invalid code signature.
macOS will refuse to run it.

Official macOS builds are signed and notarized, so the file was altered after
download — antivirus software and intercepting HTTPS proxies are the usual cause.

Do NOT work around this with 'codesign --sign -'. Ad-hoc signing does make the
binary run, but it replaces vykar's stable signing identity with one that changes
on every update, which breaks Full Disk Access for scheduled backups.

Retry on a direct connection, or download the release manually from
https://github.com/borgbase/vykar/releases and check it with:
  codesign --verify --strict --verbose=2 ./vykar
EOF
    exit 1
}

# --- Install --------------------------------------------------------------

install_binary() {
    local dest="${INSTALL_DIR}/${BINARY_NAME}"
    local staged="${dest}.new"

    verify_signature "$EXTRACTED"

    # Stage inside the destination directory, then rename. rename(2) is atomic
    # and swaps the inode rather than writing through a binary that may be
    # running, which would fail partway and leave an unrunnable file behind.
    if [ -w "$INSTALL_DIR" ]; then
        cp "$EXTRACTED" "$staged"
        chmod 755 "$staged"
        mv -f "$staged" "$dest"
    else
        log "Installing to ${INSTALL_DIR} requires elevated permissions. Using sudo..."
        sudo cp "$EXTRACTED" "$staged"
        sudo chmod 755 "$staged"
        sudo mv -f "$staged" "$dest"
    fi

    verify_signature "$dest"

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
    detect_platform
    log "Platform: ${TARGET}"

    fetch_latest_version
    log "Latest version: ${VERSION}"
    log ""

    prompt_install_dir

    log "Installing vykar ${VERSION} to ${INSTALL_DIR}"
    log ""

    download_and_extract
    install_binary
    log ""

    prompt_config

    if [ "$(uname -s)" = "Darwin" ]; then
        log ""
        log "macOS note: nightly backups need Full Disk Access, which macOS grants"
        log "reliably only to bundled executables — not to a bare binary on PATH."
        log "Setup: https://vykar.borgbase.com/daemon#launchd-macos"
    fi

    log ""
    log "Done. Run 'vykar config' to create a starter configuration."
}

main
