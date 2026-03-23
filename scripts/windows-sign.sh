#!/usr/bin/env bash
# Sign Windows executables using Certum SimplySign cloud certificate on Linux.
#
# Runs SimplySign Desktop in a virtual X11 display (Xvfb), authenticates via
# xdotool + TOTP, then signs with jsign through the PKCS#11 module (via p11-kit).
#
# Required environment variables:
#   CERTUM_SIMPLYSIGN_USER - SimplySign account email
#   CERTUM_TOTP_SECRET     - Base32 TOTP secret from the otpauth:// URI
#   CERTUM_CERT_PEM        - Base64-encoded certificate PEM file
#
# Usage: ./scripts/windows-sign.sh file1.exe file2.exe ...

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <file> [file ...]"
    exit 1
fi

for var in CERTUM_SIMPLYSIGN_USER CERTUM_TOTP_SECRET CERTUM_CERT_PEM; do
    if [[ -z "${!var:-}" ]]; then
        echo "Error: $var is not set"
        exit 1
    fi
done

for f in "$@"; do
    if [[ ! -f "$f" ]]; then
        echo "Error: file not found: $f"
        exit 1
    fi
done

# --- Install dependencies ---

echo "==> Installing dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    xvfb xdotool oathtool osslsigncode \
    p11-kit opensc stalonetray \
    libpulse-mainloop-glib0 libxss1 libnss3 libxkbcommon0 \
    > /dev/null 2>&1
echo "==> Dependencies installed"

# --- Install SimplySign Desktop ---

SSD_URL="https://files.certum.eu/software/SimplySignDesktop/Linux-Ubuntu/2.9.13-9.4.2.0/SimplySignDesktop-2.9.13-9.4.2.0-x86_64-prod-ubuntu.bin"
SSD_DIR="/opt/SimplySignDesktop"

if [[ ! -d "$SSD_DIR" ]]; then
    echo "==> Downloading SimplySign Desktop..."
    curl -sSL -o /tmp/ssd-installer.bin "$SSD_URL"
    chmod +x /tmp/ssd-installer.bin

    echo "==> Extracting..."
    EXTRACT_DIR="/tmp/ssd-extract"
    /tmp/ssd-installer.bin --noexec --nox11 --target "$EXTRACT_DIR"
    rm -f /tmp/ssd-installer.bin

    sudo mkdir -p "$SSD_DIR"
    sudo cp -a "$EXTRACT_DIR"/SSD-*-dist/* "$SSD_DIR/" 2>/dev/null || true
    sudo cp -a "$EXTRACT_DIR"/SS-*-dist/* "$SSD_DIR/" 2>/dev/null || true
    rm -rf "$EXTRACT_DIR"
fi

SSD_EXE="$SSD_DIR/SimplySignDesktop_start"
PKCS11_SO=$(find "$SSD_DIR" -name "SimplySignPKCS*.so" -type f 2>/dev/null | head -1 || true)
echo "==> SimplySign Desktop: $SSD_EXE"
echo "==> PKCS#11 module: $PKCS11_SO"

# --- Configure ---

mkdir -p "$HOME/.config"
cat > "$HOME/.config/Unknown Organization.conf" <<'CONF'
[General]
CacheUserIdAtLogon=Yes
ShowLogonDialogAfterApplicationStartup=Yes
ShowLogonDialogWhenAnyAppRequestsAccess=Yes
CONF

sudo mkdir -p /usr/share/p11-kit/modules
echo "module: $PKCS11_SO" | sudo tee /usr/share/p11-kit/modules/certum.module > /dev/null

CERT_PEM="$(mktemp --suffix=.pem)"
echo "$CERTUM_CERT_PEM" | base64 -d > "$CERT_PEM"

# --- Virtual display ---

Xvfb :99 -screen 0 1024x768x24 &
XVFB_PID=$!
export DISPLAY=:99
sleep 2

stalonetray --geometry 1x1+0+0 --grow-gravity W &
sleep 1

# --- Launch and authenticate SimplySign Desktop ---

echo "==> Launching SimplySign Desktop..."
"$SSD_EXE" &
SSD_PID=$!
sleep 8

echo "==> Searching for login window..."
WINDOW_ID=$(timeout 30 xdotool search --sync --onlyvisible --name "SimplySign" 2>/dev/null | head -1 || true)
if [[ -z "$WINDOW_ID" ]]; then
    for wid in $(xdotool search --name "" 2>/dev/null || true); do
        wname=$(xdotool getwindowname "$wid" 2>/dev/null || true)
        if [[ "$wname" == *"SimplySign"* ]]; then
            WINDOW_ID="$wid"
            break
        fi
    done
fi
if [[ -z "$WINDOW_ID" ]]; then
    echo "Error: Could not find SimplySign login window"
    exit 1
fi

xdotool windowactivate --sync "$WINDOW_ID" 2>/dev/null || true
sleep 1

# Generate TOTP right before typing to avoid expiration
TOTP=$(oathtool --totp=sha256 -b --digits=6 "$CERTUM_TOTP_SECRET")

# Click email field and enter credentials
WX=$(xdotool getwindowgeometry --shell "$WINDOW_ID" 2>/dev/null | grep X= | cut -d= -f2 || echo 0)
WY=$(xdotool getwindowgeometry --shell "$WINDOW_ID" 2>/dev/null | grep Y= | cut -d= -f2 || echo 0)
WW=$(xdotool getwindowgeometry --shell "$WINDOW_ID" 2>/dev/null | grep WIDTH= | cut -d= -f2 || echo 800)
WH=$(xdotool getwindowgeometry --shell "$WINDOW_ID" 2>/dev/null | grep HEIGHT= | cut -d= -f2 || echo 600)
xdotool mousemove "$((WX + WW / 2))" "$((WY + WH * 30 / 100))"
xdotool click 1
sleep 0.5
xdotool key ctrl+a
sleep 0.1
xdotool type --clearmodifiers --delay 30 "$CERTUM_SIMPLYSIGN_USER"
sleep 0.5
xdotool key Tab
sleep 0.3
xdotool type --clearmodifiers --delay 30 "$TOTP"
sleep 0.5
xdotool key Return
echo "==> Credentials submitted, waiting for authentication..."

sleep 10

# Click Close on "Logon succesfull" dialog (required for PKCS#11 token activation)
xdotool key Return
sleep 2
WINDOW_ID2=$(xdotool search --name "SimplySign" 2>/dev/null | head -1 || true)
if [[ -n "$WINDOW_ID2" ]]; then
    xdotool windowactivate "$WINDOW_ID2" 2>/dev/null || true
    sleep 0.5
    xdotool key Return
fi
sleep 3

# --- Wait for PKCS#11 token ---

echo "==> Waiting for PKCS#11 token..."
TOKEN_READY=false
for i in $(seq 1 12); do
    if p11-kit list-modules 2>&1 | grep -qi "SimplySign\|certum"; then
        TOKEN_READY=true
        break
    fi
    sleep 5
done
if [[ "$TOKEN_READY" != "true" ]]; then
    echo "Error: PKCS#11 token not available after 60s"
    exit 1
fi
echo "==> PKCS#11 token available"

# --- Start p11-kit server ---

P11_SOCKET="/tmp/p11kit.sock"
p11-kit server -f -n "$P11_SOCKET" "pkcs11:token=Code%20Signing" &
P11_PID=$!
sleep 3
export P11_KIT_SERVER_ADDRESS="unix:path=$P11_SOCKET"

if [[ ! -S "$P11_SOCKET" ]]; then
    kill "$P11_PID" 2>/dev/null || true
    p11-kit server -f -n "$P11_SOCKET" "pkcs11:" &
    P11_PID=$!
    sleep 3
fi

# --- Sign with jsign ---

JSIGN_JAR="/tmp/jsign.jar"
curl -sSL -o "$JSIGN_JAR" "https://github.com/ebourg/jsign/releases/download/7.4/jsign-7.4.jar"

P11_CLIENT=$(find /usr -name "p11-kit-client.so" -path "*/pkcs11/*" 2>/dev/null | head -1 || true)

PKCS11_CFG="/tmp/pkcs11.cfg"
cat > "$PKCS11_CFG" <<CFGEOF
name = p11kit
library = $P11_CLIENT
slotListIndex = 0
CFGEOF

# Discover key alias
KEY_ALIAS=$(keytool -list -keystore NONE -storetype PKCS11 \
    -providerClass sun.security.pkcs11.SunPKCS11 \
    -providerArg "$PKCS11_CFG" -storepass "" 2>/dev/null \
    | grep "PrivateKeyEntry" | head -1 | cut -d, -f1 | tr -d ' ')
echo "==> Key alias: $KEY_ALIAS"

for f in "$@"; do
    echo "==> Signing $f..."

    java -jar "$JSIGN_JAR" \
        --storetype PKCS11 \
        --keystore "$PKCS11_CFG" \
        --alias "$KEY_ALIAS" \
        --certfile "$CERT_PEM" \
        --tsaurl http://time.certum.pl \
        --tsmode RFC3161 \
        -a sha256 \
        "$f"

    echo "==> Verifying $f..."
    osslsigncode verify -in "$f"
    echo "==> Signed: $f"
done

# --- Cleanup ---

kill "$P11_PID" 2>/dev/null || true
kill "$SSD_PID" 2>/dev/null || true
kill "$XVFB_PID" 2>/dev/null || true
rm -f "$CERT_PEM"

echo "==> Done. All files signed and verified."
