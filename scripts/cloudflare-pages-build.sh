#!/usr/bin/env bash
set -euo pipefail

# Cloudflare Pages dashboard build command:
#   bash scripts/cloudflare-pages-build.sh
# Build output directory: docs/dist

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if ! command -v pnpm >/dev/null 2>&1; then
    npm install -g "$(node -p "require('./docs/package.json').packageManager")"
fi

make docs-build
