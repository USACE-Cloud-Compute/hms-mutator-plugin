#!/usr/bin/env bash
set -euo pipefail

# Run the hms-mutator in Docker
# Expects directory structure: ./payload/payload (config), ./payload/catalog.grid, etc.

docker run --rm \
  -e CC_STORE_TYPE="${CC_STORE_TYPE:-FS}" \
  -e CC_MANIFEST_ID="${CC_MANIFEST_ID:-manifest}" \
  -e CC_PAYLOAD_ID="${CC_PAYLOAD_ID:-payload}" \
  -e FSB_ROOT_PATH="${FSB_ROOT_PATH:-/mnt}" \
  -v "$PWD":/mnt \
  hms-mutator-plugin:local /app/hms-mutator



