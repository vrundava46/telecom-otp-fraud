#!/usr/bin/env bash
set -euo pipefail
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb -p local/lakehouse || true
mc mb -p local/warehouse || true
echo "buckets ready"
