#!/usr/bin/env bash
set -euo pipefail

# Load all geo-score pipelines.
#
# Usage:
#   ./scripts/load.sh 75 92 93 94    # specific departments
#   ./scripts/load.sh --all           # all departments

if [ $# -eq 0 ]; then
  echo "Usage: $0 <dep1> <dep2> ... | --all"
  exit 1
fi

# Build --dep flags or --all
if [ "$1" = "--all" ]; then
  DEP_FLAGS="--all"
else
  DEP_FLAGS=""
  for dep in "$@"; do
    DEP_FLAGS="$DEP_FLAGS --dep $dep"
  done
fi

echo "============================================"
echo "  geo-score-integration — full data load"
echo "  departments: $*"
echo "============================================"
echo ""

run() {
  local name="$1"
  shift
  echo "▸ $name"
  uv run geo-integrate "$@" || echo "  ⚠ $name failed, continuing..."
  echo ""
}

# Check DB connection first
uv run geo-integrate check-db

# Pipelines that don't need a year
run "DVF prices (2023)"         dvf --year 2023 $DEP_FLAGS
run "Crime stats (2024)"        delinquance --year 2024 $DEP_FLAGS
run "OSM shops"                 shops $DEP_FLAGS
run "OSM green spaces"          green-spaces $DEP_FLAGS
run "MNT sun exposure"          exposition $DEP_FLAGS
run "TRI flood zones"           flood-tri $DEP_FLAGS
run "RGA clay risk"             clay-risk $DEP_FLAGS
run "Storm risk"                storm-risk $DEP_FLAGS
run "BDNB buildings"            bdnb --reset $DEP_FLAGS

echo "============================================"
echo "  All pipelines completed."
echo "============================================"
