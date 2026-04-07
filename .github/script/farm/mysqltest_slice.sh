#!/usr/bin/env bash
# Run one mysqltest slice and record failures into fail_cases.output.
set -e

WORKSPACE="${GITHUB_WORKSPACE:?}"
TASK_DIR="${FARM_TASK_DIR:-${SEEKDB_TASK_DIR:?}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$SCRIPT_DIR/scripts"

if [[ ! -f "$WORKSPACE/build.sh" ]] && [[ -f "$(pwd)/build.sh" ]]; then
  WORKSPACE="$(pwd)"
fi

export GITHUB_WORKSPACE="$WORKSPACE"
export FARM_TASK_DIR="$TASK_DIR"
export SLICE_IDX="${SLICE_IDX:-0}"
export SLICES="${SLICES:-4}"
export BRANCH="${BRANCH:-master}"

if [[ -f "$SCRIPTS_DIR/frame.sh" ]]; then
  source "$SCRIPTS_DIR/frame.sh"
fi

for artifact in observer.zst obproxy.zst; do
  if [[ -f "$TASK_DIR/$artifact" ]] && [[ ! -f "$WORKSPACE/$artifact" ]]; then
    cp -f "$TASK_DIR/$artifact" "$WORKSPACE/" || true
  fi
done

if [[ -f "$SCRIPTS_DIR/mysqltest_for_farm.sh" ]]; then
  bash "$SCRIPTS_DIR/mysqltest_for_farm.sh" "$@"
else
  echo "[mysqltest_slice.sh] mysqltest_for_farm.sh is missing, skip slice $SLICE_IDX."
fi
