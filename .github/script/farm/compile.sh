#!/usr/bin/env bash
# Build observer and package artifacts via scripts/ helpers.
set -e

WORKSPACE="${GITHUB_WORKSPACE:?}"
TASK_DIR="${FARM_TASK_DIR:-${SEEKDB_TASK_DIR:?}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$SCRIPT_DIR/scripts"

export GITHUB_WORKSPACE="$WORKSPACE"
export FARM_TASK_DIR="$TASK_DIR"
export WORKSPACE
export TASK_DIR
export PACKAGE_TYPE="${RELEASE_MODE:+release}"
export PACKAGE_TYPE="${PACKAGE_TYPE:-debug}"
export MAKE="${MAKE:-make}"
export MAKE_ARGS="${MAKE_ARGS:--j32}"
export PATH="$WORKSPACE/deps/3rd/usr/local/oceanbase/devtools/bin:$PATH"

if [[ -n "${FORWARDING_HOST:-}" ]]; then
  echo "$FORWARDING_HOST mirrors.oceanbase.com" >> /etc/hosts 2>/dev/null || true
fi

deps_dir="$WORKSPACE/deps/init"
if [[ -d "$deps_dir" ]]; then
  shopt -s nullglob
  for f in "$deps_dir"/oceanbase.*.deps; do
    sed -i 's/mirrors.aliyun.com/mirrors.cloud.aliyuncs.com/g' "$f"
  done
  shopt -u nullglob
fi

mkdir -p "$TASK_DIR"
compile_ret=0
if [[ -f "$SCRIPTS_DIR/farm_compile.sh" ]]; then
  set +e
  bash "$SCRIPTS_DIR/farm_compile.sh" 2>&1 | tee "$TASK_DIR/compile.output"
  compile_ret=${PIPESTATUS[0]}
  set -e
else
  echo "[compile.sh] farm_compile.sh is missing."
  compile_ret=1
fi

if [[ -f "$SCRIPTS_DIR/farm_post_compile.sh" ]]; then
  bash "$SCRIPTS_DIR/farm_post_compile.sh" "$compile_ret"
fi

exit "$compile_ret"
