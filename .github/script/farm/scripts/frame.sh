#!/usr/bin/env bash
# Shared environment bootstrap for mysqltest slices.
set -e

WORKSPACE="${GITHUB_WORKSPACE:?}"
TASK_DIR="${SEEKDB_TASK_DIR:?}"
SLICE_SLOT="${SLICE_IDX:-0}"

export HOME="$WORKSPACE"
export USER="$(whoami)"
export HOST="$(hostname -i 2>/dev/null || hostname)"
export DOWNLOAD_DIR="$WORKSPACE/downloads"
export SLOT_ID="$SLICE_SLOT"
export WORKSPACE
export TASK_DIR

if grep 'dep_create.sh' "$WORKSPACE/build.sh" >/dev/null 2>&1; then
  DEP_PATH="$WORKSPACE/deps/3rd"
else
  DEP_PATH="$WORKSPACE/rpm/.dep_create/var"
fi

export DEP_PATH
export PATH="$DEP_PATH/usr/bin:$DEP_PATH/u01/obclient/bin:$PATH"
export OBD_HOME="$WORKSPACE/tools/deploy"
export OBD_INSTALL_PRE="$DEP_PATH"
export DATA_PATH="$WORKSPACE/data/$SLICE_SLOT"
export obd="${DEP_PATH}/usr/bin/obd"

if [[ -d "$DEP_PATH/u01/obclient/lib" ]]; then
  export LD_LIBRARY_PATH="$DEP_PATH/u01/obclient/lib:${LD_LIBRARY_PATH:-}"
fi

extract_artifact() {
  local name="$1"
  local src=""

  if [[ -x "$WORKSPACE/$name" ]]; then
    return 0
  fi
  if [[ -f "$TASK_DIR/$name.zst" ]]; then
    src="$TASK_DIR/$name.zst"
  elif [[ -f "$WORKSPACE/$name.zst" ]]; then
    src="$WORKSPACE/$name.zst"
  fi
  if [[ -n "$src" ]] && command -v zstd >/dev/null 2>&1; then
    zstd -df "$src" -o "$WORKSPACE/$name"
    chmod +x "$WORKSPACE/$name"
  fi
}

if [[ -x "$WORKSPACE/observer" ]] && "$WORKSPACE/observer" -V 2>&1 | grep -E '(OceanBase CE|OceanBase_CE)' >/dev/null 2>&1; then
  export COMPONENT="oceanbase-ce"
  export IS_CE=1
else
  export COMPONENT="oceanbase"
  export IS_CE=0
fi

extract_artifact observer
extract_artifact obproxy
