#!/usr/bin/env bash
# Shared environment bootstrap for mysqltest slices.
set -e

WORKSPACE="${GITHUB_WORKSPACE:?}"
TASK_DIR="${FARM_TASK_DIR:-${SEEKDB_TASK_DIR:?}}"
SLICE_SLOT="${SLICE_IDX:-0}"
FARM_HOME="${WORKSPACE}/farm_home_${SLICE_SLOT}"

mkdir -p "$FARM_HOME" "$FARM_HOME/downloads" "$FARM_HOME/data"
rm -rf "$FARM_HOME/oceanbase"
ln -s "$WORKSPACE" "$FARM_HOME/oceanbase"

export HOME="$FARM_HOME"
export USER="$(whoami)"
export HOST="$(hostname -i 2>/dev/null || hostname)"
export DOWNLOAD_DIR="$HOME/downloads"
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
export OBD_HOME="$HOME"
export OBD_INSTALL_PRE="$DEP_PATH"
export DATA_PATH="$HOME/data"
export obd="${DEP_PATH}/usr/bin/obd"

if [[ -d "$DEP_PATH/u01/obclient/lib" ]]; then
  export LD_LIBRARY_PATH="$DEP_PATH/u01/obclient/lib:${LD_LIBRARY_PATH:-}"
fi

extract_artifact() {
  local name="$1"
  local src=""

  if [[ -x "$HOME/$name" ]]; then
    return 0
  fi
  if [[ -x "$WORKSPACE/$name" ]]; then
    ln -sfn "$WORKSPACE/$name" "$HOME/$name"
    return 0
  fi
  if [[ -f "$TASK_DIR/$name.zst" ]]; then
    src="$TASK_DIR/$name.zst"
  elif [[ -f "$WORKSPACE/$name.zst" ]]; then
    src="$WORKSPACE/$name.zst"
  fi
  if [[ -n "$src" ]] && command -v zstd >/dev/null 2>&1; then
    zstd -df "$src" -o "$HOME/$name"
    chmod +x "$HOME/$name"
    cp -f "$HOME/$name" "$WORKSPACE/$name" 2>/dev/null || true
  fi
}

if [[ -x "$HOME/observer" ]] && "$HOME/observer" -V 2>&1 | grep -E '(OceanBase CE|OceanBase_CE)' >/dev/null 2>&1; then
  export COMPONENT="oceanbase-ce"
  export IS_CE=1
else
  export COMPONENT="oceanbase"
  export IS_CE=0
fi

extract_artifact observer
extract_artifact obproxy
