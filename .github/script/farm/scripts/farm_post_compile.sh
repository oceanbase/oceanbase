#!/usr/bin/env bash
# Collect and compress build artifacts after compile.
set -e

ret="${1:-0}"
if [[ "$ret" != "0" ]]; then
  exit "$ret"
fi

WORKSPACE="${WORKSPACE:?}"
TASK_DIR="${TASK_DIR:?}"

strip_binary() {
  local bin="$1"
  if [[ ! -f "$bin" ]]; then
    return 0
  fi
  if command -v objdump >/dev/null 2>&1 && command -v objcopy >/dev/null 2>&1; then
    if ! objdump -s -j .gnu_debuglink "$bin" >/dev/null 2>&1; then
      objcopy --only-keep-debug "$bin" "$bin.debug" || true
      objcopy --add-gnu-debuglink="$bin.debug" "$bin" || true
      objcopy -g "$bin" || true
    fi
  fi
}

copy_artifact() {
  local binary="$1"
  local source=""

  for candidate in \
    "$WORKSPACE/$binary" \
    "$WORKSPACE/build_debug/$binary" \
    "$WORKSPACE/build_release/$binary" \
    "$WORKSPACE/build/$binary"
  do
    if [[ -f "$candidate" ]]; then
      source="$candidate"
      break
    fi
  done

  if [[ -z "$source" ]]; then
    return 0
  fi

  cp -f "$source" "$WORKSPACE/$binary"
  chmod +x "$WORKSPACE/$binary" || true
  strip_binary "$WORKSPACE/$binary"
  if command -v zstd >/dev/null 2>&1; then
    zstd -f "$WORKSPACE/$binary"
    [[ -f "$WORKSPACE/$binary.zst" ]] && cp -f "$WORKSPACE/$binary.zst" "$TASK_DIR/"
  fi
}

copy_artifact observer
copy_artifact obproxy
