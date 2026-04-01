#!/usr/bin/env bash
# Compatibility entrypoint mirroring the seekdb helper layout.
set -e

cd "${WORKSPACE:?}"
mkdir -p "${TASK_DIR:?}"
build_type="${PACKAGE_TYPE:-debug}"

if [[ -x "${WORKSPACE}/build.sh" ]]; then
  bash "${WORKSPACE}/build.sh" "$build_type" --init 2>&1 | tee "${TASK_DIR}/compile_init.output"
  [[ ${PIPESTATUS[0]} -ne 0 ]] && exit 1

  build_dir="${WORKSPACE}/build_${build_type}"
  if [[ -d "$build_dir" ]]; then
    cd "$build_dir"
    exec ${MAKE:-make} ${MAKE_ARGS:--j32} observer
  fi
  echo "[farm_compile.sh] build directory not found: $build_dir"
  exit 1
else
  echo "[farm_compile.sh] build.sh not found in ${WORKSPACE}, skip."
  exit 1
fi
