#!/usr/bin/env bash
# Prepare task metadata consumed by the seekdb-style workflow.
set -e

WORKSPACE="${GITHUB_WORKSPACE:?}"
RUN_ID="${GITHUB_RUN_ID:?}"
SLICES="${MYSQLTEST_SLICES:-4}"
TASK_DIR="${FARM_TASK_DIR:-${SEEKDB_TASK_DIR:?}}"

mkdir -p "$TASK_DIR"

echo '++compile++' > "$TASK_DIR/run_jobs.output"
for i in $(seq 0 $((SLICES - 1))); do
  echo "++mysqltest++${i}++" >> "$TASK_DIR/run_jobs.output"
done

{
  echo '++is_cmake++'
  echo '++need_agentserver++0'
  echo '++need_libobserver_so++0'
  echo '++need_liboblog++0'
} > "$TASK_DIR/jobargs.output"

echo "[prepare.sh] workspace=$WORKSPACE run_id=$RUN_ID task_dir=$TASK_DIR"
