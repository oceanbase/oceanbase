#!/usr/bin/env bash
# Collect workflow result from fail_cases.output.
set -e

OUT_PATH="${1:-$GITHUB_WORKSPACE/farm_result.json}"
TASK_DIR="${FARM_TASK_DIR:-${SEEKDB_TASK_DIR:?}}"

FAIL_FILE="$TASK_DIR/fail_cases.output"
failed_cases=()

if [[ -f "$FAIL_FILE" ]] && [[ -s "$FAIL_FILE" ]]; then
  while IFS= read -r line; do
    [[ -n "$line" ]] && failed_cases+=("$line")
  done < "$FAIL_FILE"
fi

success="true"
[[ ${#failed_cases[@]} -gt 0 ]] && success="false"

cases_json=""
for case_name in "${failed_cases[@]}"; do
  escaped="${case_name//\"/\\\"}"
  [[ -n "$cases_json" ]] && cases_json="$cases_json,"
  cases_json="$cases_json\"$escaped\""
done

printf '{"success":%s,"run_id":"%s","failed_cases":[%s]}\n' \
  "$success" "${GITHUB_RUN_ID:-0}" "$cases_json" > "$OUT_PATH"

echo "[collect_result.sh] wrote $OUT_PATH success=$success failed=${#failed_cases[@]}"
