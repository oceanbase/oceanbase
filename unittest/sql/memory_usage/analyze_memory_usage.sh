#!/bin/bash

BRANCH=${1:-master}

GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
if [ -z "$GIT_ROOT" ]; then
    echo "ERROR: not inside a git repository" >&2
    exit 1
fi
cd "$GIT_ROOT"

RESULT_DIR="unittest/sql/memory_usage/result"

COMMIT=$(git merge-base HEAD origin/$BRANCH 2>/dev/null)
if [ -z "$COMMIT" ]; then
    echo "ERROR: cannot get merge-base with origin/$BRANCH" >&2
    exit 1
fi
echo "merge-base commit: $COMMIT"
echo ""

CHANGED=$(git diff "$COMMIT" --name-only -- "$RESULT_DIR")
if [ -z "$CHANGED" ]; then
    echo "No changed files in $RESULT_DIR"
    exit 0
fi

TMPDIR_WORK=$(mktemp -d)
trap "rm -rf $TMPDIR_WORK" EXIT

for file in $CHANGED; do
    if [ ! -f "$file" ]; then
        continue
    fi
    echo "=== $(basename $file) ==="

    old_tmp="$TMPDIR_WORK/old"
    new_tmp="$TMPDIR_WORK/new"

    git show "$COMMIT:$file" >"$old_tmp" 2>/dev/null
    if [ $? -ne 0 ] || [ ! -s "$old_tmp" ]; then
        echo "  (new file, no baseline)"
        echo ""
        continue
    fi
    cp "$file" "$new_tmp"

    python3 - "$old_tmp" "$new_tmp" <<'PYEOF'
import sys
import re

def parse_table(content, table_name):
    rows = []
    found = False
    header_done = False
    for line in content.splitlines():
        if line.strip() == table_name:
            found = True
            continue
        if not found:
            continue
        if re.match(r'^\| ---', line):
            continue
        if re.match(r'^\| query', line):
            header_done = True
            continue
        if header_done and line.startswith('|'):
            cells = [c.strip().replace(',', '') for c in line.split('|')[1:-1]]
            if cells:
                rows.append(cells)
        elif header_done:
            break
    return rows

def fmt_diff(o_str, n_str):
    try:
        o = int(o_str)
        n = int(n_str)
    except ValueError:
        return ('N/A', 'N/A')
    diff = n - o
    if o == 0:
        pct_str = 'N/A'
    else:
        pct = diff / o * 100
        pct_str = '0.00%' if abs(pct) < 0.001 else f'{pct:+.2f}%'
    diff_str = '0' if diff == 0 else f'{diff:+,}'
    return (pct_str, diff_str)

old_path, new_path = sys.argv[1], sys.argv[2]
with open(old_path) as f:
    old_content = f.read()
with open(new_path) as f:
    new_content = f.read()

COLS = ['parser', 'resolve', 'transform', 'optimize', 'total']
file_has_diff = False

for table_name in ['memory usage', 'max memory usage']:
    old_rows = parse_table(old_content, table_name)
    new_rows = parse_table(new_content, table_name)
    if not old_rows or not new_rows:
        continue
    diff_lines = []
    for old_row, new_row in zip(old_rows, new_rows):
        query = old_row[0]
        results = [fmt_diff(old_row[i+1], new_row[i+1]) for i in range(len(COLS))]
        if any(r[0] != '0.00%' for r in results):
            diff_lines.append((query, results))
    if diff_lines:
        file_has_diff = True
        print(f'[{table_name}]')
        col_w = 22
        header = f"  {'query':<6}" + ''.join(f"| {c:<{col_w}}" for c in COLS) + '|'
        print(header)
        sep = f"  {'-' * 6}" + ''.join(f"| {'-' * col_w}" for _ in COLS) + '|'
        print(sep)
        for query, results in diff_lines:
            row = f"  {query:<6}"
            for pct_str, diff_str in results:
                cell = f"{pct_str} ({diff_str})"
                row += f"| {cell:<{col_w}}"
            row += '|'
            print(row)
        print()

if not file_has_diff:
    print('  no diff')
    print()
PYEOF

done
