#!/usr/bin/env python
"""
OceanBase libobcdc Log TRAFFIC Analysis Tool
- Exact match for specified tenant (avoids matching 1001 when TENANT=1)
- Automatically parses libobcdc.log / libobcdc.log.* files (supports wildcards)
- Output: Average, Min/Max (with exact timestamp), log time span, record count
- Smart unit conversion (B/KB/MB/GB/TB), default binary standard
"""
from __future__ import print_function

import sys
import re
import argparse
import glob
import io
import os
from datetime import datetime

def parse_timestamp(ts_str):
    """Parse log timestamp [YYYY-MM-DD HH:MM:SS.ffffff]"""
    return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')

def format_duration(td):
    """Convert timedelta to human-readable format (prioritize larger units)"""
    total_sec = int(td.total_seconds())
    if total_sec < 60:
        return "{}s".format(total_sec)
    elif total_sec < 3600:
        m, s = divmod(total_sec, 60)
        return "{}m{}s".format(m, s)
    else:
        h, r = divmod(total_sec, 3600)
        m, s = divmod(r, 60)
        return "{}h{}m{}s".format(h, m, s)

def main():
    parser = argparse.ArgumentParser(
        description="Analyze TRAFFIC statistics for specified tenant in OceanBase libobcdc logs",
        epilog="Examples:\n"
               "  # Analyze tenant 1002 in all libobcdc.log* files in current directory\n"
               "  python traffic_analyzer.py 1002 -p 'libobcdc.log*'\n"
               "  # Analyze multiple files (shell wildcard expansion)\n"
               "  python traffic_analyzer.py 1 libobcdc.log.2026*",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('tenant_id', type=str, help='Target Tenant ID (string, e.g., "1", "1002")')
    parser.add_argument('-p', '--pattern', type=str,
                        help='Log file glob pattern (e.g., "libobcdc.log*", "libobcdc.log.2026*")')
    parser.add_argument('log_files', nargs='*',
                        help='Log file list (can be passed after shell wildcard expansion)')
    parser.add_argument('-d', '--decimal', action='store_true',
                        help='Use decimal units (1 MB = 1000 KB), default binary (1 MB = 1024 KB)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show verbose processing info')
    args = parser.parse_args()

    # === Determine Input File List ===
    if args.pattern:
        files = sorted(glob.glob(args.pattern))
        if args.verbose:
            print("🔍 Pattern '{}' matched {} files: {}".format(args.pattern, len(files), files), file=sys.stderr)
    elif args.log_files:
        files = [f for f in args.log_files if os.path.exists(f)]
        missing = [f for f in args.log_files if not os.path.exists(f)]
        if missing and args.verbose:
            print("⚠️  Warning: The following files do not exist and were skipped: {}".format(missing), file=sys.stderr)
    else:
        print("❌ Error: Please specify file pattern via -p or provide log file paths directly", file=sys.stderr)
        sys.exit(1)

    if not files:
        print("❌ Error: No valid log files found", file=sys.stderr)
        sys.exit(1)

    # === Initialize Parsing Rules ===
    tenant_re = re.compile(r'TENANT={},'.format(re.escape(args.tenant_id)))
    traffic_re = re.compile(r'TRAFFIC=([\d.]+)([A-Za-z]+)/sec')
    ts_re = re.compile(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\]')

    base = 1000 if args.decimal else 1024
    unit_factors = {
        'B': 1 / (base * base),
        'KB': 1 / base,
        'MB': 1.0,
        'GB': float(base),
        'TB': float(base * base)
    }

    # === Data Collection ===
    records = []  # (datetime, timestamp_str, traffic_mb)
    all_ts = []
    unknown_units = set()
    total_lines = 0

    for filepath in files:
        try:
            with io.open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    total_lines += 1
                    ts_match = ts_re.match(line)
                    if not ts_match:
                        continue
                    ts_str = ts_match.group(1)

                    if not tenant_re.search(line):
                        continue

                    traffic_match = traffic_re.search(line)
                    if not traffic_match:
                        continue

                    try:
                        val = float(traffic_match.group(1))
                    except ValueError:
                        continue

                    unit = traffic_match.group(2).upper().strip()
                    factor = unit_factors.get(unit)
                    if factor is None:
                        unknown_units.add(unit)
                        continue

                    traffic_mb = val * factor
                    ts_dt = parse_timestamp(ts_str)
                    records.append((ts_dt, ts_str, traffic_mb))
                    all_ts.append(ts_dt)
        except Exception as e:
            print("⚠️  Skipping file '{}': {}: {}".format(filepath, type(e).__name__, e), file=sys.stderr)
            continue

    # === Result Validation and Calculation ===
    if unknown_units and args.verbose:
        print("⚠️  Warning: Skipping unknown units {} (Supported: B, KB, MB, GB, TB)".format(sorted(unknown_units)), file=sys.stderr)

    if not records:
        print("❌ Error: No valid TRAFFIC records found for tenant '{}' (scanned {} lines)".format(args.tenant_id, total_lines), file=sys.stderr)
        sys.exit(1)

    # Statistical Calculation
    traffic_vals = [r[2] for r in records]
    avg_val = sum(traffic_vals) / len(traffic_vals)
    min_record = min(records, key=lambda x: x[2])
    max_record = max(records, key=lambda x: x[2])

    all_ts.sort()
    start_ts, end_ts = all_ts[0], all_ts[-1]
    duration = end_ts - start_ts

    # === Formatted Output ===
    print("=" * 50)
    print("📊 Tenant {} TRAFFIC Statistics Report (Unit: MB/sec)".format(args.tenant_id))
    print("=" * 50)
    print("✅ Valid Records: {}".format(len(records)))
    print("⏱️  Log Time Range: {} → {}".format(
        start_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))
    print("⏱️  Total Duration: {}".format(format_duration(duration)))
    print("-" * 50)
    print("📈 Average: {:.2f}".format(avg_val))
    print("📉 Minimum: {:.2f} (Time: {})".format(min_record[2], min_record[1]))
    print("📈 Maximum: {:.2f} (Time: {})".format(max_record[2], max_record[1]))
    print("=" * 50)
    if args.verbose:
        print("ℹ️  Processed Files: {} | Scanned Lines: {}".format(len(files), total_lines), file=sys.stderr)

if __name__ == '__main__':
    main()
