#!/usr/bin/env python
"""
requirements: pip install matplotlib

CDC Performance Visualizer
Reads cdc_performance_*.json files and generates charts as PNG (matplotlib).

Usage:
  python visualize_performance.py cdc_performance_1771924557.json
  python visualize_performance.py cdc_performance_*.json -o report.png
  python visualize_performance.py data1.json data2.json --compare
"""
from __future__ import print_function

import argparse
import json
import os
import sys
import time


def _load_json(path):
    with open(path, 'r') as f:
        return json.load(f)


def _ts_to_relative(timestamps):
    if not timestamps:
        return []
    t0 = timestamps[0]
    return [round(t - t0, 1) for t in timestamps]


def _bytes_to_mbs(values):
    return [round(v / (1024 * 1024), 2) for v in values]


def _human_duration(seconds):
    if seconds < 60:
        return "{:.0f}s".format(seconds)
    if seconds < 3600:
        return "{:.1f}min".format(seconds / 60)
    return "{:.1f}h".format(seconds / 3600)


# ---------------------------------------------------------------------------
# Shared: build chart specifications (format-agnostic)
# Each spec: {'title': str, 'x': list, 'y_name': str,
#             'series': [{'name', 'data', 'area'?, 'yAxis2'?}]}
# ---------------------------------------------------------------------------

def _chart_spec(title, x_data, series_list, y_name=''):
    return {'title': title, 'x': x_data, 'y_name': y_name, 'series': series_list}


def _build_specs(data):
    raw = data.get('raw_metrics', {})
    ts = raw.get('timestamp', [])
    x = _ts_to_relative(ts)
    if not x:
        return []

    specs = []

    cpu = raw.get('cpu_percent', [])
    if cpu:
        specs.append(_chart_spec('CPU Usage', x,
            [{'name': 'CPU %', 'data': cpu, 'area': True}], '%'))

    mem_pct = raw.get('memory_percent', [])
    mem_rss = raw.get('memory_rss', [])
    if mem_pct or mem_rss:
        s = []
        if mem_pct:
            s.append({'name': 'Memory %', 'data': mem_pct})
        if mem_rss:
            rss_mb = [round(v / (1024 * 1024), 1) for v in mem_rss]
            s.append({'name': 'RSS (MB)', 'data': rss_mb, 'yAxis2': True})
        specs.append(_chart_spec('Memory Usage', x, s, '%'))

    io_r = raw.get('io_read_bytes', [])
    io_w = raw.get('io_write_bytes', [])
    lio_r = raw.get('io_logical_read_bytes', [])
    lio_w = raw.get('io_logical_write_bytes', [])
    if io_r or io_w or lio_r or lio_w:
        s = []
        if lio_r:
            s.append({'name': 'Logical Read', 'data': _bytes_to_mbs(lio_r)})
        if lio_w:
            s.append({'name': 'Logical Write', 'data': _bytes_to_mbs(lio_w)})
        if io_r:
            s.append({'name': 'Physical Read', 'data': _bytes_to_mbs(io_r)})
        if io_w:
            s.append({'name': 'Physical Write', 'data': _bytes_to_mbs(io_w)})
        specs.append(_chart_spec('I/O Throughput (per interval)', x, s, 'MB'))

    stor = raw.get('cdc_storage', {})
    wr = stor.get('write_rate', [])
    rr = stor.get('read_rate', [])
    if wr or rr:
        sx = x[-len(wr or rr):]
        s = []
        if wr:
            s.append({'name': 'Write Rate', 'data': wr, 'area': True})
        if rr:
            s.append({'name': 'Read Rate', 'data': rr})
        specs.append(_chart_spec('CDC Storage Rate', sx, s, 'MB/s'))

    traf = raw.get('cdc_traffic', {})
    fetcher = traf.get('fetcher_mbs', [])
    pt = traf.get('parser_total_mbs', [])
    pr = traf.get('parser_remaining_mbs', [])
    pf = traf.get('parser_filtered_mbs', [])
    if fetcher or pt:
        n = max(len(fetcher), len(pt), len(pr), len(pf))
        sx = x[-n:]
        s = []
        if fetcher:
            s.append({'name': 'Fetcher', 'data': fetcher, 'area': True})
        if pt:
            s.append({'name': 'Parser Total', 'data': pt})
        if pr:
            s.append({'name': 'Parser Remaining', 'data': pr})
        if pf:
            s.append({'name': 'Parser Filtered', 'data': pf})
        specs.append(_chart_spec('CDC Traffic', sx, s, 'MB/s'))

    tp = raw.get('cdc_throughput', {})
    ntps = tp.get('next_record_tps', [])
    nrps = tp.get('next_record_rps', [])
    rtps = tp.get('release_record_tps', [])
    ctps = tp.get('committer_total_tps', [])
    if ntps or ctps:
        n = max(len(ntps), len(nrps), len(rtps), len(ctps))
        sx = x[-n:]
        s = []
        if ntps:
            s.append({'name': 'Next TPS', 'data': ntps})
        if nrps:
            s.append({'name': 'Next RPS', 'data': nrps})
        if rtps:
            s.append({'name': 'Release TPS', 'data': rtps})
        if ctps:
            s.append({'name': 'Committer TPS', 'data': ctps})
        specs.append(_chart_spec('CDC Throughput', sx, s, 'TPS / RPS'))

    comp = raw.get('rocksdb_compaction', {})
    wa = comp.get('write_amp', [])
    if wa:
        sx = x[-len(wa):]
        specs.append(_chart_spec('RocksDB Write Amplification', sx,
            [{'name': 'Write Amp', 'data': [round(v, 3) for v in wa]}], 'x'))

    ingest = comp.get('user_ingest_gb', [])
    flush = comp.get('flush_gb', [])
    cw = comp.get('compaction_write_gb', [])
    cr = comp.get('compaction_read_gb', [])
    if ingest or flush or cw:
        n = max(len(ingest), len(flush), len(cw), len(cr))
        sx = x[-n:]
        s = []
        if ingest:
            s.append({'name': 'User Ingest', 'data': ingest})
        if flush:
            s.append({'name': 'Flush', 'data': flush})
        if cw:
            s.append({'name': 'Compaction Write', 'data': cw})
        if cr:
            s.append({'name': 'Compaction Read', 'data': cr})
        specs.append(_chart_spec('RocksDB Compaction (Cumulative)', sx, s, 'GB'))

    pending = comp.get('pending_compaction_mb', [])
    stall = comp.get('stall_percent', [])
    if pending or stall:
        n = max(len(pending), len(stall))
        sx = x[-n:]
        s = []
        if pending:
            s.append({'name': 'Pending Compaction', 'data': [round(v, 2) for v in pending]})
        if stall:
            s.append({'name': 'Stall %', 'data': stall, 'yAxis2': True})
        specs.append(_chart_spec('RocksDB Pending & Stall', sx, s, 'MB / %'))

    return specs


# ---------------------------------------------------------------------------
# Matplotlib PNG renderer
# ---------------------------------------------------------------------------

def _render_matplotlib(all_specs, output, title, file_list):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    n = len(all_specs)
    if n == 0:
        print('No data to plot.')
        return

    cols = 2
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(14, 4.2 * rows))
    fig.suptitle(title, fontsize=15, fontweight='bold', y=1.0)

    if rows == 1 and cols == 1:
        axes = [[axes]]
    elif rows == 1:
        axes = [axes]
    elif cols == 1:
        axes = [[ax] for ax in axes]

    flat_axes = [axes[r][c] for r in range(rows) for c in range(cols)]

    for idx, spec in enumerate(all_specs):
        ax = flat_axes[idx]
        x = spec['x']
        has_y2 = any(s.get('yAxis2') for s in spec['series'])
        ax2 = ax.twinx() if has_y2 else None

        for s in spec['series']:
            target = ax2 if s.get('yAxis2') and ax2 else ax
            target.plot(x, s['data'], label=s['name'], linewidth=1.2)
            if s.get('area') and target is ax:
                target.fill_between(x, s['data'], alpha=0.12)

        ax.set_title(spec['title'], fontsize=11, pad=6)
        ax.set_xlabel('Time (s)', fontsize=8)
        ax.set_ylabel(spec['y_name'], fontsize=8)
        ax.tick_params(labelsize=7)
        ax.grid(True, alpha=0.3)

        nth = max(1, len(x) // 8)
        ax.set_xticks(x[::nth])
        ax.tick_params(axis='x', rotation=30)

        lines, labels = ax.get_legend_handles_labels()
        if ax2:
            y2_names = [s['name'] for s in spec['series'] if s.get('yAxis2')]
            ax2.set_ylabel(', '.join(y2_names), fontsize=8)
            ax2.tick_params(labelsize=7)
            l2, lb2 = ax2.get_legend_handles_labels()
            lines += l2
            labels += lb2
        ax.legend(lines, labels, fontsize=7, loc='best', framealpha=0.7)

    for idx in range(n, len(flat_axes)):
        flat_axes[idx].set_visible(False)

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(output, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print('PNG saved to: {}'.format(output))
    print('  Charts: {}, Source: {}'.format(n, file_list))


def generate_png(files, output, compare=False):
    all_specs = []
    for fp in files:
        data = _load_json(fp)
        all_specs.extend(_build_specs(data))

    title = 'CDC Performance Comparison' if compare else 'CDC Performance Dashboard'
    file_list = ', '.join(os.path.basename(f) for f in files)
    _render_matplotlib(all_specs, output, title, file_list)


def main():
    parser = argparse.ArgumentParser(
        description='Generate charts from CDC performance JSON files (PNG)')
    parser.add_argument('files', nargs='+', help='cdc_performance_*.json files')
    parser.add_argument('-o', '--output', default=None,
                        help='Output file (default: cdc_dashboard_<ts>.png)')
    parser.add_argument('--compare', action='store_true',
                        help='Label as comparison mode')
    args = parser.parse_args()

    for fp in args.files:
        if not os.path.isfile(fp):
            print('Error: file not found: {}'.format(fp))
            sys.exit(1)

    ts = int(time.time())
    output = args.output or 'cdc_dashboard_{}.png'.format(ts)
    generate_png(args.files, output, compare=args.compare)


if __name__ == '__main__':
    main()
