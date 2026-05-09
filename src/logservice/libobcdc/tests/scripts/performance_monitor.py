#!/usr/bin/env python
"""
CDC Storage Performance Monitor
Used to monitor and collect CDC performance metrics
"""
from __future__ import print_function

import collections
import io
import json
import os
import re
import sys
import threading
import time

import psutil

MAX_SAMPLES = 86400

_CDC_STORAGE_PATTERNS = {
    'write_rate': (re.compile(r'WRITE_RATE=([\d.]+)M/s'), '[STORAGE]'),
    'write_total_size': (re.compile(r'WRITE_TOTAL_SIZE=([\d.]+)G'), '[STORAGE]'),
    'read_rate': (re.compile(r'READ_RATE=([\d.]+)M/s'), '[READER]'),
    'read_total_size': (re.compile(r'READ_TOTAL_SIZE=([\d.]+)G'), '[READER]'),
}

_TRAFFIC_UNIT_SCALE = {'B': 1.0 / (1024 * 1024), 'KB': 1.0 / 1024, 'MB': 1.0, 'GB': 1024.0, 'TB': 1024 * 1024.0}

_FETCHER_TRAFFIC_RE = re.compile(r'\[FETCH_STREAM\]\s*TENANT=(\d+),\s*TRAFFIC=([-\d.]+)(B|KB|MB|GB)/sec')
_PARSER_TRAFFIC_RE = re.compile(
    r'\[PARSER\]\s*TOTAL_TRAFFIC=([-\d.]+)(B|KB|MB|GB)/sec,\s*'
    r'REMAINING_TRAFFIC=([-\d.]+)(B|KB|MB|GB)/sec,\s*'
    r'FILTERED_OUT_TRAFFIC=([-\d.]+)(B|KB|MB|GB)/sec')

_OUTPUT_TPS_RE = re.compile(
    r'\[DRC\]\s*NEXT_RECORD_TPS=([\d.]+)\s+RELEASE_RECORD_TPS=([\d.]+)\s+'
    r'NEXT_RECORD_RPS=([\d.]+)\s+RELEASE_RECORD_RPS=([\d.]+)')

_COMMITTER_TPS_RE = re.compile(
    r'\[COMMITTER\].*TOTAL_TPS=([\d.]+)\s+EMPTY_TPS=([\d.]+)')

_RE_DUMP_MARKER = re.compile(r'------- DUMPING STATS -------')
_RE_USER_INGEST = re.compile(r'Cumulative writes:.*ingest:\s*([\d.]+)\s*GB')
_RE_STALL_PCT = re.compile(r'Cumulative stall:.*?([\d.]+)\s*percent')
_RE_FLUSH = re.compile(r'Flush\(GB\):\s*cumulative\s*([\d.]+)')
_RE_COMP_WRITE = re.compile(r'Cumulative compaction:\s*([\d.]+)\s*GB write')
_RE_COMP_READ = re.compile(r'Cumulative compaction:.*?([\d.]+)\s*GB read')
_RE_PENDING = re.compile(r'Estimated pending compaction bytes:\s*(\d+)')
_RE_STALL_STOPS = re.compile(r'total-stops:\s*(\d+)')


def _read_tail_lines(filepath, max_lines):
    """Read last max_lines from a file without loading the whole file into memory."""
    return list(collections.deque(
        io.open(filepath, 'r', encoding='utf-8', errors='ignore'),
        maxlen=max_lines,
    ))


def _safe_avg(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _traffic_to_mbs(value, unit):
    """Convert a traffic rate value with unit to MB/s."""
    return value * _TRAFFIC_UNIT_SCALE.get(unit, 1.0)


def _human_size(mb):
    """Convert a value in MB to a human-readable size string."""
    if mb >= 1024 * 1024:
        return "{:.2f}TB".format(mb / (1024 * 1024))
    if mb >= 1024:
        return "{:.2f}GB".format(mb / 1024)
    if mb >= 1:
        return "{:.1f}MB".format(mb)
    if mb >= 1.0 / 1024:
        return "{:.1f}KB".format(mb * 1024)
    return "{:.0f}B".format(mb * 1024 * 1024)


def _human_rate(mbs):
    """Convert a value in MB/s to a human-readable rate string."""
    if mbs >= 1024 * 1024:
        return "{:.2f}TB/s".format(mbs / (1024 * 1024))
    if mbs >= 1024:
        return "{:.2f}GB/s".format(mbs / 1024)
    if mbs >= 1:
        return "{:.1f}MB/s".format(mbs)
    if mbs >= 1.0 / 1024:
        return "{:.1f}KB/s".format(mbs * 1024)
    return "{:.0f}B/s".format(mbs * 1024 * 1024)


DEFAULT_PROCESS_NAME = 'obcdc_tailf'


class CDCPerformanceMonitor:
    def __init__(self, process_name=None, pid=None,
                 cdc_log_file=None, cdc_work_dir=None):
        self.start_time = time.time()
        self.process_name = process_name
        self.pid = pid
        self.target_process = None
        self.cdc_log_file = cdc_log_file
        self._cdc_work_dir = os.path.abspath(cdc_work_dir) if cdc_work_dir else None
        self._lock = threading.Lock()
        self.metrics = {
            'timestamp': [],
            'cpu_percent': [],
            'memory_percent': [],
            'memory_rss': [],
            'io_read_bytes': [],
            'io_write_bytes': [],
            'io_logical_read_bytes': [],
            'io_logical_write_bytes': [],
            'cdc_storage': {
                'write_rate': [],
                'write_total_size': [],
                'read_rate': [],
                'read_total_size': [],
            },
            'cdc_traffic': {
                'fetcher_mbs': [],
                'parser_total_mbs': [],
                'parser_remaining_mbs': [],
                'parser_filtered_mbs': [],
            },
            'cdc_throughput': {
                'next_record_tps': [],
                'next_record_rps': [],
                'release_record_tps': [],
                'release_record_rps': [],
                'committer_total_tps': [],
                'committer_empty_tps': [],
            },
            'rocksdb_compaction': {
                'write_amp': [],
                'compaction_write_gb': [],
                'compaction_read_gb': [],
                'user_ingest_gb': [],
                'flush_gb': [],
                'pending_compaction_mb': [],
                'stall_percent': [],
            },
            'rocksdb_stats': {},
        }
        self.is_monitoring = False
        self.monitor_thread = None
        self._last_logged_msgs = set()

    def _log_once(self, msg):
        """Print a message only on first occurrence."""
        if msg not in self._last_logged_msgs:
            self._last_logged_msgs.add(msg)
            print(msg)

    def _append_metric(self, key, value):
        """Thread-safe append with bounded size."""
        lst = self.metrics[key]
        lst.append(value)
        if len(lst) > MAX_SAMPLES:
            del lst[:len(lst) - MAX_SAMPLES]

    def _append_nested(self, group, key, value):
        """Thread-safe append for nested metric dicts."""
        lst = self.metrics[group][key]
        lst.append(value)
        if len(lst) > MAX_SAMPLES:
            del lst[:len(lst) - MAX_SAMPLES]

    @property
    def cdc_work_dir(self):
        """CDC working directory: user-specified > process cwd."""
        if self._cdc_work_dir:
            return self._cdc_work_dir
        if self.target_process:
            try:
                return self.target_process.cwd()
            except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
                pass
        return None

    def start_monitoring(self, interval=1.0):
        if self.is_monitoring:
            return True

        if not self._find_target_process():
            print("Error: Cannot find target CDC process")
            return False

        work_dir = self.cdc_work_dir
        if work_dir:
            print("CDC working directory: {}".format(work_dir))

        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, args=(interval,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print("Start monitoring process {} ({}), interval: {}s".format(
            self.target_process.pid, self.target_process.name(), interval))
        return True

    def _find_target_process(self):
        try:
            if self.pid:
                self.target_process = psutil.Process(self.pid)
                print("Found target process by PID: {} ({})".format(
                    self.pid, self.target_process.name()))
                return True

            name = self.process_name or DEFAULT_PROCESS_NAME
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if name in proc.info['name'] or \
                       (proc.info['cmdline'] and any(name in str(cmd) for cmd in proc.info['cmdline'])):
                        self.target_process = psutil.Process(proc.info['pid'])
                        print("Found target process by name '{}': {} ({})".format(
                            name, proc.info['pid'], proc.info['name']))
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            print("No process found containing '{}'".format(name))
            return False

        except psutil.NoSuchProcess:
            print("Process does not exist: PID={}".format(self.pid))
            return False
        except Exception as e:
            print("Failed to find process: {}".format(e))
            return False

    def stop_monitoring(self):
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        print("Stop performance monitoring")

    @staticmethod
    def _read_proc_io(pid):
        """Read rchar/wchar from /proc/<pid>/io for logical I/O (includes page cache)."""
        result = {}
        try:
            with open('/proc/{}/io'.format(pid), 'r') as f:
                for line in f:
                    if line.startswith('rchar:'):
                        result['rchar'] = int(line.split()[1])
                    elif line.startswith('wchar:'):
                        result['wchar'] = int(line.split()[1])
        except (OSError, ValueError, IndexError):
            pass
        return result

    def _monitor_loop(self, interval):
        last_io = None
        last_logical = None
        pid = self.target_process.pid if self.target_process else None

        if self.target_process:
            try:
                last_io = self.target_process.io_counters()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        if pid:
            last_logical = self._read_proc_io(pid)

        # Discard first cpu_percent() sample (always returns 0.0)
        if self.target_process:
            try:
                self.target_process.cpu_percent()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            time.sleep(0.1)

        while self.is_monitoring:
            try:
                if not self.target_process or not self.target_process.is_running():
                    print("Target process has exited, stop monitoring")
                    break

                current_time = time.time()
                cpu_percent = self.target_process.cpu_percent()
                memory_info = self.target_process.memory_info()
                memory_percent = self.target_process.memory_percent()

                io_read_bytes = 0
                io_write_bytes = 0
                logical_read = 0
                logical_write = 0

                try:
                    current_io = self.target_process.io_counters()
                    if last_io:
                        io_read_bytes = current_io.read_bytes - last_io.read_bytes
                        io_write_bytes = current_io.write_bytes - last_io.write_bytes
                    last_io = current_io
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

                if pid:
                    cur_logical = self._read_proc_io(pid)
                    if last_logical and cur_logical:
                        logical_read = cur_logical.get('rchar', 0) - last_logical.get('rchar', 0)
                        logical_write = cur_logical.get('wchar', 0) - last_logical.get('wchar', 0)
                    if cur_logical:
                        last_logical = cur_logical

                with self._lock:
                    self._append_metric('timestamp', current_time)
                    self._append_metric('cpu_percent', cpu_percent)
                    self._append_metric('memory_percent', memory_percent)
                    self._append_metric('memory_rss', memory_info.rss)
                    self._append_metric('io_read_bytes', io_read_bytes)
                    self._append_metric('io_write_bytes', io_write_bytes)
                    self._append_metric('io_logical_read_bytes', logical_read)
                    self._append_metric('io_logical_write_bytes', logical_write)

                time.sleep(interval)

            except psutil.NoSuchProcess:
                print("Target process has exited, stop monitoring")
                break
            except Exception as e:
                print("Monitoring error: {}".format(e))
                break

    def collect_rocksdb_stats(self):
        try:
            work_dir = self.cdc_work_dir
            if not work_dir:
                self._log_once("Could not determine CDC working directory, skipping RocksDB stats collection")
                return

            storage_path = os.path.join(work_dir, "storage")
            if not os.path.isdir(storage_path):
                self._log_once("storage subdirectory not found under CDC working directory {}".format(work_dir))
                return

            with self._lock:
                self.metrics['rocksdb_stats'] = {
                    'db_path': storage_path,
                    'timestamp': time.time(),
                    'db_size': self._get_directory_size(storage_path),
                    'file_count': sum(1 for f in os.listdir(storage_path)
                                      if os.path.isfile(os.path.join(storage_path, f))),
                }
            self._collect_rocksdb_compaction_stats(storage_path)
        except Exception as e:
            print("Failed to collect RocksDB statistics: {}".format(e))

    def collect_cdc_log_stats(self):
        log_file = self._find_cdc_log_file()
        if not log_file:
            return

        lines = self._read_cdc_log_with_rotation(log_file)
        if not lines:
            return

        storage = self._parse_storage_stats(lines)
        traffic = self._parse_traffic_stats(lines)
        throughput = self._parse_throughput_stats(lines)

        with self._lock:
            for key in ('write_rate', 'write_total_size', 'read_rate', 'read_total_size'):
                self._append_nested('cdc_storage', key, storage.get(key, 0))
            for key in ('fetcher_mbs', 'parser_total_mbs', 'parser_remaining_mbs', 'parser_filtered_mbs'):
                self._append_nested('cdc_traffic', key, traffic.get(key, 0))
            for key in ('next_record_tps', 'next_record_rps', 'release_record_tps',
                        'release_record_rps', 'committer_total_tps', 'committer_empty_tps'):
                self._append_nested('cdc_throughput', key, throughput.get(key, 0))

    def _find_cdc_log_file(self):
        if self.cdc_log_file and os.path.isfile(self.cdc_log_file):
            return self.cdc_log_file

        work_dir = self.cdc_work_dir
        if not work_dir:
            return None

        candidates = [
            os.path.join(work_dir, "log", "libobcdc.log"),
            os.path.join(work_dir, "libobcdc.log"),
            os.path.join(work_dir, "tools", "obtest", "log", "libobcdc.log"),
        ]
        for path in candidates:
            if os.path.isfile(path):
                return path
        return None

    @staticmethod
    def _read_cdc_log_with_rotation(log_file, max_lines=1000):
        """Read tail of CDC log; if too few lines, prepend from rotated file."""
        try:
            lines = _read_tail_lines(log_file, max_lines)
        except Exception as e:
            print("Failed to read CDC log: {}".format(e))
            return []

        if len(lines) >= 50:
            return lines

        rotated = log_file + ".1"
        if not os.path.isfile(rotated):
            parent = os.path.dirname(log_file)
            base = os.path.basename(log_file)
            try:
                candidates = sorted(
                    (f for f in os.listdir(parent)
                     if f.startswith(base + ".") and f != base),
                    key=lambda f: os.path.getmtime(os.path.join(parent, f)),
                    reverse=True,
                )
                if candidates:
                    rotated = os.path.join(parent, candidates[0])
                else:
                    return lines
            except OSError:
                return lines

        if os.path.isfile(rotated):
            try:
                old_lines = _read_tail_lines(rotated, max_lines - len(lines))
                return old_lines + lines
            except Exception:
                pass
        return lines

    @staticmethod
    def _find_rotated_rocksdb_log(directory):
        """Find the most recent LOG.old.* file in the given directory."""
        try:
            candidates = [
                os.path.join(directory, f)
                for f in os.listdir(directory)
                if f.startswith('LOG.old')
            ]
            if candidates:
                return max(candidates, key=os.path.getmtime)
        except OSError:
            pass
        return None

    def _collect_rocksdb_compaction_stats(self, db_path):
        log_candidates = [
            os.path.join(db_path, "LOG"),
            os.path.join(os.path.dirname(db_path), "LOG"),
        ]
        log_file = next((lf for lf in log_candidates if os.path.isfile(lf)), None)
        if not log_file:
            return

        stats = self._parse_rocksdb_log_stats(log_file)
        if not stats or not stats.get('user_ingest'):
            old_log = self._find_rotated_rocksdb_log(os.path.dirname(log_file))
            if old_log:
                stats = self._parse_rocksdb_log_stats(old_log)
        if not stats:
            return

        user_ingest = stats.get('user_ingest', 0)
        flush = stats.get('flush', 0)
        comp_write = stats.get('compaction_write', 0)
        comp_read = stats.get('compaction_read', 0)
        pending = stats.get('pending_bytes', 0)
        stall_pct = stats.get('stall', 0)

        write_amp = (flush + comp_write) / user_ingest if user_ingest > 0 else 0

        with self._lock:
            self._append_nested('rocksdb_compaction', 'write_amp', write_amp)
            self._append_nested('rocksdb_compaction', 'compaction_write_gb', comp_write)
            self._append_nested('rocksdb_compaction', 'compaction_read_gb', comp_read)
            self._append_nested('rocksdb_compaction', 'user_ingest_gb', user_ingest)
            self._append_nested('rocksdb_compaction', 'flush_gb', flush)
            self._append_nested('rocksdb_compaction', 'pending_compaction_mb', pending / (1024 * 1024))
            self._append_nested('rocksdb_compaction', 'stall_percent', stall_pct)

    @staticmethod
    def _parse_storage_stats(lines):
        stats = {}
        for line in reversed(lines):
            for key, (pattern, section) in _CDC_STORAGE_PATTERNS.items():
                if key not in stats and section in line:
                    m = pattern.search(line)
                    if m:
                        stats[key] = float(m.group(1))
            if len(stats) == len(_CDC_STORAGE_PATTERNS):
                break
        return stats

    @staticmethod
    def _parse_traffic_stats(lines):
        stats = {}
        fetcher_found = False
        for line in reversed(lines):
            if not fetcher_found and '[FETCH_STREAM]' in line:
                m = _FETCHER_TRAFFIC_RE.search(line)
                if m:
                    stats['fetcher_mbs'] = _traffic_to_mbs(float(m.group(2)), m.group(3))
                    fetcher_found = True
            if 'parser_total_mbs' not in stats and '[PARSER]' in line and 'TOTAL_TRAFFIC' in line:
                m = _PARSER_TRAFFIC_RE.search(line)
                if m:
                    stats['parser_total_mbs'] = _traffic_to_mbs(float(m.group(1)), m.group(2))
                    stats['parser_remaining_mbs'] = _traffic_to_mbs(float(m.group(3)), m.group(4))
                    stats['parser_filtered_mbs'] = max(0, _traffic_to_mbs(float(m.group(5)), m.group(6)))
            if fetcher_found and 'parser_total_mbs' in stats:
                break
        return stats

    @staticmethod
    def _parse_throughput_stats(lines):
        stats = {}
        for line in reversed(lines):
            if 'next_record_tps' not in stats and '[DRC]' in line:
                m = _OUTPUT_TPS_RE.search(line)
                if m:
                    stats['next_record_tps'] = float(m.group(1))
                    stats['release_record_tps'] = float(m.group(2))
                    stats['next_record_rps'] = float(m.group(3))
                    stats['release_record_rps'] = float(m.group(4))
            if 'committer_total_tps' not in stats and '[COMMITTER]' in line and 'TOTAL_TPS' in line:
                m = _COMMITTER_TPS_RE.search(line)
                if m:
                    stats['committer_total_tps'] = float(m.group(1))
                    stats['committer_empty_tps'] = float(m.group(2))
            if len(stats) >= 6:
                break
        return stats

    def _parse_rocksdb_log_stats(self, log_file):
        """Parse the most recent stats dump, summing per-CF metrics across all CFs."""
        try:
            lines = _read_tail_lines(log_file, 3000)
        except Exception as e:
            print("Failed to parse RocksDB LOG file: {}".format(e))
            return {}

        dump_start = -1
        for i in range(len(lines) - 1, -1, -1):
            if 'DUMPING STATS' in lines[i]:
                dump_start = i
                break
        if dump_start < 0:
            return {}

        stats = {
            'user_ingest': 0, 'stall': 0,
            'flush': 0, 'compaction_write': 0, 'compaction_read': 0,
            'pending_bytes': 0, 'write_stall_stops': 0,
        }
        for line in lines[dump_start:]:
            m = _RE_USER_INGEST.search(line)
            if m:
                stats['user_ingest'] = float(m.group(1))
                continue
            m = _RE_STALL_PCT.search(line)
            if m:
                stats['stall'] = float(m.group(1))
                continue
            m = _RE_FLUSH.search(line)
            if m:
                stats['flush'] += float(m.group(1))
                continue
            m = _RE_COMP_WRITE.search(line)
            if m:
                stats['compaction_write'] += float(m.group(1))
                continue
            m = _RE_COMP_READ.search(line)
            if m:
                stats['compaction_read'] += float(m.group(1))
                continue
            m = _RE_PENDING.search(line)
            if m:
                stats['pending_bytes'] += float(m.group(1))
                continue
            m = _RE_STALL_STOPS.search(line)
            if m:
                stats['write_stall_stops'] += float(m.group(1))

        return stats

    def _get_directory_size(self, path):
        total_size = 0
        try:
            for dirpath, _, filenames in os.walk(path):
                for filename in filenames:
                    try:
                        total_size += os.path.getsize(os.path.join(dirpath, filename))
                    except OSError:
                        pass
        except Exception:
            pass
        return total_size

    def save_metrics(self, filename=None):
        if not filename:
            filename = "cdc_performance_{}.json".format(int(self.start_time))

        with self._lock:
            summary = self._calculate_summary()
            data = {
                'summary': summary,
                'raw_metrics': self.metrics,
                'collection_period': time.time() - self.start_time,
            }

        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)

        print("Performance metrics saved to: {}".format(filename))
        return filename

    def _calculate_summary(self):
        if not self.metrics['timestamp']:
            return {}

        timestamps = self.metrics['timestamp']
        duration = timestamps[-1] - timestamps[0]

        MiB = 1024 * 1024
        phys_read = sum(self.metrics['io_read_bytes']) if self.metrics['io_read_bytes'] else 0
        phys_write = sum(self.metrics['io_write_bytes']) if self.metrics['io_write_bytes'] else 0
        logical_read = sum(self.metrics['io_logical_read_bytes']) if self.metrics['io_logical_read_bytes'] else 0
        logical_write = sum(self.metrics['io_logical_write_bytes']) if self.metrics['io_logical_write_bytes'] else 0

        summary = {
            'duration_seconds': duration,
            'sample_count': len(timestamps),
            'avg_cpu_percent': _safe_avg(self.metrics['cpu_percent']),
            'max_cpu_percent': max(self.metrics['cpu_percent']) if self.metrics['cpu_percent'] else 0,
            'avg_memory_percent': _safe_avg(self.metrics['memory_percent']),
            'max_memory_percent': max(self.metrics['memory_percent']) if self.metrics['memory_percent'] else 0,
            'max_memory_rss_mb': max(self.metrics['memory_rss']) / MiB if self.metrics['memory_rss'] else 0,
            'total_physical_read_mb': phys_read / MiB,
            'total_physical_write_mb': phys_write / MiB,
            'avg_physical_read_mbs': phys_read / duration / MiB if duration > 0 else 0,
            'avg_physical_write_mbs': phys_write / duration / MiB if duration > 0 else 0,
            'total_logical_read_mb': logical_read / MiB,
            'total_logical_write_mb': logical_write / MiB,
            'avg_logical_read_mbs': logical_read / duration / MiB if duration > 0 else 0,
            'avg_logical_write_mbs': logical_write / duration / MiB if duration > 0 else 0,
        }

        stor = self.metrics['cdc_storage']
        if stor['write_rate']:
            summary.update({
                'stor_avg_write_rate_mbs': _safe_avg(stor['write_rate']),
                'stor_max_write_rate_mbs': max(stor['write_rate']),
                'stor_avg_read_rate_mbs': _safe_avg(stor['read_rate']),
                'stor_max_read_rate_mbs': max(stor['read_rate']) if stor['read_rate'] else 0,
                'stor_write_total_gb': max(stor['write_total_size']) if stor['write_total_size'] else 0,
                'stor_read_total_gb': max(stor['read_total_size']) if stor['read_total_size'] else 0,
            })

        traf = self.metrics['cdc_traffic']
        if traf['fetcher_mbs']:
            summary.update({
                'fetcher_avg_mbs': _safe_avg(traf['fetcher_mbs']),
                'fetcher_max_mbs': max(traf['fetcher_mbs']),
                'parser_avg_total_mbs': _safe_avg(traf['parser_total_mbs']),
                'parser_avg_remaining_mbs': _safe_avg(traf['parser_remaining_mbs']),
                'parser_avg_filtered_mbs': _safe_avg(traf['parser_filtered_mbs']),
            })

        tp = self.metrics['cdc_throughput']
        if tp['next_record_tps']:
            summary.update({
                'avg_next_tps': _safe_avg(tp['next_record_tps']),
                'max_next_tps': max(tp['next_record_tps']),
                'avg_next_rps': _safe_avg(tp['next_record_rps']),
                'max_next_rps': max(tp['next_record_rps']),
                'avg_release_tps': _safe_avg(tp['release_record_tps']),
                'avg_release_rps': _safe_avg(tp['release_record_rps']),
                'avg_committer_total_tps': _safe_avg(tp['committer_total_tps']),
                'avg_committer_empty_tps': _safe_avg(tp['committer_empty_tps']),
            })

        comp = self.metrics['rocksdb_compaction']
        if comp['write_amp']:
            summary.update({
                'rocksdb_latest_write_amp': comp['write_amp'][-1],
                'rocksdb_avg_write_amp': _safe_avg(comp['write_amp']),
                'rocksdb_latest_user_ingest_gb': comp['user_ingest_gb'][-1] if comp['user_ingest_gb'] else 0,
                'rocksdb_latest_flush_gb': comp['flush_gb'][-1] if comp['flush_gb'] else 0,
                'rocksdb_latest_compaction_write_gb': comp['compaction_write_gb'][-1] if comp['compaction_write_gb'] else 0,
                'rocksdb_latest_compaction_read_gb': comp['compaction_read_gb'][-1] if comp['compaction_read_gb'] else 0,
                'rocksdb_latest_pending_mb': comp['pending_compaction_mb'][-1] if comp['pending_compaction_mb'] else 0,
                'rocksdb_latest_stall_pct': comp['stall_percent'][-1] if comp['stall_percent'] else 0,
            })

        return summary

    def print_summary(self):
        with self._lock:
            summary = self._calculate_summary()
        if not summary:
            print("No performance data collected")
            return

        print("\n=== CDC Performance Report ===")
        print("Monitoring duration: {:.2f}s, Samples: {}".format(
            summary['duration_seconds'], summary['sample_count']))
        print("CPU Usage - Avg: {:.1f}%, Peak: {:.1f}%".format(
            summary['avg_cpu_percent'], summary['max_cpu_percent']))
        print("Memory Usage - Avg: {:.1f}%, Peak: {:.1f}%".format(
            summary['avg_memory_percent'], summary['max_memory_percent']))
        print("Memory RSS - Peak: {}".format(_human_size(summary['max_memory_rss_mb'])))
        print("Logical I/O (incl. page cache) - Read: {} ({}), Write: {} ({})".format(
            _human_size(summary['total_logical_read_mb']),
            _human_rate(summary['avg_logical_read_mbs']),
            _human_size(summary['total_logical_write_mb']),
            _human_rate(summary['avg_logical_write_mbs'])))
        print("Physical I/O (disk actual)   - Read: {} ({}), Write: {} ({})".format(
            _human_size(summary['total_physical_read_mb']),
            _human_rate(summary['avg_physical_read_mbs']),
            _human_size(summary['total_physical_write_mb']),
            _human_rate(summary['avg_physical_write_mbs'])))

        if 'fetcher_avg_mbs' in summary:
            print("\nNetwork Traffic:")
            print("  Fetcher Pull  - Avg: {}, Peak: {}".format(
                _human_rate(summary['fetcher_avg_mbs']),
                _human_rate(summary['fetcher_max_mbs'])))
            print("  Parser Total   - Avg: {}".format(
                _human_rate(summary['parser_avg_total_mbs'])))
            print("  Parser Remaining   - Avg: {}, Filtered: {}".format(
                _human_rate(summary['parser_avg_remaining_mbs']),
                _human_rate(summary['parser_avg_filtered_mbs'])))

        if 'avg_next_tps' in summary:
            print("\nInput/Output Throughput:")
            print("  Output TPS - Avg: {:.1f}, Peak: {:.1f}".format(
                summary['avg_next_tps'], summary['max_next_tps']))
            print("  Output RPS - Avg: {:.1f}, Peak: {:.1f}".format(
                summary['avg_next_rps'], summary['max_next_rps']))
            print("  Release TPS - Avg: {:.1f}, Release RPS - Avg: {:.1f}".format(
                summary['avg_release_tps'], summary['avg_release_rps']))
            print("  Committer - Total TPS: {:.1f}, Empty Tx TPS: {:.1f}".format(
                summary['avg_committer_total_tps'], summary['avg_committer_empty_tps']))

        if 'stor_avg_write_rate_mbs' in summary:
            print("\nRocksDB Storage:")
            print("  Write Rate - Avg: {}, Peak: {}".format(
                _human_rate(summary['stor_avg_write_rate_mbs']),
                _human_rate(summary['stor_max_write_rate_mbs'])))
            print("  Read Rate - Avg: {}, Peak: {}".format(
                _human_rate(summary['stor_avg_read_rate_mbs']),
                _human_rate(summary['stor_max_read_rate_mbs'])))
            print("  Total Write: {:.2f}GB, Total Read: {:.2f}GB".format(
                summary['stor_write_total_gb'], summary['stor_read_total_gb']))

        if 'rocksdb_latest_write_amp' in summary:
            print("\nRocksDB Compaction:")
            print("  Write Amplification: {:.2f} (Avg: {:.2f})".format(
                summary['rocksdb_latest_write_amp'], summary['rocksdb_avg_write_amp']))
            print("  User Ingest: {:.2f}GB, Flush: {:.2f}GB".format(
                summary['rocksdb_latest_user_ingest_gb'],
                summary['rocksdb_latest_flush_gb']))
            print("  Compaction Write: {:.2f}GB, Compaction Read: {:.2f}GB".format(
                summary['rocksdb_latest_compaction_write_gb'],
                summary['rocksdb_latest_compaction_read_gb']))
            print("  Pending compaction: {}, Stall: {:.1f}%".format(
                _human_size(summary['rocksdb_latest_pending_mb']),
                summary['rocksdb_latest_stall_pct']))

        print("=" * 40)


def _read_pid_file(path):
    """Read a PID from file and validate the process is alive. Returns pid or None on error."""
    if not os.path.isfile(path):
        print("Error: PID file does not exist: {}".format(path))
        return None
    try:
        content = open(path, 'r').read().strip()
    except OSError as e:
        print("Error: Cannot read PID file {}: {}".format(path, e))
        return None
    if not content:
        print("Error: PID file is empty: {}".format(path))
        return None
    try:
        pid = int(content)
    except ValueError:
        print("Error: PID file content is not a valid number: '{}'".format(content))
        return None
    if not psutil.pid_exists(pid):
        print("Error: Process with PID {} does not exist (exited)".format(pid))
        return None
    try:
        proc = psutil.Process(pid)
        if proc.status() == psutil.STATUS_ZOMBIE:
            print("Error: Process with PID {} is a zombie process".format(pid))
            return None
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        print("Error: Cannot access process with PID {}".format(pid))
        return None
    print("Read PID from file {}: {} ({})".format(path, pid, proc.name()))
    return pid


def main():
    if len(sys.argv) < 2:
        print("Usage: python performance_monitor.py monitor [process_name|PID] [options]")
        print("")
        print("Function: Real-time monitoring of CDC process performance metrics, including CPU, memory, I/O, and RocksDB stats")
        print("")
        print("Arguments:")
        print("  process_name|PID        - Optional, process name or PID to monitor, defaults to searching for '{}'".format(DEFAULT_PROCESS_NAME))
        print("  --pid-file file   - Optional, read CDC process PID from file (file should contain only one PID number)")
        print("  --cdc-dir dir    - Optional, specify CDC running directory (RocksDB stored in its storage subdirectory)")
        print("  --cdc-log file    - Optional, specify CDC log file path")
        print("")
        print("Examples:")
        print("  python performance_monitor.py monitor")
        print("  python performance_monitor.py monitor obcdc_tailf")
        print("  python performance_monitor.py monitor 12345")
        print("  python performance_monitor.py monitor --pid-file /home/admin/cdc_run/run/obcdc.pid")
        print("  python performance_monitor.py monitor --cdc-dir /home/admin/cdc_run")
        return

    command = sys.argv[1]

    if command == "monitor":
        process_identifier = None
        cdc_log_file = None
        cdc_work_dir = None
        pid_file = None

        i = 2
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == "--cdc-log" and i + 1 < len(sys.argv):
                cdc_log_file = sys.argv[i + 1]
                i += 2
            elif arg == "--cdc-dir" and i + 1 < len(sys.argv):
                cdc_work_dir = sys.argv[i + 1]
                i += 2
            elif arg == "--pid-file" and i + 1 < len(sys.argv):
                pid_file = sys.argv[i + 1]
                i += 2
            else:
                process_identifier = arg
                i += 1

        pid = None
        process_name = None

        if pid_file:
            pid = _read_pid_file(pid_file)
            if pid is None:
                return
        elif process_identifier:
            try:
                pid = int(process_identifier)
            except ValueError:
                process_name = process_identifier

        monitor = CDCPerformanceMonitor(
            process_name=process_name, pid=pid,
            cdc_log_file=cdc_log_file, cdc_work_dir=cdc_work_dir,
        )

        if monitor.start_monitoring():
            try:
                print("Monitoring... Press Ctrl+C to stop monitoring and save results")
                last_log_check = 0
                last_rocksdb_check = 0
                while True:
                    current_time = time.time()
                    if current_time - last_log_check >= 10:
                        monitor.collect_cdc_log_stats()
                        last_log_check = current_time
                    if current_time - last_rocksdb_check >= 10:
                        monitor.collect_rocksdb_stats()
                        last_rocksdb_check = current_time
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping monitoring...")
                monitor.stop_monitoring()
                monitor.save_metrics()
                monitor.print_summary()
        else:
            print("Failed to start monitoring")

    else:
        print("Unknown command: {}".format(command))
        print("Use 'python performance_monitor.py' for help")


if __name__ == "__main__":
    main()
