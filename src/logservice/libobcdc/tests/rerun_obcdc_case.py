#!/usr/bin/python

import argparse
import os
import re
import time
import shutil
import sys
import datetime
import logging

logging.basicConfig(filename='rerun_obcdc_case.log', encoding='utf-8', level=logging.DEBUG)
parser = argparse.ArgumentParser(prog='rerun_obcdc_case.py', epilog='NOTE: the script could \
                                only be executed in the working dir which OBCDC execute previously, \
                                the default output log is rerun_obcdc_case.log in current directory')

parser.add_argument('-T', '--start_ts_usec', type=int, default=-1,
                    help="Specify the start timestamp in usec of OBCDC, \
                          default is start_ts_us specified in previous case")
parser.add_argument('-C', '--count', type=int, default=10,
                    help="Specify the number of rounds to run for OBCDC test case, default run 10 rounds")
parser.add_argument('-M', '--max_time', type=str, default='10m',
                    help="Specify the MAX time the case could run, if the running time exceed the max time,\
                          the results is regarded as failed. The max time could be specified in seconds, minitus or hours, \
                          For example 270s, 1m, or 1h, if no time is specified, the script specify max time in seconds \
                          by default.")
parser.add_argument('-L', "--collect_logs", type=bool, default=True,
                    help="Speicify whether log need to be collected if a test fails, default True")
parser.add_argument('-D', "--daemon", type=bool, default=False,
                    help="Specify whether the script run as a daemon process, default False")

TIME_USEC_CONVERSION = 1000 * 1000
last_print_heartbeat_ts_us = -1

def daemonize():
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print("fork #1 failed: {}".format(e))
        sys.exit(1)
    os.setsid()
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print("fork #2 failed: {}".format(e))
        sys.exit(1)
    os.umask(0)
    sys.stdout.flush()
    sys.stderr.flush()
    os.close(sys.stdin.fileno())
    os.close(sys.stdout.fileno())
    os.close(sys.stderr.fileno())

def remove_previous_logs():
  if os.path.exists('log'):
    try:
      files = os.listdir('log')
      for file in files:
        if file.startswith('libobcdc.log'):
          os.remove(os.path.join('log', file))
    except Exception as e:
      logging.warn("remove_previous_logs failed, exception: {}".format(e))

def get_previous_start_ts():
  start_ts_ns = -1000
  log_dir = 'log'
  try:
    files = os.listdir(log_dir)
    start_ts_pattern = re.compile("init obcdc succ\(.+, start_tstamp_ns=(\d+)")

    for file in files:
      if file.startswith('libobcdc.log'):
        complete_file_path = os.path.join(log_dir, file)
        with open(complete_file_path, 'r') as f:
          for line in f:
            result = start_ts_pattern.search(line)
            if result is not None:
              start_ts_ns = int(result.group(1))
  except Exception as e:
    logging.warn("get_previous_start_ts failed, exception: {}".format(e))

  return int(start_ts_ns/1000)

def time_to_sec(time_val, time_unit):
  if time_unit in ['', 's', 'sec']:
    return time_val
  elif time_unit in ['m', 'min']:
    return time_val * 60
  else:
    return time_val * 3600

def parse_max_time(max_time_str):
  max_time = -1
  max_time_pattern = re.compile("^(\d+)(s|m|h|S|M|H|sec|min)?$")
  result = max_time_pattern.match(max_time_str)
  if result is not None:
    time_val = int(result.group(1))
    time_unit = ''
    if result.group(2) is not None:
      time_unit = str(result.group(2)).lower()
    max_time = time_to_sec(time_val, time_unit)
  return max_time

def subprocess_exists(pid):
  is_exists = False
  try:
    os.kill(pid, 0)
    is_exists = True
  except Exception as e:
    logging.info('subprocess may not exist, pid: %d' % pid)
    is_exists = False
  return is_exists

def obcdc_near_realtime():
  global last_print_heartbeat_ts_us
  heartbeat_file = 'obcdc/heartbeat.log'
  is_realtime = False
  if os.path.exists(heartbeat_file):
    try:
      with open(heartbeat_file, 'r') as f:
        heartbeat = f.readline().strip()
        heartbeat_time = int(heartbeat)
        curr_ts_us = int(time.time() * TIME_USEC_CONVERSION)
        if curr_ts_us - last_print_heartbeat_ts_us >= 10 * TIME_USEC_CONVERSION:
          logging.debug(heartbeat_time)
          last_print_heartbeat_ts_us = curr_ts_us
        if abs(time.time() * TIME_USEC_CONVERSION - heartbeat_time) < 1 * TIME_USEC_CONVERSION:
          is_realtime = True
        else:
          is_realtime = False
    except Exception as e:
      logging.warn("check near realtime failed, exception: {}".format(e))
  else:
    logging.info("heartbeat file not exists")
  return is_realtime

def collect_logs(round):
  try:
    time_ts_us = int(time.time() * TIME_USEC_CONVERSION)
    log_dir = "log_round{}_ts{}".format(round, time_ts_us)
    os.mkdir(log_dir)
    files = os.listdir('log')
    for file in files:
      if file.startswith("libobcdc.log"):
        src_file = os.path.join("log", file)
        dest_file = os.path.join(log_dir, file)
        shutil.copy(src_file, dest_file)
  except Exception as e:
    logging.warn("collectd_logs failed, exception: {}".format(e))

def monitor_cdc_process(round, pid, deadline, enable_collect_logs):
  cur_ts_us = int(time.time() * TIME_USEC_CONVERSION)
  need_stop = False
  stopped = False
  collect_log = False
  ddl_time_datetime = datetime.datetime.fromtimestamp(float(deadline)/TIME_USEC_CONVERSION)
  logging.info("start monitor process, round: {}, pid {}, deadline: {}".format(round, pid, ddl_time_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')))
  while cur_ts_us < deadline and not need_stop:
    if not subprocess_exists(pid):
      need_stop = True
      stopped = True
      collect_log = True
      logging.info("subprocess not exist, pid: {}".format(pid))
    elif obcdc_near_realtime():
      need_stop = True
    else:
      time.sleep(1)
      cur_ts_us = int(time.time() * TIME_USEC_CONVERSION)
  if cur_ts_us >= deadline:
    logging.info("exceed deadline, deadline: {}".format(ddl_time_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')))
    collect_log = True

  if not stopped:
    try:
      os.kill(pid, 9)
    except Exception as e:
      logging.warn("exception occurs, kill process {} failed, exception: {}".format(pid, e))
  child_proc_info = os.wait()
  logging.debug("child_proc_info {}".format(child_proc_info))
  if enable_collect_logs and collect_log:
    collect_logs(round)
  return collect_log

if __name__ == "__main__":
  args = parser.parse_args()
  start_ts_us = args.start_ts_usec
  max_time_per_round_str = args.max_time
  max_time_per_round = parse_max_time(max_time_per_round_str)
  if max_time_per_round_str == -1:
    print("maybe time format is not correct")
    exit(1)
  enable_collect_logs = args.collect_logs
  need_daemonlize = args.daemon
  round = args.count
  if start_ts_us == -1:
    start_ts_us = get_previous_start_ts()

  if need_daemonlize:
    daemonize()

  failed_round = []
  for i in range(round):
    # avoid immediately check real time success
    time.sleep(2)
    try:
      pid = os.fork()
      if pid == 0:
        remove_previous_logs()
        os.execl('obcdc/obcdc_tailf', 'obcdc_tailf', '-f', 'obcdc/libobcdc.conf',
                '-H', 'obcdc/heartbeat.log', '-T', str(start_ts_us))
      else:
        deadline = int(time.time() * TIME_USEC_CONVERSION) + max_time_per_round * TIME_USEC_CONVERSION
        if monitor_cdc_process(i, pid, deadline, enable_collect_logs):
          failed_round.append(i)
        else:
          logging.info("round {} test success".format(i))
    except Exception as e:
      logging.warn("error on fork process, exception:{}".format(e))
      failed_round.append(i)

  logging.info("test finish, total test round: {}, failed round count: {}, failed rounds: {}".format(
        round, len(failed_round), failed_round))