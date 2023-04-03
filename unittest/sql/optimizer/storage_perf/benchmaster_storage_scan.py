#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
import sys
from random import randint
import re

txt = """1,8712
PerfStat: scan_end_time: 2016-08-22T12:03:35Z
PerfStat: --- scan 1 iteration 0 run ---
PerfStat: runtime = 8712
PerfStat: index_cache_hit_ = 0 , index_cache_miss_ = 1 ,
PerfStat: row_cache_hit_ = 0 , row_cache_miss_ = 1 ,
PerfStat: block_cache_hit_ = 0 , block_cache_miss_ = 1 ,
PerfStat: bf_cache_hit_ = 0 , bf_cache_miss_ = 1 ,
PerfStat: io_read_count_ = 2 , io_read_size_ = 24576 , io_read_delay_ = 133, io_read_queue_delay = 17,
PerfStat: io_read_cb_alloc_delay = 43, io_read_cb_process_delay_ = 35 ,
PerfStat: io_read_prefetch_micro_cnt = 1, io_read_prefetch_micro_size_ = 16444,
PerfStat: io_read_uncomp_micro_cnt_ = 0, io_read_uncomp_micro_size_ = 0,
PerfStat: total_delay = 473, avg_delay = 236,
PerfStat: db_file_data_read_:
PerfStat: total_waits_ = 1, total_timeouts_ = 0
PerfStat: time_waited_micro_ = 114, average_wait_ = 114.00, max_wait_ = 114
PerfStat: db_file_data_index_read_:
PerfStat: total_waits_ = 1, total_timeouts_ = 0
PerfStat: time_waited_micro_ = 90, average_wait_ = 90.00, max_wait_ = 90
PerfStat: kv_cache_bucket_lock_wait_:
PerfStat: total_waits_ = 0, total_timeouts_ = 0
PerfStat: time_waited_micro_ = 0, average_wait_ = 0.00, max_wait_ = 0
PerfStat: io_queue_lock_wait_:
PerfStat: total_waits_ = 0, total_timeouts_ = 0
PerfStat: time_waited_micro_ = 0, average_wait_ = 0.00, max_wait_ = 0
PerfStat: io_controller_cond_wait_:
PerfStat: total_waits_ = 0, total_timeouts_ = 0
PerfStat: time_waited_micro_ = 0, average_wait_ = 0.00, max_wait_ = 103147
PerfStat: io_processor_cond_wait_:
PerfStat: total_waits_ = 0, total_timeouts_ = 0
PerfStat: time_waited_micro_ = 0, average_wait_ = 0.00, max_wait_ = 0"""

def get_kv(txt, k):
    pat = re.compile(k +":\s*?PerfStat: total_waits_ = (\d+)[\w\d\s#:_,=]+?PerfStat: time_waited_micro_ = (\d+)")
    return pat.findall(txt)

def remove_schema():
    global schema_file
    if os.path.exists(schema_file):
        os.remove(schema_file)

def write_schema(s):
    global schema_file
    of = open(schema_file, 'w')
    of.write(s)
    of.close()


def make_seq(t, cnt):
    types = [t]
    types *= cnt
    return types

schema_file = 'scan.schema'
def make_schema(types):
    global schema_file
    remove_schema()
    col_id = 0
    s = "create table t1 ("
    for t in types:
        s += "c%d %s, " % (col_id, t)
        col_id += 1
    s = s[:-2]
    s += ', primary key (c1))'
    print s
    write_schema(s)


types = {'bi':'bigint', 'vc32':'varchar(32)', 'vc128':'varchar(128)', 'db':'double', 'ts':'timestamp', 'nb':'number'}


types_to_test = ['bi', 'vc32', 'db', 'ts', 'nb']
col_type_repeat_times = 10
table_width_factors = [3, 2, 1]





def run_cmd(cmd):
    # print cmd
    res = ''
    p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    while True:
        line = p.stdout.readline()
        res += line
        if line:
            #print line.strip()
            sys.stdout.flush()
        else:
            break
    p.wait()
    return res

outfile_name = 'scan'

cmd_form = './storage_perf_cost -G -C 1 -s scan.schema -T 10 -r 1000 -Y S -E -I'.split()

rows_min = 1
rows_max = 1000001

row_counts = sorted(list(set(range(2, 1002, 100) + range(1002, 10002, 1000) + range(10002, 100002, 10000) + range(100002, 1000003, 100000))))
col_counts = [1,10,20,40,50]
modes = ['W', 'NORMAL']
mode_ids = {'W':1, 'NORMAL':2}

total_count = len(row_counts) * len(col_counts) * len(modes) * len(table_width_factors)
count = 0


for table_width_factor in table_width_factors:
    seq = []
    for t in types_to_test:
        seq.extend([types[t] for i in range(col_type_repeat_times)])
    make_schema(seq * table_width_factor)
    run_cmd('./storage_perf_cost -G -C 1 -s scan.schema -T 1 -r %d -Y S -R' % (2 * rows_max))

    for mode in modes:

        outfile = outfile_name + '.' + mode + '.w%d.res' % table_width_factor
        if os.path.exists(outfile):
            os.remove(outfile)

        for col_count in col_counts:
            for rc in row_counts:

                prop = '%d,%d,%d,' % (rc, col_count, table_width_factor)

                count += 1
                cmd_form[9] = str(rc)
                cmd_form[3] = str(col_count)
                if mode == 'NORMAL':
                    cmd_form[12] = ''
                else:
                    cmd_form[12] = '-' + mode
                cmd = ' '.join(cmd_form)
                print '%d / %d : ' % (count, total_count) + cmd
                run_cmd('echo "# %s" >> ' % cmd + outfile)
                cmd_res = filter(lambda x : x.strip() != '', run_cmd(cmd).strip().split('\n\n'))
                if len(cmd_res) == 0:
                    run_cmd('echo "# error" >> ' + outfile)
                for runinfo in cmd_res:
                    # print runinfo
                    waitinfo = get_kv(runinfo, 'db_file_data_read_')
                    # print waitinfo
                    lines = runinfo.splitlines()
                    resline = prop + lines[1] + ',' + ','.join(list(waitinfo[0]))
                    print resline
                    run_cmd('echo "%s" >> ' % (resline) + outfile)
                    for statline in lines[2:]:
                        run_cmd('echo "# %s" >> ' % statline + outfile)
