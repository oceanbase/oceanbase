#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
from cost_test_conf import Config

schema_file = 'sort.schema'
outfile = 'sort.result'


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


def make_schema(types):
    global schema_file
    remove_schema()
    col_id = 1
    s = "create table t1 ("
    for t in types:
        s += "c%d %s, " % (col_id, t)
        col_id += 1
    s = s[:-2]
    s += ', primary key (c1))'
    print s
    write_schema(s)

if os.path.exists("sort_result"):
    os.remove("sort_result")

#cmd_form = "LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -GBR -t sort -s c20.schema -r 1000 -c 10  -i4".split()
cmd_form = "LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -GR -t sort -s c20.schema -r 1000 -c 10  -i4".split()


types_to_test = {'bigint':'bigint', 'double':'double', 'float':'float', 'timestamp':'timestamp', 'number':'number(20,3)', 'v32':'varchar(32)', 'v64':'varchar(64)', 'v128':'varchar(128)'}
row_counts = [1000, 2000, 4000, 8000, 10000, 20000, 50000]
sort_column_counts = [1, 2, 3, 5]
input_col_cnts = [1, 2, 6]
case_run_time = 7

keys = sorted(types_to_test.keys())

total_case_count = len(row_counts) * len(sort_column_counts) * len(input_col_cnts) * len(keys)
case_count = 0

cmd_form[6] = schema_file




def make_headed_seq(head, arr):
    a = [head] + arr[0:arr.index(head)] + arr[arr.index(head) + 1:]
    b = [types_to_test[i] for i in a]
    return b

#for t in keys:
if Config.u_to_test_type is not None:
    #outfile = 'sort.result.' + t
    t = Config.u_to_test_type
    outfile = 'sort_add_' + t + '_' + 'result'
    if os.path.exists(outfile):
        os.remove(outfile)
    for n in input_col_cnts:
        make_schema(make_headed_seq(t, keys) * n)
        for rc in row_counts:
            cmd_form[8] = str(rc)
            for order_count in sort_column_counts:
                cmd_form[-2] = str(order_count)
                case_count+=1
                prompt = "# %d / %d  %s col_cnt = %d rc = %d order_cnt = %d\n# %s" % (case_count, total_case_count, t, n * len(keys), rc, order_count, ' '.join(cmd_form))

                print prompt
                sp.check_call('echo "%s" >> ' % prompt + outfile, shell=True)
                if order_count > n * len(keys):
                    print 'PASS'
                    sp.check_call('echo "# PASS" >> '  + outfile, shell=True)
                    continue
                for times in xrange(0, case_run_time):
                    print times
                    sp.check_call("echo -n '%s,' >> " % str(rc) + outfile, shell=True)
                    sp.check_call("echo -n '%s,' >> " % str(n) + outfile, shell=True)
                    sp.check_call("echo -n '%s,' >> " % str(order_count) + outfile, shell=True)
                    sp.check_call(" ".join(cmd_form) + ' >> ' + outfile, shell=True)
