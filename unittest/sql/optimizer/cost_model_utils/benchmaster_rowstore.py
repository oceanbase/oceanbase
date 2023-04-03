#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os

schema_file = 'rowstore.schema'
outfile = 'rowstore.result'


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


cmdform = 'LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -RBGK -t material -s rowstore.schema -r 10 -i1'.split()


types_to_test = {'bigint':'bigint', 'double':'double', 'float':'float', 'timestamp':'timestamp', 'number':'number(20,3)', 'v32':'varchar(32)', 'v64':'varchar(64)', 'v128':'varchar(128)'}
row_counts = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 50000]
col_nums = [1, 3, 20, 50]
case_run_time = 7

total_case_count = len(types_to_test) * len(row_counts) * len(col_nums)
case_count = 0

if os.path.exists(outfile):
    os.remove(outfile)

# for t in types_to_test:
#     outfile = 'rowstore.result.' + t
#     if os.path.exists(outfile):
#         os.remove(outfile)
#     for n in col_nums:
#         make_schema(make_seq(types_to_test[t], n))
#         for rc in row_counts:
#             cmdform[8] = str(rc)
#             case_count += 1
#             prompt = "# %d / %d %s col_cnt = %d rc = %d \n# %s" % (case_count, total_case_count, t, n, rc, ' '.join(cmdform))
#             print prompt
#             sp.check_call('echo "%s" >> ' % prompt + outfile, shell=True)
#             for times in xrange(0, case_run_time):
#                 print times
#                 sp.check_call("echo -n '%s,' >> " % str(rc) + outfile, shell=True)
#                 sp.check_call("echo -n '%s,' >> " % str(n) + outfile, shell=True)
#                 sp.check_call(" ".join(cmdform) + ' >> ' + outfile, shell=True)

make_schema(make_seq('bigint', 50))


