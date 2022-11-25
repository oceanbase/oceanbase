#!/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'dongyun.zdy'

import datetime
import multiprocessing
import MySQLdb
import Queue
import signal
import re
import argparse
import time
import sys
import subprocess as sp
import os

outfile = 'miss.result'
schema_file = 'miss.schema'
if os.path.exists(outfile):
    os.remove(outfile)


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
    run_cmd('echo "# %s" >> ' % s + outfile)
    write_schema(s)


def run_cmd(cmd):
    # print cmd
    res = ''
    p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    while True:
        line = p.stdout.readline()
        res += line
        if line:
            # print line.strip()
            sys.stdout.flush()
        else:
            break
    p.wait()
    return res


#cmd_form1 = 'LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -BGK -t material -s miss.schema -r 500000'.split()
cmd_form1 = 'LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -GK -t material -s miss.schema -r 500000'.split()

types_to_test = {'bigint': 'bigint', 'double': 'double', 'float': 'float', 'timestamp': 'timestamp',
                 'number': 'number(20,3)', 'v32': 'varchar(32)', 'v64': 'varchar(64)', 'v128': 'varchar(128)'}
row_counts = [1000, 2000, 4000, 7000, 8000, 10000, 20000, 50000]
input_col_cnts = [1, 2, 3, 6]
case_run_time = 7

total_case_count = len(row_counts) * len(input_col_cnts)
case_count = 0

print "Total case count %s ..." % (total_case_count)
for col_count in input_col_cnts:
    make_schema(sorted(types_to_test.values()) * col_count)
    for row_count in row_counts:
        cmd_form1[-1] = str(row_count)
        case_count += 1
        prompt = "Running case %s / %s ... : %s " % (case_count, total_case_count, " ".join(cmd_form1))
        print prompt
        sp.check_call('echo "### %s" >> ' % prompt + outfile, shell=True)
        caseinfo = '%d,%d,' % (row_count, col_count)
        for t in xrange(case_run_time):
            print t
            res = caseinfo + run_cmd(" ".join(cmd_form1) + " -i3").strip()
            run_cmd('echo "%s" >> ' % (res) + outfile)
        for t in xrange(case_run_time):
            print t
            res = caseinfo + run_cmd(" ".join(cmd_form1) + " -i4").strip()
            run_cmd('echo "%s" >> ' % (res) + outfile)
