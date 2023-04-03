#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
import sys

def run_cmd(cmd):
    print cmd
    res = ''
    p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    while True:
        line = p.stdout.readline()
        res += line
        if line:
            print line.strip()
            sys.stdout.flush()
        else:
            break
    p.wait()
    return res


prep_file_name = 'scan.io.prep'
fit_file_name = 'io.fit'

col_counts = [1,10,20,40,50]

prep_cmd = './preprocess.py -i scan.W.res -o io.prep -t 10 -C 4 -S -d'.split()

prep_cmd[4] = prep_file_name
run_cmd(' '.join(prep_cmd))
prep_cmd.extend('-f 1,e,'.split())
for col_count in col_counts:
    prep_cmd[4] = prep_file_name + "." + str(col_count)
    run_cmd(' '.join(prep_cmd) + str(col_count))

run_cmd('./fit_io.py -i scan.io.prep -o io.fit -m io.model')

for col_count in col_counts:
    run_cmd('./apply_io_model.py -i %s -o %s -m %s' % (prep_file_name + '.' + str(col_count),
                                                       fit_file_name + '.' + str(col_count),
                                                       'io.model'))


