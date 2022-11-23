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
fit_file_name = 'scan.io.fit'

col_counts = [1,10,20,40,50]

size_factors = [1,2,3]

run_cmd('cat scan.W.w1.res > scan.W.res')
run_cmd('cat scan.W.w2.res >> scan.W.res')
run_cmd('cat scan.W.w3.res >> scan.W.res')

prep_cmd = './preprocess.py -i scan.W.res -o scan.W.small.prep -t 10 -C 4 -S -d'.split()

prep_cmd[4] = prep_file_name
run_cmd(' '.join(prep_cmd))
for col_count in col_counts:
    for size_factor in size_factors:
        prep_cmd[4] = prep_file_name + ".w%d.%d" % (size_factor, col_count)
        run_cmd(' '.join(prep_cmd) + ' -f 1,e,%d -f 2,e,%d' % (col_count, size_factor))


run_cmd('./fit_scan_io.py -i scan.io.prep -o scan.io.fit -m scan.io.model')

for col_count in col_counts:
    for size_factor in size_factors:
        run_cmd('./apply_scan_io_model.py -i %s -o %s -m %s' % (prep_file_name + ".w%d.%d" % (size_factor, col_count),
                                                                fit_file_name + ".w%d.%d" % (size_factor, col_count),
                                                                'scan.io.model'))


