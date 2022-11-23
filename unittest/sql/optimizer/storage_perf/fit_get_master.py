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


prep_file_name = 'scan.cache.prep'
fit_file_name = 'scan.cache.fit'

col_counts = [1, 20, 40, 50]
modes = ['WE', 'E', 'W']
mode_ids = {'WE':1, 'E':2, 'W':3}
mode_names = {'WE':'io', 'E':'bc', 'W':'rc'}

prep_cmd = './preprocess.py -i scan.NORMAL.res -o scan.cache.prep -t 10 -C 4 -d'.split()

for mode in modes:
    mode_name = mode_names[mode]
    prep_cmd[2] = 'get.' + mode + '.res'
    prep_cmd[4] = 'get.' + mode_name + '.prep'
    run_cmd(' '.join(prep_cmd))
    for col_count in col_counts:
        prep_cmd[4] = 'get.' + mode_name + '.prep.' + str(col_count)
        run_cmd(' '.join(prep_cmd) + ' -f 1,e,%d' % col_count)


fit_cmd = './fit_get.py -i aaa -o bbb'.split()
apply_cmd = './apply_get_model.py -i aaa -o bbb -m mmm'.split()


for mode in modes:
    mode_name = mode_names[mode]
    fit_cmd[2] = 'get.' + mode_name + '.prep'
    fit_cmd[4] = 'get.' + mode_name + '.model'
    run_cmd(' '.join(fit_cmd))
    for col_count in col_counts:
        apply_cmd[2] = 'get.' + mode_name + '.prep.' + str(col_count)
        apply_cmd[4] = 'get.' + mode_name + '.fit.' + str(col_count)
        apply_cmd[6] = 'get.' + mode_name + '.model'
        run_cmd(' '.join(apply_cmd))










