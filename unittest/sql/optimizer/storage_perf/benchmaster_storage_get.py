#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
import sys

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


schema_file = 'scan.schema'
outfile_name = 'get'

cmd_form = './storage_perf_cost -G -C 1 -s scan.schema -T 10 -r 1000 -Y M -U2000000 -E -I'.split()

rows_min = 2
rows_max = 1003
step = 50
col_counts = [1, 20, 40, 50]
modes = ['WE', 'E', 'W']
mode_ids = {'WE':1, 'E':2, 'W':3}

total_count = ((rows_max - rows_min) / step) * len(col_counts) * len(modes)
count = 0
for mode in modes:

    outfile = outfile_name + '.' + mode + '.res'
    if os.path.exists(outfile):
        os.remove(outfile)

    for col_count in col_counts:
        for rc in range(rows_min, rows_max + 1, step):

            prop = '%d,%d,%d,' % (rc, col_count, mode_ids[mode])

            count += 1
            cmd_form[9] = str(rc)
            cmd_form[3] = str(col_count)
            cmd_form[13] = '-' + mode
            cmd = ' '.join(cmd_form)
            print '%d / %d : ' % (count, total_count) + cmd
            run_cmd('echo "# %s" >> ' % cmd + outfile)
            cmd_res = filter(lambda x : x.strip() != '', run_cmd(cmd).strip().split('\n\n'))
            if len(cmd_res) == 0:
                run_cmd('echo "# error" >> ' + outfile)
            for runinfo in cmd_res:
                lines = runinfo.splitlines()
                resline = prop + lines[1]
                print resline
                run_cmd('echo "%s" >> ' % (resline) + outfile)
                for statline in lines[2:]:
                    run_cmd('echo "# %s" >> ' % statline + outfile)







            # res_lines = [prop + line for line in run_cmd(cmd).strip().splitlines()][1:]








