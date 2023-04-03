#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
import sys
import getopt
import time

ISOTIMEFORMAT = '%Y-%m-%d %X'

# cmd_form = "./cost_model_util -t mg -B -s c10k1.schema -r 10000 -Z 1 -V 10 -e 1 -o 10 -p 1 >> out_file"
cmd_form = "./cost_model_util -t mg -G -s c10k1.schema -r 10000 -Z 1 -V 10 -e 1 -o 10 -p 1 >> mergegroupby_result"
cmd_elements = cmd_form.split(" ")

row_counts = [10, 30, 50, 70, 100, 1000, 5000, 10000]
steps = [1, 3, 5, 10, 20]
aggr_funcs = [1, 4, 7, 10]
group_cols = [1, 4, 7, 10]
non_group_cols = [10]

case_run_time = 7

total_case_count = len(row_counts)
total_case_count *= len(steps)
total_case_count *= len(aggr_funcs)
total_case_count *= len(group_cols)
total_case_count *= len(non_group_cols)
total_case_count *= case_run_time

print total_case_count
wrong_arg = False

out_file_name = "mergegroupby_result"
if os.path.exists(out_file_name):
    os.remove(out_file_name)
opts, args = getopt.getopt(sys.argv[1:], "o:")
for op, value in opts:
    if "-o" == op:
        out_file_name = value
    else:
        wrong_arg = True

if wrong_arg:
    print "wrong arg"
    sys.exit(1)

case_count = 0
cmd_elements[-1] = out_file_name
if os.path.exists(out_file_name):
    os.remove(out_file_name)

print "Total case count %s ..." % (total_case_count)

for row_count in row_counts:
    for step in steps:
        for aggr_func in aggr_funcs:
            for group_col in group_cols:
                for non_group_col in non_group_cols:
                    for run_time in xrange(case_run_time):

                        cmd_elements[7] = str(row_count)
                        cmd_elements[11] = str(step)
                        cmd_elements[13] = str(aggr_func)
                        cmd_elements[15] = str(group_col)
                        cmd_elements[17] = str(non_group_col)
                        cmd_elements[19] = out_file_name

                        param = ",".join([cmd_elements[7],
                                          cmd_elements[11],
                                          cmd_elements[13],
                                          cmd_elements[15],
                                          cmd_elements[17]]) + ","

                        prompt = "%s Running case %s / %s ... : %s " % (
                        time.strftime(ISOTIMEFORMAT, time.localtime()), case_count, total_case_count,
                        " ".join(cmd_elements))
                        print prompt

                        case_count += 1

                        sp.check_call("echo '#%s' >> %s" % (prompt, out_file_name), shell=True)

                        if group_col <= non_group_col:
                            sp.check_call("echo -n '%s' >> %s" % (param, out_file_name), shell=True)
                            sp.check_call(" ".join(cmd_elements), shell=True)
                        else:
                            sp.check_call("echo '#%s skipped' >> %s" % (param, out_file_name), shell=True)
