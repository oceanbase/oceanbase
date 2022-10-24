#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
import sys
import getopt
import time

ISOTIMEFORMAT='%Y-%m-%d %X'

# cmd_form = "./cost_model_util -B -s c10k1x2.schema -t nestloop -r 900 -r 900 -Z1 -Z1 -C 2 -C 2 -V 3 -V 3 >> res"
cmd_form = "./cost_model_util -G -s c10k1x2.schema -t nestloop -r 900 -r 900 -Z1 -Z1 -C 2 -C 2 -V 3 -V 3 >> nl_result"
cmd_elements = cmd_form.split(" ")

row_count_max = 10001
row_count_step = 100


left_row_counts = [10, 100, 500, 1000]
right_row_counts = [10, 100, 500, 1000]

left_steps = [1, 3, 4, 5, 7, 10]
right_steps = [1, 3, 4, 5, 7, 10]

left_step_lengths = [1, 2, 4, 5, 10]
right_step_lengths = [1, 2, 4, 5, 10]

case_run_time = 7

total_case_count = len(left_row_counts)
total_case_count *= len(right_row_counts)
total_case_count *= len(left_steps)
total_case_count *= len(right_steps)
total_case_count *= len(left_step_lengths)
total_case_count *= len(right_step_lengths)
total_case_count *= case_run_time


wrong_arg = False

#out_file_name = "nestloop_result"
out_file_name = "nl_result"
opts,args = getopt.getopt(sys.argv[1:],"o:")
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
for left_row_count in left_row_counts:
    for right_row_count in right_row_counts:
        for left_step in left_steps:
            for right_step in right_steps:
                for left_step_length in left_step_lengths:
                    for right_step_length in right_step_lengths:
                        for i in xrange(case_run_time):
                            case_count += 1
                            cmd_elements[7] = str(left_row_count)
                            cmd_elements[9] = str(right_row_count)
                            cmd_elements[13] = str(left_step)
                            cmd_elements[15] = str(right_step)
                            cmd_elements[17] = str(left_step_length)
                            cmd_elements[19] = str(right_step_length)

                            prompt = "%s Running case %s / %s ... : %s " % (time.strftime( ISOTIMEFORMAT, time.localtime()), case_count, total_case_count, " ".join(cmd_elements))
                            print prompt

                            params = [str(p) for p in [left_row_count, right_row_count, left_step, right_step, left_step_length, right_step_length]]
                            sp.check_call("echo '#%s' >> %s"%(prompt, out_file_name), shell=True)
                            sp.check_call("echo -n '%s,' >> %s"%(",".join(params), out_file_name), shell=True)
                            sp.check_call(" ".join(cmd_elements), shell=True)




#
# total_case_count = (row_count_max / row_count_step + 1) * len(column_counts) * case_run_time
# case_count = 0
#
# print "Total case count %s ..." % (total_case_count)
# for row_count in xrange(1, row_count_max + 1, row_count_step):
#     for column_count in column_counts:
#         for time in xrange(case_run_time):
#             case_count += 1
#             cmd_elements[7] = str(row_count)
#             cmd_elements[9] = str(column_count)
#             sp.check_call("echo -n '%s,' >> material_result"%(row_count), shell=True)
#             sp.check_call("echo -n '%s,' >> material_result"%(column_count), shell=True)
#             print "Running case %s / %s ... : %s " % (case_count, total_case_count, " ".join(cmd_elements))
#             sp.check_call(" ".join(cmd_elements), shell=True)
#
