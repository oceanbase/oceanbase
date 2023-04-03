#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os

if os.path.exists("material_result"):
    os.remove("material_result")

if os.path.exists("material_final_result"):
    os.remove("material_final_result")

# cmd_form = "./cost_model_util -B -t material -s c10k1.schema -r 1000 -p 1 >> material_result"
cmd_form = "./cost_model_util -G -t material -s c10k1.schema -r 1000 -p 1 >> material_result"
cmd_elements = cmd_form.split(" ")

row_count_max = 10001
row_count_step = 100

column_counts = [3, 5, 8]

case_run_time = 7

total_case_count = (row_count_max / row_count_step + 1) * len(column_counts) * case_run_time
case_count = 0

print "Total case count %s ..." % (total_case_count)
for row_count in xrange(1, row_count_max + 1, row_count_step):
    for column_count in column_counts:
        for time in xrange(case_run_time):
            case_count += 1
            cmd_elements[7] = str(row_count)
            cmd_elements[9] = str(column_count)
            sp.check_call("echo -n '%s,' >> material_result" % (row_count), shell=True)
            sp.check_call("echo -n '%s,' >> material_result" % (column_count), shell=True)
            print "Running case %s / %s ... : %s " % (case_count, total_case_count, " ".join(cmd_elements))
            sp.check_call(" ".join(cmd_elements), shell=True)
