#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os

if os.path.exists("sort_result"):
    os.remove("sort_result")

#cmd_form = "LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -GBR -t sort -s c20.schema -r 1000 -c 10 -p 10 >> sort_result"
cmd_form = "LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -GR -t sort -s sort.schema -r 1000 -c 10 -p 10 >> sort_result"
cmd_elements = cmd_form.split(" ")

row_counts = [1, 100, 500, 800, 1000, 3000, 5000, 8000, 9000, 10000, 20000, 40000, 60000, 70000, 100000, 300000]
column_counts = [1, 2, 3, 4, 5]
#input_col_cnts = [15, 30, 45]
input_col_cnts = [3, 5, 9] #schema file related, col counts should not be less than projector count
case_run_time = 7

total_case_count = len(row_counts) * len(column_counts) * len(input_col_cnts)
case_count = 0

print "Total case count %s ..." % (total_case_count)
for row_count in row_counts:
    for column_count in column_counts:
        for input_col in input_col_cnts:
            cmd_elements[8] = str(row_count)
            cmd_elements[10] = str(column_count)
            cmd_elements[12] = str(input_col)

            case_count += 1
            prompt = "Running case %s / %s ... : %s " % (case_count, total_case_count, " ".join(cmd_elements))
            print prompt
            sp.check_call('echo "### %s" >> sort_result' % prompt, shell=True)
            if column_count > input_col:
                print "### PASS"
                sp.check_call('echo "### PASS" >> sort_result', shell=True)
                continue
            for time in xrange(case_run_time):
                print "running the %d time" % time
                sp.check_call("echo -n '%s,' >> sort_result"%(row_count), shell=True)
                sp.check_call("echo -n '%s,' >> sort_result"%(column_count), shell=True)
                sp.check_call("echo -n '%s,' >> sort_result"%(input_col), shell=True)
                sp.check_call(" ".join(cmd_elements), shell=True)

