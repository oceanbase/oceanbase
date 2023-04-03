#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os

if os.path.exists("array_result"):
    os.remove("array_result")

#cmd_form = 'LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -GB -s c10k1.schema -t array -r 1000000'
cmd_form = 'LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./cost_model_util -G -s c10k1.schema -t array -r 1000000'
cmd_elements = cmd_form.split(" ")

minrc = 1
maxrc = 1100001
step = 1000
case_run_time = 5

total_case_count = (maxrc - minrc) / step 
case_count = 0

print "Total case count %s ..." % (total_case_count)
for row_count in xrange(minrc, maxrc + 1, step):
    cmd_elements[-1] = str(row_count)

    case_count += 1
    prompt = "Running case %s / %s ... : %s " % (case_count, total_case_count, " ".join(cmd_elements))
    print prompt
    sp.check_call('echo "### %s" >> array_result' % prompt, shell=True)
    for time in xrange(case_run_time):
        #print "running the %d time" % time
        sp.check_call("echo -n '%s,' >> array_result"%(row_count), shell=True)
        sp.check_call(" ".join(cmd_elements) + ' >> array_result', shell=True)

