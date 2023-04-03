#!/bin/env python
__author__ = 'dongyun.zdy'

import sys
import os
import numpy as np
import getopt


file_name = "scan_model.res.formal"
if len(sys.argv) >= 2:
    file_name = sys.argv[1]
out_file_name = file_name + ".prep"
time_per_case = 5
use_delete_min_max = False
filters = []
out_columns = [c for c in xrange(100)]
cols_supplied = False
wrong_arg = False
target_column_id = 0

#sys.argv.extend("-i sort_result -o sort.8.test -t 7 -C 2 -f 1,e,8".split(" "))
sys.argv.extend("-i nestloop_result -o nl_result".split(" "))
opts,args = getopt.getopt(sys.argv[1:],"i:o:t:f:a:dc:C:")
for op, value in opts:
    if "-i" == op:
        file_name = value
    elif "-o" == op:
        out_file_name = value
    elif "-t" == op:
        time_per_case = int(value)
    elif "-f" == op:
        filter_str = value
        filter_elements = filter_str.split(",")
        if not filter_elements[1] in ["g","l","ge","le","e","ne"]:
            print "invalid filter type"
            sys.exit(1)
        filters.append(filter_str.split(","))
    elif "-a" == op:
        time_per_case = int(value)
    elif "-d" == op:
        use_delete_min_max = True
    elif "-C" == op:
        target_column_id = int(value)
    elif "-c" == op:
        if not cols_supplied:
            cols_supplied = True
            out_columns = []
        out_columns.extend([int(c) for c in value.split(",")])
    else:
        wrong_arg = True

if wrong_arg:
    print "wrong arg"
    sys.exit(1)

if time_per_case < 5:
    use_delete_min_max = False

if os.path.exists(out_file_name):
    os.remove(out_file_name)

origin_file = open(file_name, "r")
out_file = open(out_file_name,"w")

i = 0
column_nums = []
avgs = []
avg_strs = []

def delete(li, index):
    li = li[:index] + li[index+1:]
    return li

def find_max_index(l):
    max = -9999999999999999999999
    max_i = -1
    for i in xrange(len(l)):
        if l[i] > max:
            max = l[i]
            max_i = i
    return max_i

def find_min_index(l):
    min = 999999999999999999999999
    min_i = -1
    for i in xrange(len(l)):
        if l[i] < min:
            min = l[i]
            min_i = i
    return min_i

def delete_max_min_case(column_nums, column_id):
    # min_i = find_min_index(column_nums[len(column_nums) - 1])
    # for j in xrange(len(column_nums)):
    #     column_nums[j] = delete(column_nums[j], min_i)
    max_i = find_max_index(column_nums[column_id])
    for j in xrange(len(column_nums)):
       column_nums[j] = delete(column_nums[j], max_i)
    max_i = find_max_index(column_nums[column_id])
    for j in xrange(len(column_nums)):
       column_nums[j] = delete(column_nums[j], max_i)


def do_filter(column_strs):
    filtered = False
    for f in filters:
        if f[1] == "g" and float(column_strs[int(f[0])]) <= int(f[2]) :
            filtered = True
            break
        elif f[1] == "l" and float(column_strs[int(f[0])]) >= int(f[2]) :
            filtered = True
            break
        elif f[1] == "ge" and float(column_strs[int(f[0])]) < int(f[2]) :
            filtered = True
            break
        elif f[1] == "le" and float(column_strs[int(f[0])]) > int(f[2]) :
            filtered = True
            break
        elif f[1] == "e" and float(column_strs[int(f[0])]) != int(f[2]) :
            filtered = True
            break
        elif f[1] == "ne" and float(column_strs[int(f[0])]) == int(f[2]) :
            filtered = True
            break
    return filtered



state = 0 #comment line

elements = []


for line in origin_file:
    line = line.strip()
    if state == 0:
        out_file.write(line + "\n")
    elif state == 1:
        elements = line.split(",row_count : ")
    elif state == 2:
        pass
    elif state == 3:
        pass
    elif state == 4:
        elements.append(line.split("join_time except conds : ")[1])
    elif state == 5:
        elements.append(line.split("equal_eval : ")[1])
    elif state == 6:
        pass
    elif state == 7:
        elements.append(line.split("other_eval : ")[1])
    elif state == 8:
        pass
    elif state == 9:
        elements.append(line.split("right_cache_put : ")[1])
    elif state == 10:
        elements.append(line.split("right_cache_acc : ")[1])
    elif state == 11:
        elements.append(line.split("match_group_count : ")[1])
        out_file.write(",".join(elements) + "\n")
    else:
        print "wrong state"
    state = (state + 1) % 12

origin_file.close()
out_file.close()





