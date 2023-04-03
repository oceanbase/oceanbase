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
time_per_case = 2
use_delete_min_max = False
filters = []
out_columns = [c for c in xrange(100)]
cols_supplied = False
wrong_arg = False
target_column_id = 0

#sys.argv.extend("-i sort_result -o sort.prep -t 5 -C 4 -f 0,g,1 -f 0,le,100000".split(" "))

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
    # max_i = find_max_index(column_nums[column_id])
    # for j in xrange(len(column_nums)):
    #    column_nums[j] = delete(column_nums[j], max_i)
    # max_i = find_max_index(column_nums[column_id])
    # for j in xrange(len(column_nums)):
    #    column_nums[j] = delete(column_nums[j], max_i)


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


for line in origin_file:
    if line.startswith("#"):
        out_file.write(line)
        continue #skip comment
    column_strs_raw = line.split(",")
    if do_filter(column_strs_raw):
        continue
    column_count = len(column_strs_raw)
    if i == 0:
        avg_strs = []
        avgs = []
        column_nums = []
        for n in xrange(column_count):
            column_nums.append([])
    #split line and cast to float
    for n in xrange(column_count):
        column_nums[n].append(float(column_strs_raw[n]))
    if i == time_per_case - 1:
        if use_delete_min_max:
            delete_max_min_case(column_nums, target_column_id)
        #calc avg per column
        for n in xrange(column_count):
            avgs.append(np.mean(column_nums[n]))
        #cast to str
        avg_strs = [str(a) for a in avgs]
        real_avg_strs = []
        #out_columns filter
        for cid in xrange(len(avg_strs)):
            if cid in out_columns:
                real_avg_strs.append(avg_strs[cid])

        out_file.write(",".join(real_avg_strs) + "\n")
    i = (i + 1) % time_per_case

origin_file.close()
out_file.close()





