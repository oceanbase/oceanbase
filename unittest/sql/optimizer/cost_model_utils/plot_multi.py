#!/bin/env python
__author__ = 'dongyun.zdy'


import sys
import numpy as np
import matplotlib as mpl
from matplotlib import cm
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import math

def extract_int_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(int(float(item)))
    return line_info

def case_cmp(a,b,c):
    if c > 1251:
        print c
    if a[c] < b[c] :
        return -1
    elif a[c] > b[c] :
        return 1
    else :
        return 0

cmp_n = [lambda x, y, z = count: case_cmp(x, y, z) for count in range(10)]
#cmp_n = [lambda x, y: cmp(x[count], y[count]) for count in range(10)]

colors = ["red", "green", "blue", "yellow", "purple", "black", "pink", "cyan", "brown", "gray"]

def do_plot(arg, horizen, need_columns_id,label):
    arrs = []
    for i in arg[0]:
        arrs.append([])
    for case in arg:
        for i in xrange(len(case)):
            arrs[i].append(case[i])

    np_arrs = [np.array(a) for a in arrs]
    fig = plt.figure()

    fig.set_size_inches((20,10))
    ax1 = fig.add_subplot(111)
    ax1.set_label(label)
    color_id = 0

    for i in xrange(len(np_arrs)):
        if i == horizen:
            continue
        elif i in need_columns_id:
            ax1.plot(np_arrs[horizen], np_arrs[i], color=colors[color_id])
            color_id = color_id + 1
    plt.show()

if __name__ == '__main__':
    #filename column_count horizen
    if len(sys.argv) < 4:
        print "wrong arg"
        pass
    else:
        file_name = sys.argv[1]
        horizen = int(sys.argv[2])
        file = open(file_name, "r")
        need_columns = sys.argv[3]
        if need_columns == "all":
            need_columns_id = [i for i in xrange(100)]
        else:
            need_columns_id = [int(i) for i in need_columns.split(",")]

        cases = []
        for line in file:
            if line[0] == '[' or line.startswith('#'):
                continue
            case_param = extract_int_info_from_line(line)
            cases.append(case_param)
        cases.sort(cmp_n[horizen])
        do_plot(cases, horizen, need_columns_id, file)
