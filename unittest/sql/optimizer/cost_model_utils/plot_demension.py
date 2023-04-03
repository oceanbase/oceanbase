#!/bin/env python
__author__ = 'dongyun.zdy'


import sys
import numpy as np
import matplotlib as mpl
from matplotlib import cm
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import math
import getopt

def extract_int_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(int(float(item)))
    return line_info

def case_cmp(a,b,c):
    if a[c] < b[c] :
        return -1
    elif a[c] > b[c] :
        return 1
    else :
        return 0

cmp_n = [lambda x, y, z = count: case_cmp(x, y, z) for count in range(10)]
#cmp_n = [lambda x, y: cmp(x[count], y[count]) for count in range(10)]

colors = ["red", "green", "blue", "yellow", "purple", "black", "pink" , "brown", "cyan" ,"orange"]

def do_plot(file_cases):
    fig = plt.figure()
    fig.set_size_inches((20,10))
    ax1 = fig.add_subplot(111)
    for i in xrange(len(file_cases)):
        ax1.plot(file_cases[i][0], file_cases[i][1], color=colors[i])
    plt.show()

if __name__ == '__main__':

    file_names = []
    horizen = 0
    demension = 0
    wrong_arg = False
    opts,args = getopt.getopt(sys.argv[1:],"f:h:d:")

    for op, value in opts:
        if "-f" == op:
            file_names.append(value)
        elif "-h" == op:
            horizen = int(value)
        elif "-d" == op:
            demension = int(value)
        else:
            wrong_arg = True

    if horizen == demension or len(file_names) == 0 or wrong_arg:
        print "wrong arg"
        sys.exit()

    file_cases = []
    for name in file_names:
        file = open(name)
        horizens = []
        demensions = []
        cases = []
        for line in file:
            if line[0] == '[' or line.startswith('#'):
                continue
            case_param = extract_int_info_from_line(line)
            cases.append(case_param)
        cases.sort(cmp_n[horizen])
        for case in cases:
            horizens.append(case[horizen])
            demensions.append(case[demension])
        file_cases.append([np.array(horizens), np.array(demensions)])

    do_plot(file_cases)
