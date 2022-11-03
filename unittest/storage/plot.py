#!/bin/env python
__author__ = 'dongyun.zdy'


import sys
import numpy as np
import matplotlib as mpl
from matplotlib import cm
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import math

def extract_info_from_line(line):
    splited = line.split(",")
    cost = int(splited[0])
    cost_est = int(float(splited[1]))
    row = int(splited[2])
    row_est = int(float(splited[3]))
    return (cost, cost_est, row, row_est)

def case_cmp(a,b):
    if a[2] < b[2] :
        return -1
    elif a[2] > b[2] :
        return 1
    else :
        return 0

def do_plot(arg, cost_to_nsec_arg):
    costs = np.array([a[0] for a in arg])
    cost_ests = np.array([a[1] for a in arg])
    rows = np.array([a[2] for a in arg])
    row_ests = np.array([a[3] for a in arg])

    K = cost_to_nsec_arg[0]
    #b = cost_to_nsec_arg[1]

    fig = plt.figure()
    fig.set_size_inches((20,10))
    ax1 = fig.add_subplot(121)
    ax1.plot(rows, costs, color="red")
    ax1.plot(rows, cost_ests, color="blue")
    ax1.plot(rows, K * cost_ests, color="green")
    ax2 = fig.add_subplot(122)
    ax2.scatter(rows, row_ests)

    plt.show()

if __name__ == '__main__':
    file_name = ""

    cost_to_nsec_arg_str = raw_input()
    cost_to_nsec_arg = eval(cost_to_nsec_arg_str)

    if len(sys.argv) < 2:
        print "wrong arg"
        pass
    else:
        file_name = sys.argv[1]
        file = open(file_name, "r")
        cases = []
        for line in file:
            case_param = extract_info_from_line(line)
            cases.append(case_param)
        cases.sort(case_cmp)

        do_plot(cases,cost_to_nsec_arg)
