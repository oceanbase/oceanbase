#!/bin/env python
__author__ = 'dongyun.zdy'


import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit



import sys
import math

def totalcost_to_nsec_single(totalcost_est,K):
    return K * totalcost_est


def totalcost_to_nsec_array(totalcost_est_arr,K):
    res = []
    for single_est in totalcost_est_arr:
        C = totalcost_to_nsec_single(single_est,K)
        res.append(C)
    return np.array(res)

def case_cmp(a,b):
    if a[2] < b[2] :
        return -1
    elif a[2] > b[2] :
        return 1
    else :
        return 0

def extract_info_from_line(line):
    splited = line.split(",")
    cost = int(splited[0])
    cost_est = int(float(splited[1]))
    row = int(splited[2])
    row_est = int(float(splited[3]))
    return (cost, cost_est, row, row_est)

if __name__ == '__main__':
    file_name = ""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print "wrong arg"
    else:
        file_name = sys.argv[1]
        if len(sys.argv) == 3 and cmp(sys.argv[2], "-w") == 0:
            start = False
            while not start:
                start_str = raw_input()
                if cmp(start_str, "#") == 0:
                    start = True
        costs = []
        cost_ests = []
        rows = []
        row_ests = []
        file = open(file_name, "r")
        cases = []
        for line in file:
            case_param = extract_info_from_line(line)
            cases.append(case_param)
        cases.sort(case_cmp)
        for case in cases:
            costs.append(case[0])
            cost_ests.append(case[1])
            rows.append(case[2])
            row_ests.append(case[3])

        costs_np = np.array(costs)
        cost_ests_np = np.array(cost_ests)
        rows_np = np.array(rows)
        row_ests_np = np.array(row_ests)

        params_init = [1]
        params_fit, conf = curve_fit(totalcost_to_nsec_array,cost_ests_np,costs_np,params_init)

        p = [param for param in params_fit]
        print p


