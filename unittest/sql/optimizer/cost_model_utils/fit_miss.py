#!/bin/env python
__author__ = 'dongyun.zdy'


import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt


def get_row_size(col):
    size = 16
    size += col * (3 + 8 + 4 + 8 + 16 + 32 + 64 + 128)
    size += col
    return size

def round_wasted_spave(rsize, psize):
    nr = math.floor(float(psize / rsize))
    waste = psize - nr * rsize
    return rsize + waste / nr



def get_miss_prob(Nrow, Ncol, Turn):
    total_size = Nrow * get_row_size(Ncol)
    TLBcovered = Turn
    if TLBcovered >= 0.9 * total_size:
        hit = 0.9
    else:
        hit = TLBcovered / total_size
    return 1 - hit

def sort_model_form(args,
                    Tmiss,
                    Turn
                    ):
    (
        Nrow,
        Ncol,
    ) = args

    total_cost = 0

    total_cost += Nrow * Tmiss * Ncol * get_miss_prob(Nrow, Ncol, Turn)

    return total_cost

def sort_model_arr(arg_sets,
                   Tmiss,
                   Turn,
                   ):
    res = []
    for single_arg_set in arg_sets:
        res.append(sort_model_form(single_arg_set,
                                   Tmiss,
                                   Turn,
                                   ))
    return np.array(res)

sort_model = Model(sort_model_arr)
sort_model.set_param_hint("Tmiss", min=0.0)
sort_model.set_param_hint("Turn", min=2097152.0, max=2097153.0)

# sort_model.set_param_hint("Tmiss_K2", min=0.0)

def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    file_name = "miss.prep.1"
    out_file_name = "miss.model"

    # sys.argv.extend("-i sort.prep.bigint -o sort.model".split(" "))

    output_fit_res = False
    wrong_arg = False
    opts,args = getopt.getopt(sys.argv[1:],"i:o:R:C:")
    for op, value in opts:
        if "-i" == op:
            file_name = value
        elif "-o" == op:
            output_fit_res = True
            out_file_name = value
        elif "-R" == op:
            MATERIAL_ROW_ONCE = float(value)
        elif "-C" == op:
            MATERIAL_ROW_COL = float(value)
        else:
            wrong_arg = True

    if wrong_arg:
        print "wrong arg"
        sys.exit(1)

    file = open(file_name, "r")
    arg_sets = []
    times = []
    case_params = []
    for line in file:
        if line.startswith('#'):
            continue
        case_param = extract_info_from_line(line)
        case_params.append(case_param)
        arg_sets.append((case_param[0], case_param[1]))
        times.append(case_param[3])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = sort_model.fit(times_np, arg_sets=arg_sets_np,
                            Tmiss=1.0,
                            Turn=2097152,
                            )

    Tmiss = result.best_values["Tmiss"]
    Turn = result.best_values["Turn"]
    res_line = str(Tmiss) + ","
    res_line += str(Turn)
    # res_line += str(result.best_values["Tmiss_K2"])


    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()

    for i, args in enumerate(arg_sets):
        cost = sort_model_form(args, Tmiss, Turn)
        time = times[i]
        print "\t".join([str(args), str(time), str(cost)])
