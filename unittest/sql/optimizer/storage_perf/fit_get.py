#!/bin/env python
__author__ = 'dongyun.zdy'


import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt

STARTUP = 0.0

def get_model_form(args,
                   # Tstartup,
                   Tper_row,
                   Tper_col,
                   Tcorrection1,
                   Tcorrection2,
                   # Tlog3
                   ):
    (
        Nrow,
        Ncol,
    ) = args

    global STARTUP
    total_cost = STARTUP#Tstartup
    total_cost += Nrow * (Tper_row + Ncol * Tper_col)
    total_cost += Tcorrection1 * math.log(Tcorrection2 * Nrow , 2)
    return total_cost

def get_model_arr(arg_sets,
                  # Tstartup,
                  Tper_row,
                  Tper_col,
                  Tcorrection1,
                  Tcorrection2,
                  # Tlog3
                  ):
    res = []
    for single_arg_set in arg_sets:
        res.append(get_model_form(single_arg_set,
                                  # Tstartup,
                                  Tper_row,
                                  Tper_col,
                                  Tcorrection1,
                                  Tcorrection2,
                                  # Tlog3
                                  ))
    return np.array(res)

get_model = Model(get_model_arr)
# get_model.set_param_hint("Tstartup", min=0.0)
get_model.set_param_hint("Tper_row", min=0.0)
get_model.set_param_hint("Tper_col", min=0.0)
get_model.set_param_hint("Tcorrection1", min=0.0)
get_model.set_param_hint("Tcorrection2", min=0.0)
# get_model.set_param_hint("Tlog3", min=0.0)


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    file_name = "scan_model.res.formal.prep"
    out_file_name = "scan_model.fit"

    # sys.argv.extend("-i get.IO.prep -o get.IO.model".split(" "))

    output_fit_res = False
    wrong_arg = False
    opts,args = getopt.getopt(sys.argv[1:],"i:o:")
    for op, value in opts:
        if "-i" == op:
            file_name = value
        elif "-o" == op:
            output_fit_res = True
            out_file_name = value
        else:
            wrong_arg = True

    if wrong_arg:
        print "wrong arg"
        sys.exit(1)

    if file_name.find('rc') != -1:
        STARTUP = 170.0
    elif file_name.find('bc') != -1:
        STARTUP = 210.0
    else:
        STARTUP = 520.0

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
        times.append(case_param[4])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = get_model.fit(times_np, arg_sets=arg_sets_np,
                           # Tstartup=10.0,
                           Tper_row=10.0,
                           Tper_col=1.0,
                           Tcorrection1=1.0,
                           Tcorrection2=1.0,
                           # Tlog3=1.0,
                           )


    # res_line = str(result.best_values["Tstartup"]) + ","
    res_line = str(result.best_values["Tper_row"]) + ","
    res_line += str(result.best_values["Tper_col"]) + ","
    res_line += str(result.best_values["Tcorrection1"]) + ","
    res_line += str(result.best_values["Tcorrection2"]) #+ ","
    # res_line += str(result.best_values["Tlog3"])

    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
