#!/bin/env python
__author__ = 'dongyun.zdy'

import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt
import os


def material_model_form(args,
                        # Tstartup,
                        Trow_once,
                        Trow_col):
    (
        Nrow,
        Ncol,
    ) = args

    total_cost = 0  # Tstartup
    total_cost += Nrow * (Trow_once + Ncol * Trow_col)
    return total_cost


def material_model_arr(arg_sets,
                       # Tstartup,
                       Trow_once,
                       Trow_col):
    res = []
    for single_arg_set in arg_sets:
        res.append(material_model_form(single_arg_set,
                                       # Tstartup,
                                       Trow_once,
                                       Trow_col))
    return np.array(res)


material_model = Model(material_model_arr)
# material_model.set_param_hint("Tstartup", min=0.0)
material_model.set_param_hint("Trow_once", min=0.0)
material_model.set_param_hint("Trow_col", min=0.0)


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    # file_name = "scan_model.res.formal.prep"
    file_name = "material_result_final"
    # out_file_name = "scan_model.fit"
    out_file_name = "material_model"

    if os.path.exists(out_file_name):
        os.remove(out_file_name)
    # sys.argv.extend("-i rowstore.prepare.bigint -o rowstore.model".split(" "))

    output_fit_res = True
    wrong_arg = False
    opts, args = getopt.getopt(sys.argv[1:], "i:o:")
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
    # 10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = material_model.fit(times_np, arg_sets=arg_sets_np,
                                # Tstartup=10.0,
                                Trow_once=10.0,
                                Trow_col=1.0
                                )

    # res_line = str(result.best_values["Tstartup"]) + ","
    res_line = str(result.best_values["Trow_once"]) + ","
    res_line += str(result.best_values["Trow_col"])

    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
