#!/bin/env python
__author__ = 'dongyun.zdy'


import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt



def io_model_form(args,
                  # Trow_once,
                  # Trow_col,
                  Tio_row,
                  # Tio_col_desc,
                  Tio_row_col_desc,
                  ):
    (
        Nrow,
        Ncol,
    ) = args
    #
    # total_cost = Tstartup
    # total_cost += Nrow * (Trow_once + Ncol * Trow_col)

    io_cost = Nrow * Tio_row
    # io_cost -= Ncol * Tio_col_desc
    io_cost -= Nrow * Ncol * Tio_row_col_desc
    if io_cost < 0:
        io_cost = 0

    total_cost = io_cost
    return total_cost

def io_model_arr(arg_sets,
                      # Tstartup,
                      # Trow_once,
                      # Trow_col,
                      Tio_row,
                      # Tio_col_desc,
                      Tio_row_col_desc
                      ):
    res = []
    for single_arg_set in arg_sets:
        res.append(io_model_form(single_arg_set,
                                 # Tstartup,
                                 # Trow_once,
                                 # Trow_col,
                                 Tio_row,
                                 # Tio_col_desc,
                                 Tio_row_col_desc
                                 ))
    return np.array(res)

io_model = Model(io_model_arr)
# io_model.set_param_hint("Tstartup", min=0.0)
# io_model.set_param_hint("Trow_once", min=0.0)
# io_model.set_param_hint("Trow_col", min=0.0)
io_model.set_param_hint("Tio_row", min=0.0)
# io_model.set_param_hint("Tio_col_desc", min=0.0)
io_model.set_param_hint("Tio_row_col_desc", min=0.0, max=0.07)

def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    file_name = "scan_model.res.formal.prep"
    out_file_name = "scan_model.fit"
    model_file = None


    # sys.argv.extend("-i scan.W.small.prep -o scan.io.fit".split(" "))

    output_fit_res = False
    wrong_arg = False
    opts,args = getopt.getopt(sys.argv[1:],"i:o:m:")
    for op, value in opts:
        if "-i" == op:
            file_name = value
        elif "-o" == op:
            output_fit_res = True
            out_file_name = value
        elif "-m" == op:
            model_file = value
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
        times.append(case_param[6])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = io_model.fit(times_np, arg_sets=arg_sets_np,
                          # Tstartup=10.0,
                          # Trow_once=10.0,
                          # Trow_col=1.0,
                          Tio_row=1.0,
                          # Tio_col_desc=1.0,
                          Tio_row_col_desc=1.0
                          )

    # Tstartup = result.best_values["Tstartup"]
    # Trow_once = result.best_values["Trow_once"]
    # Trow_col = result.best_values["Trow_col"]
    Tio_row = result.best_values["Tio_row"]
    # Tio_col_desc = result.best_values["Tio_col_desc"]
    Tio_row_col_desc = result.best_values["Tio_row_col_desc"]

    print result.fit_report()

    fit_res = io_model_arr(arg_sets_np,
                                # Tstartup,
                                # Trow_once,
                                # Trow_col,
                                Tio_row,
                                # Tio_col_desc,
                                Tio_row_col_desc
                                )

    if output_fit_res:
        out_file = open(out_file_name, "w")
        for i in xrange(len(arg_sets_np)):
            a = list(arg_sets_np[i])
            b = [times_np[i]]
            c = [fit_res[i]]
            d = [(fit_res[i] - times_np[i]) * 100 / times_np[i]]
            out_file.write(','.join([str(i) for i in  a + b + c + d]) + "\n")
        out_file.close()

    if model_file is not None:
        mf = open(model_file, 'w')
        mf.write(','.join([str(i) for i in [
                                            # Tstartup,
                                            # Trow_once,
                                            # Trow_col,
                                            Tio_row,
                                            # Tio_col_desc,
                                            Tio_row_col_desc
                                            ]]))
        mf.close()

