#!/bin/env python
__author__ = 'dongyun.zdy'


import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt



def mg_model_form(args,
                  Tstartup,
                  Trow_once,
                  Tres_once,
                  Taggr_prepare_result,
                  Taggr_process,
                  Tgroup_hash_col,
                  Tcopy_col
                  ):
    (
        Nrow_input,
        Nrow_res,
        Ncol_input,
        Ncol_aggr,
        Ncol_group
    ) = args

    total_cost = Tstartup +  Nrow_res * Tres_once + Nrow_input * Trow_once
    #cost for judge group
    total_cost += Nrow_input * Ncol_group * Tgroup_hash_col

    #cost for group related operation
    total_cost += Nrow_res * (Ncol_input * Tcopy_col)
    total_cost += Nrow_res * (Ncol_aggr * Taggr_prepare_result)

    #cost for input row process
    total_cost += Nrow_input * (Ncol_aggr * Taggr_process)

    return total_cost




eval_count = 0


def mg_model_arr(arg_sets,
                 Tstartup,
                 Trow_once,
                 Tres_once,
                 Taggr_prepare_result,
                 Taggr_process,
                 Tgroup_hash_col,
                 Tcopy_col
                 ) :

    res = [mg_model_form(single_arg_set,
                         Tstartup,
                         Trow_once,
                         Tres_once,
                         Taggr_prepare_result,
                         Taggr_process,
                         Tgroup_hash_col,
                         Tcopy_col
                        ) for single_arg_set in arg_sets]
    global eval_count
    eval_count += 1
    print "eval "+ str(eval_count)
    return np.array(res)

mg_model = Model(mg_model_arr)
mg_model.set_param_hint("Tstartup", min=0.0)
mg_model.set_param_hint("Trow_once", min=0.0)
mg_model.set_param_hint("Tres_once", min=0.0)
mg_model.set_param_hint("Taggr_prepare_result", min=0.0)
mg_model.set_param_hint("Taggr_process", min=0.0)
mg_model.set_param_hint("Tgroup_hash_col", min=0.0)
mg_model.set_param_hint("Tcopy_col", min=0.0)
def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    file_name = "scan_model.res.formal.prep"
    out_file_name = "scan_model.fit"


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

    file = open(file_name, "r")
    arg_sets = []
    times = []
    case_params = []
    for line in file:
        case_param = extract_info_from_line(line)
        case_params.append(case_param)

        # Nrow_input,
        # Nrow_res,
        # Ncol_input,
        # Ncol_aggr,
        # Ncol_group


        arg_sets.append((case_param[0],
                         case_param[5],
                         case_param[4],
                         case_param[2],
                         case_param[3]
                         ))
        times.append(case_param[6])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = mg_model.fit(times_np, arg_sets=arg_sets_np,
                         Tstartup = 0.1,
                         Trow_once = 0.1,
                         Tres_once = 0.1,
                         Taggr_prepare_result = 0.1,
                         Taggr_process = 0.1,
                         Tgroup_hash_col = 0.1,
                         Tcopy_col = 0.1
                         )

    res_line = str(result.best_values["Tstartup"]) + ","
    res_line += str(result.best_values["Trow_once"]) + ","
    res_line += str(result.best_values["Tres_once"]) + "," 
    res_line += str(result.best_values["Taggr_prepare_result"]) + ","
    res_line += str(result.best_values["Taggr_process"]) + ","
    res_line += str(result.best_values["Tgroup_hash_col"]) + ","
    res_line += str(result.best_values["Tcopy_col"]) 


    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
