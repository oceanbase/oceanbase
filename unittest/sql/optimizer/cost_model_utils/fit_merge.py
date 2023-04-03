#!/bin/env python
__author__ = 'dongyun.zdy'


import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt

def merge_model_form(args,
                     Tstartup,
                     Tres_right_op,
                     Tres_right_cache,
                     Tmatch_group,
                     #Tassemble_row,
                     Tequal_fail,
                     Trow_left,
                     Trow_right
                     ):
    (
        Nrow_res,
        Nrow_left,
        Nrow_right,
        Nright_cache_in,
        Nright_cache_out,
        Nright_cache_clear,
        Nequal_cond
    ) = args

    total_cost = Tstartup
    total_cost += Nrow_left * Trow_left
    total_cost += (Nrow_right - Nright_cache_in) * Trow_right
    total_cost += Nright_cache_in * Tres_right_op
    total_cost += Nright_cache_out * Tres_right_cache
    #total_cost += Nrow_res * Tassemble_row
    total_cost += Nright_cache_clear * Tmatch_group
    total_cost += (Nequal_cond - Nrow_res - 2 * Tmatch_group) * Tequal_fail


    # total_cost += Nright_cache_in * Tres_right_op
    # total_cost += (Nrow_res - Nright_cache_in) * Tres_right_cache
    # total_cost += Nright_cache_clear * Tmatch_group
    # total_cost += Nrow_res * Tassemble_row
    # total_cost += (Nequal_cond - Nrow_res - 2 * Tmatch_group) * Tequal_fail
    # total_cost += Nrow_left * Trow_left
    # total_cost += (Nrow_right - Nright_cache_in) * Trow_right

    return total_cost

eval_count = 0

def merge_model_arr(arg_sets,
                    Tstartup,
                    Tres_right_op,
                    Tres_right_cache,
                    Tmatch_group,
                    #Tassemble_row,
                    Tequal_fail,
                    Trow_left,
                    Trow_right
                    ):
    res = [merge_model_form(single_arg_set,
                            Tstartup,
                            Tres_right_op,
                            Tres_right_cache,
                            Tmatch_group,
                            #Tassemble_row,
                            Tequal_fail,
                            Trow_left,
                            Trow_right
                            ) for single_arg_set in arg_sets]
    global eval_count
    eval_count += 1
    return np.array(res)


merge_model = Model(merge_model_arr)
merge_model.set_param_hint("Tstartup", min=0.0)
merge_model.set_param_hint("Tres_right_op", min=0.0)
merge_model.set_param_hint("Tres_right_cache", min=0.0)
merge_model.set_param_hint("Tmatch_group", min=0.0)
#merge_model.set_param_hint("Tassemble_row", min=0.0)
merge_model.set_param_hint("Tequal_fail", min=0.0)
merge_model.set_param_hint("Trow_left", min=0.0)
merge_model.set_param_hint("Trow_right", min=0.0)

def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    file_name = "scan_model.res.formal.prep"
    out_file_name = "scan_model.fit"

    sys.argv.extend("-i merge.prep.1 -o merge.model".split(" "))

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
        arg_sets.append((case_param[6],     #Nrow_res
                         case_param[0],     #Nrow_left
                         case_param[1],     #Nrow_right
                         case_param[-3],    #Nright_cache_in
                         case_param[-2],    #Nright_cache_out
                         case_param[-1],    #Nright_cache_clear
                         case_param[8]      #Nequal_cond
                         ))
        times.append(case_param[7])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = merge_model.fit(times_np, arg_sets=arg_sets_np,
                             Tstartup=0.1,
                             Tres_right_op=0.1,
                             Tres_right_cache=0.1,
                             Tmatch_group=1.0,
                             #Tassemble_row=0.5,
                             Tequal_fail=1.0,
                             Trow_left=0.05,
                             Trow_right=0.05
                            )


    res_line = str(result.best_values["Tstartup"]) + ","
    res_line += str(result.best_values["Tres_right_op"]) + ","
    res_line += str(result.best_values["Tres_right_cache"]) + ","
    res_line += str(result.best_values["Tmatch_group"]) + ","
    #res_line += str(result.best_values["Tassemble_row"]) + ","
    res_line += str(result.best_values["Tequal_fail"]) + ","
    res_line += str(result.best_values["Trow_left"]) + ","
    res_line += str(result.best_values["Trow_right"])


    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
