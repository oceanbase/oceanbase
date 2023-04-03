#!/bin/env python
__author__ = 'dongyun.zdy'


import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt



def nl_model_form(args,
                  Tstartup,
                  #Tqual,
                  Tres,
                  Tfail,
                  Tleft_row,
                  Tright_row
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
    total_cost += Nrow_res * Tres
    #total_cost += Nequal_cond * Tqual
    total_cost += (Nequal_cond - Nrow_res) * Tfail
    total_cost += Nrow_left * Tleft_row
    total_cost += Nrow_right * Tright_row

    return total_cost

eval_count = 0

def nl_model_arr(arg_sets,
                 Tstartup,
                 #Tqual,
                 Tres,
                 Tfail,
                 Tleft_row,
                 Tright_row
                 ):
    res = [nl_model_form(single_arg_set,
                         Tstartup,
                         #Tqual,
                         Tres,
                         Tfail,
                         Tleft_row,
                         Tright_row
                         ) for single_arg_set in arg_sets]
    global eval_count
    eval_count += 1
    return np.array(res)


nl_model = Model(nl_model_arr)
nl_model.set_param_hint("Tstartup", min=0.0, max = 50)
#nl_model.set_param_hint("Tqual", min=0.0)
nl_model.set_param_hint("Tres", min=0.0)
nl_model.set_param_hint("Tfail", min=0.0)
nl_model.set_param_hint("Tleft_row", min=0.0)
nl_model.set_param_hint("Tright_row", min=0.0)


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    file_name = "scan_model.res.formal.prep"
    out_file_name = "scan_model.fit"

    sys.argv.extend("-i nl.prep -o nl.model".split(" "))

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
    result = nl_model.fit(times_np, arg_sets=arg_sets_np,
                             Tstartup=50.0,
                             #Tqual=0.1,
                             Tres=0.3,
                             Tfail=0.3,
                             Tleft_row=0.3,
                             Tright_row=0.3
                            )


    res_line = str(result.best_values["Tstartup"]) + ","
    #res_line += str(result.best_values["Tqual"]) + ","
    res_line += str(result.best_values["Tres"]) + ","
    res_line += str(result.best_values["Tfail"]) + ","
    res_line += str(result.best_values["Tleft_row"]) + ","
    res_line += str(result.best_values["Tright_row"])


    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
