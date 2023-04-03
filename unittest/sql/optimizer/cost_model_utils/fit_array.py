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
#
# def array_model_form(args):
#     # (
#     #     Nelem,
#     # ) = args
#
#     Telem_ence = 0.00898860
#     Telem_copy = 0.00631888
#
#     Nelem = args
#
#     ELEM_PER_PAGE = 1024
#     extend_cnt = math.ceil(math.log(float(Nelem)/ELEM_PER_PAGE, 2))
#     if extend_cnt < 0:
#         extend_cnt = 0
#     copy_cnt = ELEM_PER_PAGE * (math.pow(2, extend_cnt) - 1)
#
#     total_cost = Telem_ence * Nelem
#     #total_cost += Tmem_alloc * extend_cnt
#     total_cost += Telem_copy * copy_cnt
#
#     return total_cost

def array_model_form(args,
                     #Tstartup,
                     Telem_ence,
                     Telem_copy,
                     #Tmem_alloc
                     ):
    # (
    #     Nelem,
    # ) = args

    Nelem = args

    ELEM_PER_PAGE = 1024
    extend_cnt = math.ceil(math.log(float(Nelem)/ELEM_PER_PAGE, 2))
    if extend_cnt < 0:
        extend_cnt = 0
    copy_cnt = ELEM_PER_PAGE * (math.pow(2, extend_cnt) - 1)

    total_cost = Telem_ence * Nelem
    #total_cost += Tmem_alloc * extend_cnt
    total_cost += Telem_copy * copy_cnt

    return total_cost

def material_model_arr(arg_sets,
                       # Tstartup,
                       Telem_ence,
                       Telem_copy,
                       #Tmem_alloc
                       ):
    res = []
    for single_arg_set in arg_sets:
        res.append(array_model_form(single_arg_set,
                                    # Tstartup,
                                    Telem_ence,
                                    Telem_copy,
                                    #Tmem_alloc
                                    ))
    return np.array(res)

material_model = Model(material_model_arr)
# material_model.set_param_hint("Tstartup", min=0.0)
material_model.set_param_hint("Telem_ence", min=0.0)
material_model.set_param_hint("Telem_copy", min=0.0)
#material_model.set_param_hint("Tmem_alloc", min=0.0)

def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    #file_name = "scan_model.res.formal.prep"
    #out_file_name = "scan_model.fit"
    file_name = "array_result_final"
    out_file_name = "array_model"
    if os.path.exists(out_file_name):
        os.remove(out_file_name)
    #sys.argv.extend("-i arr.prep -o arr.model".split(" "))

    output_fit_res = True
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
        if line.startswith('#'):
            continue
        case_param = extract_info_from_line(line)
        case_params.append(case_param)
        arg_sets.append((case_param[0]))
        times.append(case_param[1])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = material_model.fit(times_np, arg_sets=arg_sets_np,
                            # Tstartup=10.0,
                            Telem_ence=1.0,
                            Telem_copy=1.0,
                            #Tmem_alloc=1.0
                            )


    # res_line = str(result.best_values["Tstartup"]) + ","
    res_line = str(result.best_values["Telem_ence"]) + ","
    res_line += str(result.best_values["Telem_copy"])# + ","
    #res_line += str(result.best_values["Tmem_alloc"])

    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
