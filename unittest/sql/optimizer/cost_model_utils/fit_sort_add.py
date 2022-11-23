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
from cost_test_conf import Config

MATERIAL_ROW_COL = 0.02674675
MATERIAL_ROW_ONCE = 0.07931677
RESERVE_CELL = 0.0044

def material_model_form(args):
    (
        Nrow,
        Ncol,
    ) = args

    global MATERIAL_ROW_COL
    global MATERIAL_ROW_ONCE

    Trow_col = MATERIAL_ROW_COL
    Trow_once = MATERIAL_ROW_ONCE

    total_cost = 0 #Tstartup
    total_cost += Nrow * (Trow_once + Ncol * Trow_col)
    return total_cost

def array_model_form(args):
    # (
    #     Nelem,
    # ) = args

    Telem_ence = 0.00898860
    Telem_copy = 0.00631888

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

def get_row_size(reserve, col):
    size = 16
    size += reserve * 16
    col /= 8
    size += col * (3 + 8 + 4 + 8 + 16 + 32 + 64 + 128)
    size += col
    return size

def round_wasted_spave(rsize, psize):
    nr = math.floor(float(psize / rsize))
    waste = psize - nr * rsize
    return rsize + waste / nr



def get_miss_prob(Nrow, Ncol, Nord, Turn):
    total_size = Nrow * get_row_size(Nord, Ncol)
    TLBcovered = Turn
    if TLBcovered >= 0.9 * total_size:
        hit = 0.9
    else:
        hit = TLBcovered / total_size
    return 1 - hit



def sort_model_form(args,
                    #Tstartup,
                    #Trowstore_once,
                    #Trowstore_col,
                    # Tarray_once,
                    # Tarray_elem_copy,
                    # Tordercol,
                    #Treserve_cell,
                    Tcompare,
                    # Trow_once,
                    Tmiss_K1,
                    Turn
                    # Tmiss_K2
                    ):
    (
        Nrow,
        Ncol,
        Nordering,
    ) = args

    total_cost = 0 #Tstartup

    # total_cost += Nrow * Trow_once
    #cost for rowstore
    # total_cost += material_model_form((Nrow, Ncol))
    # total_cost += 0.0044 * Nrow * Ncol * Nordering
    # total_cost += Tordercol * Nrow * Nordering

    #cost for push array
    # total_cost += array_model_form(Nrow)

    # cost for sorting
    Nordering_cmp = Nordering
    if Nordering >= 1:
        Nordering_cmp = 1
    compare_cost = Tcompare * Nordering_cmp + Tmiss_K1 * get_miss_prob(Nrow, Ncol, Nordering, Turn)
    total_cost += Nrow * compare_cost * math.log(Nrow, 2)

    #cost for get row
    # total_cost += Nrow * (Tmiss_K2 * get_miss_prob(Nrow, Ncol, Nordering))
    return total_cost

def sort_model_arr(arg_sets,
                   #Tstartup,
                   # Trowstore_once,
                   # Trowstore_col,
                   # Tarray_once,
                   # Tarray_elem_copy,
                   # Tordercol,
                   # Treserve_cell,
                   Tcompare,
                   # Trow_once,
                   Tmiss_K1,
                   Turn,
                   # Tmiss_K2
                   ):
    res = []
    for single_arg_set in arg_sets:
        res.append(sort_model_form(single_arg_set,
                                   # Tstartup,
                                   # Trowstore_once,
                                   # Trowstore_col,
                                   # Tarray_once,
                                   # Tarray_elem_copy,
                                   # Tordercol,
                                   # Treserve_cell,
                                   Tcompare,
                                   # Trow_once,
                                   Tmiss_K1,
                                   Turn,
                                   # Tmiss_K2
                                   ))
    return np.array(res)

sort_model = Model(sort_model_arr)
# #sort_model.set_param_hint("Tstartup", min=0.0)
# #sort_model.set_param_hint("Trow_startup", min=0.0)
# sort_model.set_param_hint("Trow_col", min=0.0)
# #sort_model.set_param_hint("Tcmp_startup", min=0.0)
# sort_model.set_param_hint("Trow_once", min=0.0)
# sort_model.set_param_hint("Tcompare", min=0.0)
# sort_model.set_param_hint("Talloc", min=0.0)
# sort_model.set_param_hint("Treserve_cell", min=0.0)

# sort_model.set_param_hint("Tstartup", min=0)
# sort_model.set_param_hint("Trowstore_once", min=0.0)
# sort_model.set_param_hint("Trowstore_col", min=0.0)
# sort_model.set_param_hint("Tarray_once", min=0.0)
# sort_model.set_param_hint("Tarray_elem_copy", min=0.0)
# sort_model.set_param_hint("Tordercol", min=0.0)
# sort_model.set_param_hint("Treserve_cell", min=0.0)
sort_model.set_param_hint("Tcompare", min=0.0)
# sort_model.set_param_hint("Trow_once", min=0.0)
sort_model.set_param_hint("Tmiss_K1", min=0.0)
sort_model.set_param_hint("Turn", min=2097152.0, max=2097153.0)

# sort_model.set_param_hint("Tmiss_K2", min=0.0)

def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


if __name__ == '__main__':
    #file_name = "scan_model.res.formal.prep"
    #out_file_name = "scan_model.fit"

    #file_name = "scan_model.res.formal.prep"
    #out_file_name = "scan_model.fit"

    file_name = "sort_add_" + Config.u_to_test_type + "_result_final"
    out_file_name = "sort_add_" + Config.u_to_test_type + "_model"
    # sys.argv.extend("-i sort.prep.bigint -o sort.model".split(" "))
    if os.path.exists(out_file_name):
        os.remove(out_file_name)

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
        arg_sets.append((case_param[0], case_param[1], case_param[2]))
        times.append(case_param[4])
    file.close()
    arg_sets_np = np.array(arg_sets)
    times_np = np.array(times)
    #10, 0.20406430879623488, 0.016618100054245379, 14.0, 4.5, 37.0, -0.005, 0.5, -7.0
    result = sort_model.fit(times_np, arg_sets=arg_sets_np,
                            # Tstartup=25.0,
                            # Trowstore_once=1.0,
                            # Trowstore_col=1.0,
                            # Tarray_once=1.0,
                            # Tarray_elem_copy=1.0,
                            # Tordercol=1.0,
                            # Treserve_cell=1.0,
                            Tcompare=1.0,
                            # Trow_once=1.0,
                            Tmiss_K1=1.0,
                            Turn=2097152,
                            # Tmiss_K2=1.0
                            )

    # res_line = str(result.best_values["Tstartup"]) + ","
    # res_line += str(result.best_values["Trowstore_once"]) + ","
    # res_line += str(result.best_values["Trowstore_col"]) + ","
    # res_line += str(result.best_values["Tarray_once"]) + ","
    # res_line += str(result.best_values["Tarray_elem_copy"]) + ","
    # res_line = str(result.best_values["Tordercol"]) + ","
    # res_line = str(result.best_values["Treserve_cell"]) + ","
    res_line = str(result.best_values["Tcompare"]) + ","
    # res_line += str(result.best_values["Trow_once"]) #+ ","
    res_line += str(result.best_values["Tmiss_K1"]) + ","
    res_line += str(result.best_values["Turn"])
    # res_line += str(result.best_values["Tmiss_K2"])


    print result.fit_report()

    if output_fit_res:
        out_file = open(out_file_name, "w")
        out_file.write(res_line)
        out_file.close()
