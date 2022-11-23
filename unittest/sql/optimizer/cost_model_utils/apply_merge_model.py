#!/bin/env python
__author__ = 'dongyun.zdy'
import getopt
import sys
import math



def merge_model_form(args,
                     params
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

    (
        Tstartup,
        Tres_right_op,
        Tres_right_cache,
        Tmatch_group,
        #Tassemble_row,
        Tequal_fail,
        Trow_left,
        Trow_right
    ) = params

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
#
# def merge_model_form(args,
#                      params
#                      ):
#     (
#         Nrow_res,
#         Nrow_left,
#         Nrow_right,
#         Nright_cache_in,
#         Nright_cache_out,
#         Nright_cache_clear,
#         Nequal_cond,
#     ) = args
#
#     (
#         Tstartup,
#         Tright_cache_in,
#         Tright_cache_out,
#         Tright_cache_clear,
#         Tassemble_row,
#         Tequal_fail,
#         Trow_left,
#         #Trow_right
#     ) = params
#
#     total_cost = Tstartup
#     total_cost += Nright_cache_in * Tright_cache_in
#     total_cost += (Nright_cache_out - Nright_cache_clear) * Tright_cache_out
#     total_cost += Nright_cache_clear * Tright_cache_clear
#     total_cost += Nrow_res * Tassemble_row
#     total_cost += (Nequal_cond - Nrow_res - 2 * Tright_cache_clear) * Tequal_fail
#     total_cost += Nrow_left * Trow_left
#     #total_cost += (Nrow_right - Nright_cache_in) * Trow_right
#
#     return total_cost



def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


file_name = "get_total.data.prep"
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
        model_file_name = value
    else:
        wrong_arg = True

if wrong_arg:
    print "wrong arg"
    sys.exit(1)

input_file = open(file_name, "r")
model_file = open(model_file_name, "r")
out_file = open(out_file_name, "w")


line = model_file.readline()
model_params = [float(p) for p in line.split(",")]


for line in input_file:
    case_param = extract_info_from_line(line)
    args = (case_param[6],     #Nrow_res
            case_param[0],     #Nrow_left
            case_param[1],     #Nrow_right
            case_param[-3],    #Nright_cache_in
            case_param[-2],    #Nright_cache_out
            case_param[-1],
            case_param[8])
    time = case_param[7]
    cost_val = merge_model_form(args, model_params)
    percent = (cost_val - time) / time

    new_line = ",".join([line.strip(),"\t" ,str(cost_val),"\t" , str(time),"\t\t" , str(percent * 100)])
    new_line += "\n"
    out_file.write(new_line)

out_file.close()



