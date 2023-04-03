#!/bin/env python
__author__ = 'dongyun.zdy'
import getopt
import sys
import math


def mg_model_form(args,
                  params
                  ):
    (
        Nrow_input,
        Nrow_res,
        Ncol_input,
        Ncol_aggr,
        Ncol_group
    ) = args

    (
       #Tstartup,
       Trow_once,
       Tres_once,
       Taggr_prepare_result,
       Taggr_process,
       Tgroup_cmp_col,
       Tcopy_col
    ) = params

    total_cost = Nrow_res * Tres_once + Nrow_input * Trow_once
    #cost for judge group
    total_cost += Nrow_res * Tgroup_cmp_col
    total_cost += (Nrow_input - Nrow_res) * Ncol_group * Tgroup_cmp_col

    #cost for group related operation
    total_cost += Nrow_res * (Ncol_input * Tcopy_col)
    total_cost += Nrow_res * (Ncol_aggr * Taggr_prepare_result)

    #cost for input row process
    total_cost += Nrow_input * (Ncol_aggr * Taggr_process)

    return total_cost




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
    args = (case_param[0],
            case_param[5],
            case_param[4],
            case_param[2],
            case_param[3])
    time = case_param[6]
    cost_val = mg_model_form(args, model_params)
    percent = (cost_val - time) / time

    new_line = ",".join([line.strip(),"\t" ,str(cost_val),"\t" , str(time),"\t\t" , str(percent * 100)])
    new_line += "\n"
    out_file.write(new_line)

out_file.close()



