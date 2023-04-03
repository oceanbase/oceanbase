#!/bin/env python
__author__ = 'dongyun.zdy'
import getopt
import sys
import math



def nl_model_form(args,
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
        #Tqual,
        Tres,
        Tfail,
        Tleft_row,
        Tright_row
    ) = params

    total_cost = Tstartup
    total_cost += Nrow_res * Tres
    #total_cost += Nequal_cond * Tqual
    total_cost += (Nequal_cond - Nrow_res) * Tfail
    total_cost += Nrow_left * Tleft_row
    total_cost += Nrow_right * Tright_row

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
    args = (case_param[6],     #Nrow_res
            case_param[0],     #Nrow_left
            case_param[1],     #Nrow_right
            case_param[-3],    #Nright_cache_in
            case_param[-2],    #Nright_cache_out
            case_param[-1],
            case_param[8])
    time = case_param[7]
    cost_val = nl_model_form(args, model_params)
    percent = (cost_val - time) / time

    new_line = ",".join([line.strip(),"\t" ,str(cost_val),"\t" , str(time),"\t\t" , str(percent * 100)])
    new_line += "\n"
    out_file.write(new_line)

out_file.close()



