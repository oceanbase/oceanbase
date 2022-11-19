#!/bin/env python
__author__ = 'dongyun.zdy'
import getopt
import sys
import math



def scan_io_model_form(args,
                       params
                       ):
    (
        Nrow,
        Ncol,
        Nsize_factor
    ) = args

    (
        # Tstartup,
        # Trow_once,
        Tper_col,
        Tper_row,
        # Tio_col_desc,
        Tper_row_io_wait,
        Tper_row_pipline_factor
    ) = params

    # Tper_row_col_desc = 0.02933630
    # Tper_row = 0.56897932

    total_cost = 0  # Tstartup
    total_cost += Nrow * (Tper_row * Nsize_factor + Ncol * Tper_col)

    io_cost = Nrow * Tper_row_io_wait * Nsize_factor
    io_cost -= Nrow * Ncol * Tper_row_pipline_factor
    if io_cost < 0:
        io_cost = 0

    total_cost += io_cost
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
    if line.startswith('#'):
        out_file.write(line)
        continue
    case_param = extract_info_from_line(line)
    args = (case_param[0],
            case_param[1],
            case_param[2])
    time = case_param[4]
    cost_val = scan_io_model_form(args, model_params)
    percent = (cost_val - time) / time

    new_line = ",".join([line.strip(),str(cost_val),str(percent * 100)])
    new_line += "\n"
    out_file.write(new_line)

out_file.close()


