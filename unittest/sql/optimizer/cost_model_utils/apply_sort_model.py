#!/bin/env python
__author__ = 'dongyun.zdy'
import getopt
import sys
import math



def material_model_form(args):
    (
        Nrow,
        Ncol,
    ) = args

    Trow_col = 0.02674675
    Trow_once = 0.07931677

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

def get_miss_prob(Nrow, Ncol, Nord, Turn):
    total_size = Nrow * get_row_size(Nord, Ncol)
    TLBcovered = Turn
    if TLBcovered >= 0.9 * total_size:
        hit = 0.9
    else:
        hit = TLBcovered / total_size
    return 1 - hit


def sort_model_form(args,
                    params
                    ):
    (
        Nrow,
        Ncol,
        Nordering
    ) = args

    (
       # Tstartup,
        #Trowstore_once,
        #Trowstore_col,
        # Tarray_once,
        # Tarray_elem_copy,
        # Tordercol,
        # Treserve_cell,
        Tcompare,
        # Trow_once,
        Tmiss_K1,
        Turn
        # Tmiss_K2,
        # Turn

    ) = params


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

#
# def sort_model_form(args,
#                     params):
#     (
#         Nrow,
#         Nordering,
#         Ncol,
#     ) = args
#
#     (
#         Tstartup,
#         Trowstore_once,
#         Trowstore_col,
#         # Tarray_once,
#         # Tarray_elem_copy,
#         Treserve_cell,
#         Tcompare
#     ) = params
#
#
#     total_cost = Tstartup
#
#     #cost for row store
#     total_cost += Nrow * (Trowstore_once + Ncol * Trowstore_col)
#     total_cost += Treserve_cell * Nrow * Ncol * Nordering
#
#     #cost for array
#     # ELEM_PER_PAGE = 1024
#     # extend_cnt = math.ceil(math.log(float(Nrow)/ELEM_PER_PAGE, 2))
#     # copy_cnt = ELEM_PER_PAGE * (math.pow(2, extend_cnt) - 1)
#     #total_cost += Tarray_once * Nrow + Tarray_elem_copy * copy_cnt
#
#     #cost for sorting
#     if Nordering > 2:
#         Nordering_cmp = 2
#     else:
#         Nordering_cmp = Nordering
#     compare_cost = Tcompare * Nordering_cmp
#     total_cost += Nrow * compare_cost * math.log(Nrow, 2)
#
#     return total_cost


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


# sys.argv.extend('-i sort.prep.double -o sort.fit.double -m sort.model.double'.split())

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
# if len(model_params) == 1:
#     model_params = model_params[0]


for line in input_file:
    if line.startswith('#'):
        out_file.write(line)
        continue
    case_param = extract_info_from_line(line)
    args = (case_param[0],
            case_param[1],
            case_param[2])
    time = case_param[4]
    cost_val = sort_model_form(args, model_params)
    percent = (cost_val - time) / time

    new_line = ",".join([line.strip(),str(cost_val),str(percent * 100)])
    new_line += "\n"
    out_file.write(new_line)

out_file.close()



