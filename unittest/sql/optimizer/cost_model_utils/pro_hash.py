__author__ = 'canfang.scf'
from op_generator import op_generator
from cost_test_conf import Config
import subprocess as sp
import os
from lmfit import Model
import numpy as np

hash_cls = op_generator.gen_operator("hash_join")
conf = Config()
conf.u_to_test_op_c = 'hash'
conf.is_not_running_as_unittest_c = True
conf.schema_file_c = 'c10k1x2.schema'
conf.left_row_count_c = 1000
conf.right_row_count_c = 1000
conf.left_min_c = 1
conf.right_min_c = 1
conf.is_random_c = True
hash_op = hash_cls(conf)
# step 3 process data
final_file_name = "hash_join_result_final"
if os.path.exists(final_file_name):
    os.remove(final_file_name)

data_cmd = hash_op.get_data_preprocess_cmd()
sp.check_call(data_cmd, shell=True)

# step 4 fit and output

out_model_file_name = "hash_model"
if os.path.exists(out_model_file_name):
    os.remove(out_model_file_name)


def hash_model_form(args,
                    Tstart_up,
                    Tright_outer_once,
                    Tleft_outer_once,
                    #Tjoin_row
                    ):
    (
        Nres_row,
        Nleft_row,
        Nright_row,
        Nequal_cond,
        Nno_matched_right,
        Nno_matched_left
    ) = args
    total_cost = Tstart_up  # Tstartup
    total_cost += Nleft_row * 0.74497774
    total_cost += Nright_row * 0.26678144
    total_cost += Nequal_cond * 0.86340381
    total_cost += Nres_row * 0.28939532
    total_cost += Nno_matched_left * Tright_outer_once
    total_cost += Nno_matched_right * Tleft_outer_once
    return total_cost


def hash_model_arr(arg_sets,
                   Tstart_up,
                   Tright_outer_once,
                   Tleft_outer_once):
    res = []
    for single_arg_set in arg_sets:
        res.append(hash_model_form(single_arg_set,
                                   Tstart_up,
                                   Tright_outer_once,
                                   Tleft_outer_once))
    return np.array(res)


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info

hash_model = Model(hash_model_arr)
hash_model.set_param_hint("Tstart_up", min=0.0)
# hash_model.set_param_hint("Tbuild_htable", min=0.0)
# hash_model.set_param_hint("Tright_row_once", min=0.0)
# hash_model.set_param_hint("Tconvert_tuple", min=0.0)
hash_model.set_param_hint("Tright_outer_once", min=0.0)
hash_model.set_param_hint("Tleft_outer_once", min=0.0)
#hash_model.set_param_hint("Tjoin_row", min=0.0)
file = open(final_file_name, "r")
arg_sets = []
times = []
case_params = []
for line in file:
    if line.startswith('#'):
        continue
    case_param = extract_info_from_line(line)
    case_params.append(case_param)
    arg_sets.append((case_param[2], case_param[0], case_param[1], case_param[3], case_param[4], case_param[5]))
    times.append(case_param[6])
file.close()
arg_sets_np = np.array(arg_sets)
times_np = np.array(times)

result = hash_model.fit(times_np, arg_sets=arg_sets_np,
                        Tstartup=0.0,
                        Tright_outer_once=0.0,
                        Tleft_outer_once=0.0)
res_line = str(result.best_values["Tstart_up"]) + ","
res_line += str(result.best_values["Tright_outer_once"]) + ","
res_line += str(result.best_values["Tleft_outer_once"])
print result.fit_report()

if out_model_file_name:
    out_file = open(out_model_file_name, "w")
    out_file.write(res_line)
    out_file.close()
